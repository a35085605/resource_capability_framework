from __future__ import annotations

from collections.abc import Callable
from typing import Generic, TypeVar, cast

from lifecycle.capability.diagnostics import LifecycleDiagnostics
from lifecycle.capability.result import (
    AcquireResult,
    LifecycleOutcome,
    LifecycleResult,
    ReleaseResult,
)
from lifecycle.capability.snapshot import LifecyclePhase, LifecycleSnapshot
from lifecycle.capability.state import Acquiring, Active, Idle, LifecycleState, Releasing
from lifecycle.effect import IO
from lifecycle.effect.io import _Exit
from lifecycle.resource import (
    ReleaseReport,
    Resource,
    ResourceAllocationError,
    ResourceScope,
    ScopeClosedError,
)
from lifecycle.resource.resource import _CloseResult


RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")
ChildRequestT = TypeVar("ChildRequestT")
ChildCapabilityT = TypeVar("ChildCapabilityT")


def _snapshot_from_state(
    state: LifecycleState[RequestT, CapabilityT],
) -> LifecycleSnapshot[RequestT, CapabilityT]:
    if isinstance(state, Idle):
        return LifecycleSnapshot(state.generation)
    if isinstance(state, Acquiring):
        return LifecycleSnapshot(
            state.generation,
            state.request,
            phase=LifecyclePhase.ACQUIRING,
        )
    if isinstance(state, Active):
        return LifecycleSnapshot(
            state.generation,
            state.request,
            state.capability,
            phase=LifecyclePhase.ACTIVE,
        )
    if isinstance(state, Releasing):
        return LifecycleSnapshot(
            state.generation,
            state.request,
            phase=LifecyclePhase.RELEASING,
        )
    raise RuntimeError("unsupported lifecycle state")


class CapabilityLifecycleCoordinator(Generic[RequestT, CapabilityT]):
    """Describe lifecycle operations and publish under the owning scope's lock.

    Each accepted activation owns a fresh child scope. Acquisition, rollback,
    finalization, and recovery handoff remain entirely in the resource layer.
    """

    __slots__ = ("_condition", "_resource_factory", "_scope", "_state")

    def __init__(
        self,
        resource_factory: Callable[[RequestT], Resource[CapabilityT]],
        *,
        scope: ResourceScope,
    ) -> None:
        if not callable(resource_factory):
            raise TypeError("resource_factory must be callable")
        if not isinstance(scope, ResourceScope):
            raise TypeError("scope must be a ResourceScope")
        self._resource_factory = resource_factory
        self._scope = scope
        self._condition = scope._condition
        self._state: LifecycleState[RequestT, CapabilityT] = Idle(0)

    def read(self) -> IO[LifecycleSnapshot[RequestT, CapabilityT]]:
        def observe() -> LifecycleSnapshot[RequestT, CapabilityT]:
            with self._condition:
                self._reconcile_locked()
                return _snapshot_from_state(self._state)

        return IO.delay(observe)

    def acquire(
        self,
        expected_generation: int,
        request: RequestT,
    ) -> IO[AcquireResult[RequestT, CapabilityT]]:
        self._validate_generation(expected_generation)
        if request is None:
            raise TypeError("request cannot be None")

        def begin() -> IO[AcquireResult[RequestT, CapabilityT]]:
            with self._condition:
                self._reconcile_locked()
                state = self._state
                if expected_generation != state.generation or not isinstance(state, Idle):
                    return IO.pure(LifecycleResult.not_executed(_snapshot_from_state(state)))
                generation = state.generation
                try:
                    activation = self._scope._child_locked()
                except ScopeClosedError as exc:
                    self._state = Idle(generation + 1)
                    return IO.pure(LifecycleResult(
                        _snapshot_from_state(self._state), LifecycleOutcome.ACQUIRE_FAILED,
                        LifecycleDiagnostics(exc, ReleaseReport()),
                    ))
                self._state = Acquiring(generation, request, activation)

            # The factory and capability validation run inside the inflight batch.
            # Shutdown must wait for a factory using its parent's capability.
            resource = Resource.pure(request).flat_map(self._resource_for_request)

            def finish(outcome: _Exit[CapabilityT]) -> IO[AcquireResult[RequestT, CapabilityT]]:
                if outcome.error is not None:
                    return activation._close().map(lambda cleanup: self._acquire_failed(
                        generation, activation, outcome.error, cleanup
                    ))
                return self._publish_or_close(
                    generation, request, cast(CapabilityT, outcome.value), activation
                )

            return activation.acquire(resource)._capture().flat_map(finish)

        return IO.defer(begin)

    def _acquire_failed(
        self, generation: int, activation: ResourceScope,
        error: BaseException, cleanup: _CloseResult,
    ) -> AcquireResult[RequestT, CapabilityT]:
        snapshot = self._complete_operation(generation, activation)
        if not isinstance(error, Exception):
            raise error
        if cleanup.interruption is not None:
            raise cleanup.interruption from error
        if isinstance(error, ResourceAllocationError):
            # The rollback report may also be in concurrent closure; count it once.
            diagnostics = LifecycleDiagnostics(error.cause, error.release_report)
        else:
            diagnostics = LifecycleDiagnostics(error, cleanup.report)
        return LifecycleResult(snapshot, LifecycleOutcome.ACQUIRE_FAILED, diagnostics)

    def _publish_or_close(
        self, generation: int, request: RequestT,
        capability: CapabilityT, activation: ResourceScope,
    ) -> IO[AcquireResult[RequestT, CapabilityT]]:
        with self._condition:
            if (
                self._matches_locked(generation, activation)
                and isinstance(self._state, Acquiring)
                and activation._is_open_locked()
            ):
                self._state = Active(generation, request, capability, activation)
                return IO.pure(LifecycleResult(
                    _snapshot_from_state(self._state), LifecycleOutcome.ACQUIRE_SUCCEEDED,
                ))

        def unpublished(cleanup: _CloseResult) -> AcquireResult[RequestT, CapabilityT]:
            snapshot = self._complete_operation(generation, activation)
            if cleanup.interruption is not None:
                raise cleanup.interruption
            return LifecycleResult(
                snapshot, LifecycleOutcome.ACQUIRE_FAILED,
                LifecycleDiagnostics(
                    ScopeClosedError("activation closed before capability publication"),
                    cleanup.report,
                ),
            )

        # Shutdown won publication, possibly after commit. Cleanup precedes failure.
        return activation._close().map(unpublished)

    def _resource_for_request(self, request: RequestT) -> Resource[CapabilityT]:
        resource = self._resource_factory(request)
        if not isinstance(resource, Resource):
            raise TypeError("resource_factory must return Resource")

        def require_capability(value: CapabilityT) -> CapabilityT:
            if value is None:
                raise TypeError("capability resource cannot produce None")
            return value

        return resource.map(require_capability)

    def release(
        self,
        expected_generation: int,
        request: RequestT,
    ) -> IO[ReleaseResult[RequestT, CapabilityT]]:
        self._validate_generation(expected_generation)
        if request is None:
            raise TypeError("request cannot be None")

        def begin() -> IO[ReleaseResult[RequestT, CapabilityT]]:
            with self._condition:
                self._reconcile_locked()
                state = self._state
                if expected_generation != state.generation or not isinstance(state, Active):
                    return IO.pure(LifecycleResult.not_executed(_snapshot_from_state(state)))
                if state.request != request:
                    return IO.pure(LifecycleResult.not_executed(_snapshot_from_state(state)))
                generation = state.generation
                activation = state.scope
                self._state = Releasing(generation, request, activation)

            def complete(cleanup: _CloseResult) -> ReleaseResult[RequestT, CapabilityT]:
                snapshot = self._complete_operation(generation, activation)
                if cleanup.interruption is not None:
                    raise cleanup.interruption
                return LifecycleResult(
                    snapshot, LifecycleOutcome.RELEASE_COMPLETED,
                    LifecycleDiagnostics(release_report=cleanup.report),
                )

            return activation._close().map(complete)

        return IO.defer(begin)

    def create_child(
        self,
        expected_generation: int,
        factory: Callable[[CapabilityT, ChildRequestT], Resource[ChildCapabilityT]],
    ) -> IO[CapabilityLifecycleCoordinator[ChildRequestT, ChildCapabilityT]]:
        self._validate_generation(expected_generation)
        if not callable(factory):
            raise TypeError("factory must be callable")

        def create() -> CapabilityLifecycleCoordinator[ChildRequestT, ChildCapabilityT]:
            with self._condition:
                self._reconcile_locked()
                state = self._state
                if expected_generation != state.generation or not isinstance(state, Active):
                    raise ValueError("child creation requires the requested ACTIVE generation")
                parent_capability = state.capability
                activation = state.scope

                def child_resource_factory(request: ChildRequestT) -> Resource[ChildCapabilityT]:
                    return factory(parent_capability, request)

                return CapabilityLifecycleCoordinator(child_resource_factory, scope=activation)

        return IO.delay(create)

    def _matches_locked(self, generation: int, activation: ResourceScope) -> bool:
        state = self._state
        return (
            not isinstance(state, Idle)
            and state.generation == generation
            and state.scope is activation
        )

    def _complete_operation(
        self, generation: int, activation: ResourceScope,
    ) -> LifecycleSnapshot[RequestT, CapabilityT]:
        with self._condition:
            if self._matches_locked(generation, activation):
                self._state = Idle(generation + 1)
            # A read may already have retired this activation and started the next.
            self._reconcile_locked()
            return _snapshot_from_state(self._state)

    def _reconcile_locked(self) -> None:
        state = self._state
        if isinstance(state, Idle):
            return
        if state.scope._is_closed_locked():
            self._state = Idle(state.generation + 1)
        elif not state.scope._is_open_locked():
            self._state = Releasing(state.generation, state.request, state.scope)

    @staticmethod
    def _validate_generation(expected_generation: int) -> None:
        if not isinstance(expected_generation, int) or expected_generation < 0:
            raise TypeError("expected_generation must be a non-negative int")


__all__ = ["CapabilityLifecycleCoordinator"]
