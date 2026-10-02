from __future__ import annotations

from collections.abc import Callable
from typing import Generic, TypeVar

from lifecycle.capability.diagnostics import LifecycleDiagnostics
from lifecycle.capability.result import (
    AcquireResult,
    LifecycleOutcome,
    LifecycleResult,
    ReleaseResult,
)
from lifecycle.capability.snapshot import LifecyclePhase, LifecycleSnapshot
from lifecycle.capability.state import Acquiring, Active, Idle, LifecycleState, Releasing
from lifecycle.resource import (
    ReleaseReport,
    Resource,
    ResourceAllocationError,
    ResourceScope,
    ScopeClosedError,
)


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
    """Publish generations and capabilities under the owning scope's control lock.

    Each accepted activation owns a fresh child scope. Resource acquisition, rollback,
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

    def read(self) -> LifecycleSnapshot[RequestT, CapabilityT]:
        with self._condition:
            self._reconcile_locked()
            return _snapshot_from_state(self._state)

    def acquire(
        self,
        expected_generation: int,
        request: RequestT,
    ) -> AcquireResult[RequestT, CapabilityT]:
        self._validate_generation(expected_generation)
        if request is None:
            raise TypeError("request cannot be None")

        with self._condition:
            self._reconcile_locked()
            state = self._state
            if expected_generation != state.generation or not isinstance(state, Idle):
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            generation = state.generation
            try:
                activation = self._scope.child()
            except ScopeClosedError as exc:
                self._state = Idle(generation + 1)
                return LifecycleResult(
                    _snapshot_from_state(self._state),
                    LifecycleOutcome.ACQUIRE_FAILED,
                    LifecycleDiagnostics(exc, ReleaseReport()),
                )
            self._state = Acquiring(generation, request, activation)

        try:
            # Run the factory and capability validation inside the acquisition batch.
            # Shutdown must wait for a factory already using its parent capability.
            resource = Resource.pure(request).flat_map(self._resource_for_request)
            capability = activation.acquire(resource)
        except BaseException as exc:
            cleanup = activation._close()
            snapshot = self._complete_operation(generation, activation)
            if not isinstance(exc, Exception):
                raise
            if cleanup.interruption is not None:
                raise cleanup.interruption from exc
            if isinstance(exc, ResourceAllocationError):
                # This activation has exactly one failed batch. Its rollback report
                # may also be in the concurrent close report; count it only once.
                diagnostics = LifecycleDiagnostics(exc.cause, exc.release_report)
            else:
                diagnostics = LifecycleDiagnostics(exc, cleanup.report)
            return LifecycleResult(snapshot, LifecycleOutcome.ACQUIRE_FAILED, diagnostics)

        with self._condition:
            if (
                self._matches_locked(generation, activation)
                and isinstance(self._state, Acquiring)
                and activation._is_open_locked()
            ):
                self._state = Active(generation, request, capability, activation)
                return LifecycleResult(
                    _snapshot_from_state(self._state),
                    LifecycleOutcome.ACQUIRE_SUCCEEDED,
                )

        # Shutdown won the publication race, possibly after the batch committed.
        # Finish cleanup before reporting failure and never publish the closed value.
        cleanup = activation._close()
        snapshot = self._complete_operation(generation, activation)
        if cleanup.interruption is not None:
            raise cleanup.interruption
        return LifecycleResult(
            snapshot,
            LifecycleOutcome.ACQUIRE_FAILED,
            LifecycleDiagnostics(
                ScopeClosedError("activation closed before capability publication"),
                cleanup.report,
            ),
        )

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
    ) -> ReleaseResult[RequestT, CapabilityT]:
        self._validate_generation(expected_generation)
        if request is None:
            raise TypeError("request cannot be None")

        with self._condition:
            self._reconcile_locked()
            state = self._state
            if expected_generation != state.generation or not isinstance(state, Active):
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            if state.request != request:
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            generation = state.generation
            activation = state.scope
            self._state = Releasing(generation, request, activation)

        cleanup = activation._close()
        snapshot = self._complete_operation(generation, activation)
        if cleanup.interruption is not None:
            raise cleanup.interruption
        return LifecycleResult(
            snapshot,
            LifecycleOutcome.RELEASE_COMPLETED,
            LifecycleDiagnostics(release_report=cleanup.report),
        )

    def create_child(
        self,
        expected_generation: int,
        factory: Callable[[CapabilityT, ChildRequestT], Resource[ChildCapabilityT]],
    ) -> "CapabilityLifecycleCoordinator[ChildRequestT, ChildCapabilityT]":
        self._validate_generation(expected_generation)
        if not callable(factory):
            raise TypeError("factory must be callable")

        with self._condition:
            self._reconcile_locked()
            state = self._state
            if expected_generation != state.generation or not isinstance(state, Active):
                raise ValueError("child creation requires the requested ACTIVE generation")
            parent_capability = state.capability
            activation = state.scope

            def child_resource_factory(request: ChildRequestT) -> Resource[ChildCapabilityT]:
                return factory(parent_capability, request)

            return CapabilityLifecycleCoordinator(
                child_resource_factory,
                scope=activation,
            )

    def _matches_locked(self, generation: int, activation: ResourceScope) -> bool:
        state = self._state
        return (
            not isinstance(state, Idle)
            and state.generation == generation
            and state.scope is activation
        )

    def _complete_operation(
        self,
        generation: int,
        activation: ResourceScope,
    ) -> LifecycleSnapshot[RequestT, CapabilityT]:
        with self._condition:
            if self._matches_locked(generation, activation):
                self._state = Idle(generation + 1)
            # A read may already have retired the old activation and a caller may
            # have started the next generation while this operation was finishing.
            self._reconcile_locked()
            return _snapshot_from_state(self._state)

    def _reconcile_locked(self) -> None:
        state = self._state
        if isinstance(state, Idle):
            return
        if state.scope.closed:
            self._state = Idle(state.generation + 1)
        elif not state.scope._is_open_locked():
            self._state = Releasing(state.generation, state.request, state.scope)

    @staticmethod
    def _validate_generation(expected_generation: int) -> None:
        if not isinstance(expected_generation, int) or expected_generation < 0:
            raise TypeError("expected_generation must be a non-negative int")


__all__ = ["CapabilityLifecycleCoordinator"]
