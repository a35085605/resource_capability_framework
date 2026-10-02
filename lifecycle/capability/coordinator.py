from __future__ import annotations

from collections.abc import Callable
from threading import Lock
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
from lifecycle.resource import (
    ReleaseReport,
    Resource,
    ResourceAllocationError,
    ResourceHandle,
    ResourceRecoveryPool,
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
            state.handle.value,
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
    """Fence capability publication while delegating ownership to ResourceHandle.

    Generation is local to each coordinator and starts at zero. Resource acquisition,
    rollback, finalization, and residual handoff stay entirely in the resource layer.
    """

    __slots__ = (
        "_lock",
        "_parent_handle",
        "_pool",
        "_resource_factory",
        "_state",
    )

    def __init__(
        self,
        resource_factory: Callable[[RequestT], Resource[CapabilityT]],
        recovery_pool: ResourceRecoveryPool,
        *,
        _parent_handle: ResourceHandle[object] | None = None,
    ) -> None:
        if not callable(resource_factory):
            raise TypeError("resource_factory must be callable")
        if not isinstance(recovery_pool, ResourceRecoveryPool):
            raise TypeError("recovery_pool must be a ResourceRecoveryPool")
        if _parent_handle is not None and not isinstance(_parent_handle, ResourceHandle):
            raise TypeError("_parent_handle must be a ResourceHandle or None")

        self._resource_factory = resource_factory
        self._pool = recovery_pool
        self._parent_handle = _parent_handle
        self._lock = Lock()
        self._state: LifecycleState[RequestT, CapabilityT] = Idle(0)

    def read(self) -> LifecycleSnapshot[RequestT, CapabilityT]:
        with self._lock:
            self._reconcile_closed_active_locked()
            return _snapshot_from_state(self._state)

    def acquire(
        self,
        expected_generation: int,
        request: RequestT,
    ) -> AcquireResult[RequestT, CapabilityT]:
        self._validate_generation(expected_generation)
        if request is None:
            raise TypeError("request cannot be None")

        with self._lock:
            self._reconcile_closed_active_locked()
            state = self._state
            if expected_generation != state.generation or not isinstance(state, Idle):
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            generation = state.generation
            self._state = Acquiring(generation, request)

        try:
            resource = self._resource_factory(request)
            if not isinstance(resource, Resource):
                raise TypeError("resource_factory must return Resource")
            if self._parent_handle is None:
                handle = resource.allocate(self._pool)
            else:
                handle = self._parent_handle.allocate_child(resource)
        except ResourceAllocationError as exc:
            diagnostics = LifecycleDiagnostics(exc.cause, exc.release_report)
            return self._finish_failed_acquire(generation, diagnostics)
        except Exception as exc:
            diagnostics = LifecycleDiagnostics(exc, ReleaseReport())
            return self._finish_failed_acquire(generation, diagnostics)
        except BaseException:
            self._reset_started_operation(generation)
            raise

        if handle.value is None:
            try:
                report = handle.release()
            except BaseException:
                report = handle.release()
                self._reset_started_operation(generation)
                raise
            diagnostics = LifecycleDiagnostics(
                TypeError("capability resource cannot produce None"),
                report,
            )
            return self._finish_failed_acquire(generation, diagnostics)

        with self._lock:
            self._state = Active(generation, request, handle)
            snapshot = _snapshot_from_state(self._state)
        return LifecycleResult(snapshot, LifecycleOutcome.ACQUIRE_SUCCEEDED)

    def release(
        self,
        expected_generation: int,
        request: RequestT,
    ) -> ReleaseResult[RequestT, CapabilityT]:
        self._validate_generation(expected_generation)
        if request is None:
            raise TypeError("request cannot be None")

        with self._lock:
            self._reconcile_closed_active_locked()
            state = self._state
            if expected_generation != state.generation or not isinstance(state, Active):
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            if state.request != request:
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            generation = state.generation
            handle = state.handle
            self._state = Releasing(generation, request, handle)

        try:
            report = handle.release()
        except BaseException as exc:
            # ResourceHandle completes the release and caches its report before a
            # control-flow interruption is propagated to the initiating caller.
            if isinstance(exc, Exception):
                self._reset_started_operation(generation)
                raise
            report = handle.release()
            self._reset_started_operation(generation)
            raise

        with self._lock:
            self._state = Idle(generation + 1)
            snapshot = _snapshot_from_state(self._state)
        return LifecycleResult(
            snapshot,
            LifecycleOutcome.RELEASE_COMPLETED,
            LifecycleDiagnostics(release_report=report),
        )

    def create_child(
        self,
        expected_generation: int,
        factory: Callable[[CapabilityT, ChildRequestT], Resource[ChildCapabilityT]],
    ) -> "CapabilityLifecycleCoordinator[ChildRequestT, ChildCapabilityT]":
        self._validate_generation(expected_generation)
        if not callable(factory):
            raise TypeError("factory must be callable")

        with self._lock:
            self._reconcile_closed_active_locked()
            state = self._state
            if expected_generation != state.generation or not isinstance(state, Active):
                raise ValueError("child creation requires the requested ACTIVE generation")
            parent_capability = state.handle.value
            parent_handle = cast(ResourceHandle[object], state.handle)

        def child_resource_factory(request: ChildRequestT) -> Resource[ChildCapabilityT]:
            return factory(parent_capability, request)

        return CapabilityLifecycleCoordinator(
            child_resource_factory,
            self._pool,
            _parent_handle=parent_handle,
        )

    def _finish_failed_acquire(
        self,
        generation: int,
        diagnostics: LifecycleDiagnostics,
    ) -> AcquireResult[RequestT, CapabilityT]:
        with self._lock:
            self._state = Idle(generation + 1)
            snapshot = _snapshot_from_state(self._state)
        return LifecycleResult(snapshot, LifecycleOutcome.ACQUIRE_FAILED, diagnostics)

    def _reset_started_operation(self, generation: int) -> None:
        with self._lock:
            self._state = Idle(generation + 1)

    def _reconcile_closed_active_locked(self) -> None:
        state = self._state
        if isinstance(state, Active) and state.handle.closed:
            self._state = Idle(state.generation + 1)

    @staticmethod
    def _validate_generation(expected_generation: int) -> None:
        if not isinstance(expected_generation, int) or expected_generation < 0:
            raise TypeError("expected_generation must be a non-negative int")


__all__ = ["CapabilityLifecycleCoordinator"]
