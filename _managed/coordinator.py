from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Generic, TypeVar

from _capability.projection import CapabilityProjection
from _managed.result import (
    AcquireRequestMismatch,
    AcquireBusy,
    AcquireCommitted,
    AcquireExisting,
    AcquireResult,
    GenerationMismatch,
    ReleaseRequestMismatch,
    ReleaseDetached,
    ReleaseInactive,
    ReleaseResult,
)
from _managed.snapshot import ManagedPhase, Snapshot
from _managed.state import (
    Acquiring,
    CleanupPending,
    Current,
    Idle,
    ManagedState,
    Releasing,
)
from _resource.manager import (
    ResourceAttempt,
    ResourceCleanupPendingError,
    ResourceManagement,
)
from _resource.result import ResourceBlocked, ResourceReady


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
PhysicalResourceT = TypeVar("PhysicalResourceT")
CapabilityT = TypeVar("CapabilityT")


class ManagedCoordinator(Generic[GenerationT, RequestT, PhysicalResourceT, CapabilityT]):
    """Coordinate a serial Managed lifecycle over synchronous ResourceManagement.

    Only one ``acquire`` or ``release`` operation executes at a time for this coordinator.
    Resource acquisition, capability projection, and cleanup complete synchronously.
    Cleanup failures retain the attempt in ``CleanupPending`` for an explicit retry.
    ``read`` observes progress without waiting for lifecycle I/O. Lifecycle methods
    are not reentrant: driver/projection callbacks may read this coordinator, but
    must not acquire or release it recursively.
    """

    def __init__(
        self,
        issue_generation: Callable[[], GenerationT],
        resource_manager: ResourceManagement[RequestT, PhysicalResourceT],
        capability_projection: CapabilityProjection[
            RequestT, PhysicalResourceT, CapabilityT
        ],
    ) -> None:
        if not callable(issue_generation):
            raise TypeError("issue_generation must be callable")
        self._issue_generation = issue_generation
        self._resource_manager = resource_manager
        self._capability_projection = capability_projection
        self._operation_lock = Lock()
        self._lock = Lock()
        generation = issue_generation()
        if generation is None:
            raise TypeError("issue_generation cannot return None")
        self._state: ManagedState[
            GenerationT, RequestT, PhysicalResourceT, CapabilityT
        ] = Idle(generation)

    def read(self) -> Snapshot[GenerationT, RequestT, CapabilityT]:
        """Read lifecycle progress under a short state lock, independent of I/O."""

        with self._lock:
            return self._snapshot_locked()

    def acquire(
        self,
        expected: GenerationT,
        request: RequestT,
    ) -> AcquireResult[GenerationT, RequestT, CapabilityT]:
        """Wait for this coordinator's prior operation, then acquire and project."""

        if expected is None:
            raise TypeError("expected cannot be None")
        if request is None:
            raise TypeError("request cannot be None")

        with self._operation_lock:
            with self._lock:
                state = self._state
                if expected != state.generation:
                    return GenerationMismatch(state.generation)
                if isinstance(state, Current):
                    snapshot = self._snapshot_locked()
                    if state.request == request:
                        return AcquireExisting(snapshot)
                    return AcquireRequestMismatch(state.request)
                if isinstance(state, CleanupPending):
                    return AcquireBusy()
                if not isinstance(state, Idle):
                    raise RuntimeError("unsupported Managed state")
                generation = state.generation
                self._state = Acquiring(generation, request)

            def project(resources: tuple[PhysicalResourceT, ...]) -> CapabilityT:
                capability = self._capability_projection.project(request, resources)
                if capability is None:
                    raise TypeError("CapabilityProjection.project() cannot return None")
                return capability

            attempt: ResourceAttempt[RequestT, PhysicalResourceT] | None = None
            try:
                attempt = self._resource_manager.open_attempt()
                result = attempt.acquire(request, project)
            except BaseException as exc:
                self._finish_failed_acquire(generation, request, attempt, exc)
                raise

            if isinstance(result, ResourceBlocked):
                with self._lock:
                    self._state = Idle(generation)
                return AcquireBusy()

            try:
                if not isinstance(result, ResourceReady):
                    raise RuntimeError(
                        "unsupported ResourceManagement acquire result"
                    )

                snapshot = Snapshot(
                    generation, request, result.value, phase=ManagedPhase.CURRENT
                )
                with self._lock:
                    self._state = Current(
                        generation,
                        request,
                        result.value,
                        attempt,
                    )
                return AcquireCommitted(snapshot)
            except BaseException as exc:
                # Once an attempt reports success, any later failure before commit must
                # synchronously relinquish the uncommitted resources. This also protects
                # the ResourceManagement protocol boundary from malformed implementations.
                error = exc
                try:
                    attempt.release()
                except BaseException as cleanup_error:
                    if not isinstance(cleanup_error, Exception):
                        error = cleanup_error
                    elif attempt.cleanup_pending and isinstance(exc, Exception):
                        error = ResourceCleanupPendingError(exc, cleanup_error)
                    else:
                        exc.add_note(f"resource cleanup also failed: {cleanup_error!r}")
                self._finish_failed_acquire(generation, request, attempt, error)
                if error is not exc:
                    raise error from exc
                raise

    def release(
        self,
        expected: GenerationT,
        request: RequestT,
    ) -> ReleaseResult[GenerationT, RequestT]:
        """Wait for prior acquisition/projection, then finish cleanup before success.

        On failure, read the retained request/error and explicitly retry release.
        Expected generation and request are checked after waiting for the prior operation.
        """

        if expected is None:
            raise TypeError("expected cannot be None")
        if request is None:
            raise TypeError("request cannot be None")

        with self._operation_lock:
            with self._lock:
                state = self._state
                if expected != state.generation:
                    return GenerationMismatch(state.generation)
                if isinstance(state, Idle):
                    return ReleaseInactive()
                if not isinstance(state, (Current, CleanupPending)):
                    raise RuntimeError("unsupported Managed state")
                if state.request != request:
                    return ReleaseRequestMismatch(state.request)

                generation = state.generation
                attempt = state.attempt
                # Capability authority ends before physical cleanup starts. If cleanup
                # fails, the same attempt remains here for an explicit retry.
                self._state = Releasing(generation, request, attempt)

            try:
                attempt.release()
                next_generation = self._fresh_generation(generation)
            except BaseException as exc:
                with self._lock:
                    self._state = CleanupPending(generation, request, attempt, exc)
                raise

            with self._lock:
                self._state = Idle(next_generation)
            return ReleaseDetached(next_generation)

    def _snapshot_locked(self) -> Snapshot[GenerationT, RequestT, CapabilityT]:
        state = self._state
        if isinstance(state, Idle):
            return Snapshot(state.generation)
        if isinstance(state, Current):
            return Snapshot(
                state.generation, state.request, state.capability, phase=ManagedPhase.CURRENT
            )
        if isinstance(state, Acquiring):
            return Snapshot(state.generation, state.request, phase=ManagedPhase.ACQUIRING)
        if isinstance(state, Releasing):
            return Snapshot(state.generation, state.request, phase=ManagedPhase.RELEASING)
        if isinstance(state, CleanupPending):
            return Snapshot(
                state.generation,
                state.request,
                phase=ManagedPhase.CLEANUP_PENDING,
                last_error=state.last_error,
            )
        raise RuntimeError("unsupported Managed state")

    def _fresh_generation(self, previous: GenerationT) -> GenerationT:
        next_generation = self._issue_generation()
        if next_generation is None:
            raise TypeError("issue_generation cannot return None")
        if next_generation == previous:
            raise RuntimeError("issue_generation must return a fresh generation")
        return next_generation

    def _finish_failed_acquire(
        self,
        generation: GenerationT,
        request: RequestT,
        attempt: ResourceAttempt[RequestT, PhysicalResourceT] | None,
        error: BaseException,
    ) -> None:
        cleanup_pending = attempt is not None and attempt.cleanup_pending
        with self._lock:
            if attempt is not None and cleanup_pending:
                self._state = CleanupPending(generation, request, attempt, error)
            else:
                self._state = Idle(generation)


__all__ = ["ManagedCoordinator"]
