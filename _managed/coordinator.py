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
from _managed.snapshot import Snapshot
from _managed.state import CleanupPending, Current, Idle, ManagedState
from _resource.manager import ResourceAttempt, ResourceManagement
from _resource.result import ResourceBlocked, ResourceFailed, ResourceReady


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
PhysicalResourceT = TypeVar("PhysicalResourceT")
CapabilityT = TypeVar("CapabilityT")


class ManagedCoordinator(Generic[GenerationT, RequestT, PhysicalResourceT, CapabilityT]):
    """Coordinate a serial Managed lifecycle over synchronous ResourceManagement.

    Only one ``acquire`` or ``release`` operation executes at a time for this coordinator.
    Resource acquisition, capability projection, and cleanup complete synchronously.
    Cleanup failures retain the attempt in ``CleanupPending`` for an explicit retry.
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
        self._state: ManagedState[
            GenerationT, RequestT, PhysicalResourceT, CapabilityT
        ] = Idle(issue_generation())

    def read(self) -> Snapshot[GenerationT, RequestT, CapabilityT]:
        with self._lock:
            return self._snapshot_locked()

    def acquire(
        self,
        expected: GenerationT,
        request: RequestT,
    ) -> AcquireResult[GenerationT, RequestT, CapabilityT]:
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

            attempt = self._resource_manager.open_attempt()

            def project(resources: tuple[PhysicalResourceT, ...]) -> CapabilityT:
                capability = self._capability_projection.project(request, resources)
                if capability is None:
                    raise TypeError("CapabilityProjection.project() cannot return None")
                return capability

            try:
                result = attempt.acquire(request, project)
            except BaseException:
                self._retain_cleanup_if_needed(generation, request, attempt)
                raise

            if isinstance(result, ResourceBlocked):
                return AcquireBusy()

            if isinstance(result, ResourceFailed):
                self._retain_cleanup_if_needed(generation, request, attempt)
                raise result.error

            if not isinstance(result, ResourceReady):
                raise RuntimeError("unsupported ResourceManagement acquire result")

            snapshot = Snapshot(generation, request, result.value)
            with self._lock:
                self._state = Current(
                    generation,
                    request,
                    result.value,
                    attempt,
                )
            return AcquireCommitted(snapshot)

    def release(
        self,
        expected: GenerationT,
        request: RequestT,
    ) -> ReleaseResult[GenerationT, RequestT]:
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
                self._state = CleanupPending(generation, request, attempt)

            attempt.release()

            next_generation = self._fresh_generation(generation)
            with self._lock:
                self._state = Idle(next_generation)
            return ReleaseDetached(next_generation)

    def _snapshot_locked(self) -> Snapshot[GenerationT, RequestT, CapabilityT]:
        state = self._state
        if isinstance(state, Current):
            return Snapshot(state.generation, state.request, state.capability)
        return Snapshot(state.generation)

    def _fresh_generation(self, previous: GenerationT) -> GenerationT:
        next_generation = self._issue_generation()
        if next_generation == previous:
            raise RuntimeError("issue_generation must return a fresh generation")
        return next_generation

    def _retain_cleanup_if_needed(
        self,
        generation: GenerationT,
        request: RequestT,
        attempt: ResourceAttempt[RequestT, PhysicalResourceT],
    ) -> None:
        if not attempt.cleanup_pending:
            return
        with self._lock:
            self._state = CleanupPending(generation, request, attempt)


__all__ = ["ManagedCoordinator"]
