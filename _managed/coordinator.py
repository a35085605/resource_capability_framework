from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Generic, TypeVar

from _capability.projection import CapabilityProjection
from _managed.result import (
    AcquireCommitted,
    AcquireExisting,
    AcquireFailed,
    AcquireReleaseRequired,
    AcquireRequestMismatch,
    AcquireResult,
    Busy,
    GenerationMismatch,
    ReleaseDetached,
    ReleaseFailed,
    ReleaseInactive,
    ReleaseRequestMismatch,
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
from _resource.driver import PhysicalResourceSet
from _resource.manager import ResourceManagement
from _resource.result import ResourceFailed, ResourceReady


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
PhysicalResourceT = TypeVar("PhysicalResourceT")
CapabilityT = TypeVar("CapabilityT")


class ManagedCoordinator(Generic[GenerationT, RequestT, PhysicalResourceT, CapabilityT]):
    """Own the complete synchronous lifecycle for one managed capability.

    State transitions are serialized only by a short state lock. Physical resource I/O,
    capability projection, cleanup, and generation issuance run without holding that lock.
    Concurrent lifecycle calls therefore observe ACQUIRING/RELEASING and return ``Busy``
    immediately instead of waiting for the in-progress operation.

    Once acquisition begins, every failure enters ``CleanupPending`` and requires an
    explicit ``release`` before the generation can end, even when no resources were
    acquired. Generation advances only after release cleanup has completed successfully.
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
        expected_generation: GenerationT,
        request: RequestT,
    ) -> AcquireResult[GenerationT, RequestT, CapabilityT]:
        """Begin acquisition if idle; otherwise return the current lifecycle condition."""

        if expected_generation is None:
            raise TypeError("expected_generation cannot be None")
        if request is None:
            raise TypeError("request cannot be None")

        with self._lock:
            state = self._state
            if expected_generation != state.generation:
                return GenerationMismatch(state.generation)
            if isinstance(state, Acquiring):
                return Busy(ManagedPhase.ACQUIRING)
            if isinstance(state, Releasing):
                return Busy(ManagedPhase.RELEASING)
            if isinstance(state, Current):
                snapshot = self._snapshot_locked()
                if state.request == request:
                    return AcquireExisting(snapshot)
                return AcquireRequestMismatch(state.request)
            if isinstance(state, CleanupPending):
                return AcquireReleaseRequired(self._snapshot_locked())
            if not isinstance(state, Idle):
                raise RuntimeError("unsupported Managed state")

            generation = state.generation
            self._state = Acquiring(generation, request)

        try:
            result = self._resource_manager.acquire(request)
        except BaseException as exc:
            return self._fail_acquire_or_reraise(generation, request, (), exc)

        if isinstance(result, ResourceFailed):
            if not isinstance(result.error, BaseException):
                error = TypeError("ResourceFailed.error must be a BaseException")
                return self._finish_failed_acquire(
                    generation, request, result.resources, error
                )
            if not isinstance(result.resources, tuple):
                error = TypeError("ResourceFailed.resources must be a PhysicalResourceSet tuple")
                return self._finish_failed_acquire(generation, request, (), error)
            return self._fail_acquire_or_reraise(
                generation, request, result.resources, result.error
            )

        if not isinstance(result, ResourceReady):
            return self._finish_failed_acquire(
                generation,
                request,
                (),
                TypeError("ResourceManagement.acquire() must return a ResourceAcquireResult"),
            )
        if not isinstance(result.resources, tuple):
            return self._finish_failed_acquire(
                generation,
                request,
                (),
                TypeError("ResourceReady.resources must be a PhysicalResourceSet tuple"),
            )

        resources = result.resources
        try:
            capability = self._capability_projection.project(request, resources)
            if capability is None:
                raise TypeError("CapabilityProjection.project() cannot return None")
        except BaseException as exc:
            return self._fail_acquire_or_reraise(
                generation, request, resources, exc
            )

        snapshot = Snapshot(
            generation,
            request,
            capability,
            phase=ManagedPhase.CURRENT,
        )
        with self._lock:
            self._state = Current(generation, request, capability, resources)
        return AcquireCommitted(snapshot)

    def release(
        self,
        expected_generation: GenerationT,
        request: RequestT,
    ) -> ReleaseResult[GenerationT, RequestT, CapabilityT]:
        """Release the current lifecycle without waiting for another operation."""

        if expected_generation is None:
            raise TypeError("expected_generation cannot be None")
        if request is None:
            raise TypeError("request cannot be None")

        with self._lock:
            state = self._state
            if expected_generation != state.generation:
                return GenerationMismatch(state.generation)
            if isinstance(state, Acquiring):
                return Busy(ManagedPhase.ACQUIRING)
            if isinstance(state, Releasing):
                return Busy(ManagedPhase.RELEASING)
            if isinstance(state, Idle):
                return ReleaseInactive()
            if not isinstance(state, (Current, CleanupPending)):
                raise RuntimeError("unsupported Managed state")
            if state.request != request:
                return ReleaseRequestMismatch(state.request)

            generation = state.generation
            resources = state.resources
            self._state = Releasing(generation, request, resources)

        if resources:
            try:
                self._resource_manager.release(resources)
            except BaseException as exc:
                return self._fail_release_or_reraise(
                    generation, request, resources, exc
                )

        # Cleanup is complete before generation advancement. From here on, retain an
        # empty resource set so a failed issuer can be retried without cleaning twice.
        try:
            next_generation = self._fresh_generation(generation)
        except BaseException as exc:
            return self._fail_release_or_reraise(generation, request, (), exc)

        with self._lock:
            self._state = Idle(next_generation)
        return ReleaseDetached(next_generation)

    def _snapshot_locked(self) -> Snapshot[GenerationT, RequestT, CapabilityT]:
        state = self._state
        if isinstance(state, Idle):
            return Snapshot(state.generation)
        if isinstance(state, Current):
            return Snapshot(
                state.generation,
                state.request,
                state.capability,
                phase=ManagedPhase.CURRENT,
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
        resources: PhysicalResourceSet[PhysicalResourceT],
        error: BaseException,
    ) -> AcquireFailed[GenerationT, RequestT, CapabilityT]:
        with self._lock:
            self._state = CleanupPending(generation, request, resources, error)
            snapshot = self._snapshot_locked()
        return AcquireFailed(snapshot)

    def _fail_acquire_or_reraise(
        self,
        generation: GenerationT,
        request: RequestT,
        resources: PhysicalResourceSet[PhysicalResourceT],
        error: BaseException,
    ) -> AcquireFailed[GenerationT, RequestT, CapabilityT]:
        result = self._finish_failed_acquire(generation, request, resources, error)
        if not isinstance(error, Exception):
            raise error
        return result

    def _finish_failed_release(
        self,
        generation: GenerationT,
        request: RequestT,
        resources: PhysicalResourceSet[PhysicalResourceT],
        error: BaseException,
    ) -> ReleaseFailed[GenerationT, RequestT, CapabilityT]:
        with self._lock:
            self._state = CleanupPending(generation, request, resources, error)
            snapshot = self._snapshot_locked()
        return ReleaseFailed(snapshot)

    def _fail_release_or_reraise(
        self,
        generation: GenerationT,
        request: RequestT,
        resources: PhysicalResourceSet[PhysicalResourceT],
        error: BaseException,
    ) -> ReleaseFailed[GenerationT, RequestT, CapabilityT]:
        result = self._finish_failed_release(generation, request, resources, error)
        if not isinstance(error, Exception):
            raise error
        return result


__all__ = ["ManagedCoordinator"]
