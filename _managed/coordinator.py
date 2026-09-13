from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Generic, TypeVar

from _capability.projection import CapabilityProjector
from _managed.result import (
    AcquireExisting,
    AcquireFailed,
    AcquireReleaseRequired,
    AcquireRequestMismatch,
    AcquireResult,
    AcquireSucceeded,
    Busy,
    GenerationMismatch,
    ReleaseAlreadyIdle,
    ReleaseFailed,
    ReleaseRequestMismatch,
    ReleaseResult,
    ReleaseSucceeded,
)
from _managed.snapshot import ManagedPhase, ManagedSnapshot
from _managed.state import (
    Acquiring,
    Active,
    Idle,
    ManagedState,
    ReleasePending,
    Releasing,
)
from _resource.contract import ResourceProvider
from _resource.driver import PhysicalResources
from _resource.result import ResourceAcquireFailed, ResourceAcquireSucceeded


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
PhysicalResourceT = TypeVar("PhysicalResourceT")
CapabilityT = TypeVar("CapabilityT")


def _snapshot_from_state(
    state: ManagedState[GenerationT, RequestT, PhysicalResourceT, CapabilityT],
) -> ManagedSnapshot[GenerationT, RequestT, CapabilityT]:
    """Project one internal lifecycle state into its public snapshot."""

    if isinstance(state, Idle):
        return ManagedSnapshot(state.generation)
    if isinstance(state, Active):
        return ManagedSnapshot(
            state.generation,
            state.request,
            state.capability,
            phase=ManagedPhase.ACTIVE,
        )
    if isinstance(state, Acquiring):
        return ManagedSnapshot(
            state.generation,
            state.request,
            phase=ManagedPhase.ACQUIRING,
        )
    if isinstance(state, Releasing):
        return ManagedSnapshot(
            state.generation,
            state.request,
            phase=ManagedPhase.RELEASING,
        )
    if isinstance(state, ReleasePending):
        return ManagedSnapshot(
            state.generation,
            state.request,
            phase=ManagedPhase.RELEASE_PENDING,
            last_error=state.last_error,
        )
    raise RuntimeError("unsupported Managed state")


def _normalize_resource_acquire_result(
    result: object,
) -> ResourceAcquireSucceeded[PhysicalResourceT] | ResourceAcquireFailed[PhysicalResourceT]:
    """Validate a provider result without allowing invalid resources into lifecycle state."""

    if isinstance(result, ResourceAcquireFailed):
        if not isinstance(result.resources, tuple):
            return ResourceAcquireFailed(
                TypeError(
                    "ResourceAcquireFailed.resources must be a PhysicalResources tuple"
                ),
                (),
            )
        if not isinstance(result.error, BaseException):
            return ResourceAcquireFailed(
                TypeError("ResourceAcquireFailed.error must be a BaseException"),
                result.resources,
            )
        return result

    if isinstance(result, ResourceAcquireSucceeded):
        if not isinstance(result.resources, tuple):
            return ResourceAcquireFailed(
                TypeError(
                    "ResourceAcquireSucceeded.resources must be a PhysicalResources tuple"
                ),
                (),
            )
        return result

    return ResourceAcquireFailed(
        TypeError("ResourceProvider.acquire() must return a ResourceAcquireResult"),
        (),
    )


class ManagedCoordinator(Generic[GenerationT, RequestT, PhysicalResourceT, CapabilityT]):
    """Coordinate the synchronous lifecycle of one managed capability.

    State changes use a short lock, while resource I/O, capability projection, cleanup,
    and generation issuance run outside it. Calls that observe one of those operations
    return ``Busy`` immediately; ACQUIRING includes projection and RELEASING includes
    next-generation issuance.

    Once acquisition begins, any acquisition or projection failure enters
    ``ReleasePending`` and requires an explicit ``release`` before the generation can
    complete. A release failure also enters ``ReleasePending``. If physical cleanup has
    already succeeded, a later generation-issuance failure retains no resources, so a
    retry does not clean them up twice. Non-``Exception`` interruptions are recorded in
    the same failure state before they are re-raised.
    """

    def __init__(
        self,
        issue_generation: Callable[[], GenerationT],
        resource_provider: ResourceProvider[RequestT, PhysicalResourceT],
        capability_projector: CapabilityProjector[
            RequestT, PhysicalResourceT, CapabilityT
        ],
    ) -> None:
        if not callable(issue_generation):
            raise TypeError("issue_generation must be callable")
        self._issue_generation = issue_generation
        self._resource_provider = resource_provider
        self._capability_projector = capability_projector
        self._lock = Lock()
        generation = issue_generation()
        if generation is None:
            raise TypeError("issue_generation cannot return None")
        self._state: ManagedState[
            GenerationT, RequestT, PhysicalResourceT, CapabilityT
        ] = Idle(generation)

    def read(self) -> ManagedSnapshot[GenerationT, RequestT, CapabilityT]:
        """Return one consistent point-in-time snapshot of lifecycle state."""

        with self._lock:
            return _snapshot_from_state(self._state)

    def acquire(
        self,
        expected_generation: GenerationT,
        request: RequestT,
    ) -> AcquireResult[GenerationT, RequestT, CapabilityT]:
        """Acquire and project a capability when the expected generation is idle."""

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
            if isinstance(state, Active):
                snapshot = _snapshot_from_state(state)
                if state.request == request:
                    return AcquireExisting(snapshot)
                return AcquireRequestMismatch(state.request)
            if isinstance(state, ReleasePending):
                return AcquireReleaseRequired(_snapshot_from_state(state))
            if not isinstance(state, Idle):
                raise RuntimeError("unsupported Managed state")

            generation = state.generation
            self._state = Acquiring(generation, request)

        try:
            result = self._resource_provider.acquire(request)
        except BaseException as exc:
            return self._fail_acquire_or_reraise(generation, request, (), exc)

        result = _normalize_resource_acquire_result(result)
        if isinstance(result, ResourceAcquireFailed):
            return self._fail_acquire_or_reraise(
                generation, request, result.resources, result.error
            )

        resources = result.resources
        try:
            capability = self._capability_projector.project(request, resources)
            if capability is None:
                raise TypeError("CapabilityProjector.project() cannot return None")
        except BaseException as exc:
            return self._fail_acquire_or_reraise(
                generation, request, resources, exc
            )

        snapshot = ManagedSnapshot(
            generation,
            request,
            capability,
            phase=ManagedPhase.ACTIVE,
        )
        with self._lock:
            self._state = Active(generation, request, capability, resources)
        return AcquireSucceeded(snapshot)

    def release(
        self,
        expected_generation: GenerationT,
        request: RequestT,
    ) -> ReleaseResult[GenerationT, RequestT, CapabilityT]:
        """Complete release for the matching request and issue the next generation."""

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
                return ReleaseAlreadyIdle()
            if not isinstance(state, (Active, ReleasePending)):
                raise RuntimeError("unsupported Managed state")
            if state.request != request:
                return ReleaseRequestMismatch(state.request)

            generation = state.generation
            resources = state.resources
            self._state = Releasing(generation, request, resources)

        if resources:
            try:
                self._resource_provider.release(resources)
            except BaseException as exc:
                return self._fail_release_or_reraise(
                    generation, request, resources, exc
                )

        # Cleanup is complete before generation advancement. From here on, retain an
        # empty resource tuple so a failed issuer can be retried without cleaning twice.
        try:
            next_generation = self._issue_next_generation(generation)
        except BaseException as exc:
            return self._fail_release_or_reraise(generation, request, (), exc)

        with self._lock:
            self._state = Idle(next_generation)
        return ReleaseSucceeded(next_generation)

    def _issue_next_generation(self, previous_generation: GenerationT) -> GenerationT:
        next_generation = self._issue_generation()
        if next_generation is None:
            raise TypeError("issue_generation cannot return None")
        if next_generation == previous_generation:
            raise RuntimeError("issue_generation must return a fresh generation")
        return next_generation

    def _finish_failed_acquire(
        self,
        generation: GenerationT,
        request: RequestT,
        resources: PhysicalResources[PhysicalResourceT],
        error: BaseException,
    ) -> AcquireFailed[GenerationT, RequestT, CapabilityT]:
        with self._lock:
            state = ReleasePending(generation, request, resources, error)
            self._state = state
            snapshot = _snapshot_from_state(state)
        return AcquireFailed(snapshot)

    def _fail_acquire_or_reraise(
        self,
        generation: GenerationT,
        request: RequestT,
        resources: PhysicalResources[PhysicalResourceT],
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
        resources: PhysicalResources[PhysicalResourceT],
        error: BaseException,
    ) -> ReleaseFailed[GenerationT, RequestT, CapabilityT]:
        with self._lock:
            state = ReleasePending(generation, request, resources, error)
            self._state = state
            snapshot = _snapshot_from_state(state)
        return ReleaseFailed(snapshot)

    def _fail_release_or_reraise(
        self,
        generation: GenerationT,
        request: RequestT,
        resources: PhysicalResources[PhysicalResourceT],
        error: BaseException,
    ) -> ReleaseFailed[GenerationT, RequestT, CapabilityT]:
        result = self._finish_failed_release(generation, request, resources, error)
        if not isinstance(error, Exception):
            raise error
        return result


__all__ = ["ManagedCoordinator"]
