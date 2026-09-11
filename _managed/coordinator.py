from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Generic, TypeVar

from _attempt import AttemptId
from _capability.projection import CapabilityProjection
from _managed.result import (
    AcquireAccessMismatch,
    AcquireBusy,
    AcquireCommitted,
    AcquireExisting,
    AcquireResult,
    AcquireSuperseded,
    GenerationMismatch,
    ReleaseAccessMismatch,
    ReleaseAcquisitionRevoked,
    ReleaseDetached,
    ReleaseInactive,
    ReleaseResult,
)
from _managed.snapshot import Snapshot
from _managed.state import Current, Idle, ManagedState, Preparing
from _resource.manager import ResourceManagement
from _resource.result import ResourceAcquired, ResourceBlocked, ResourceFailed


GenerationT = TypeVar("GenerationT")
AccessT = TypeVar("AccessT")
ResourceT = TypeVar("ResourceT")
CapabilityT = TypeVar("CapabilityT")


class ManagedCoordinator(Generic[GenerationT, AccessT, ResourceT, CapabilityT]):
    """Coordinate Managed authority while ResourceManagement owns Resources.

    Managed state contains only generation, Access value, opaque attempt identity,
    and published Capability.  Resource attempt lifecycle, reservation, physical
    I/O, interruption, retention, retirement, and cleanup stay below the
    ResourceManagement boundary.
    """

    def __init__(
        self,
        issue_generation: Callable[[], GenerationT],
        resource_manager: ResourceManagement[AccessT, ResourceT],
        capability_projection: CapabilityProjection[
            AccessT, ResourceT, CapabilityT
        ],
    ) -> None:
        if not callable(issue_generation):
            raise TypeError("issue_generation must be callable")
        self._issue_generation = issue_generation
        self._resource_manager = resource_manager
        self._capability_projection = capability_projection
        self._lock = Lock()
        self._state: ManagedState[GenerationT, AccessT, CapabilityT] = (
            Idle(issue_generation())
        )

    def read(self) -> Snapshot[GenerationT, AccessT, CapabilityT]:
        with self._lock:
            return self._snapshot_locked()

    def acquire(
        self,
        expected: GenerationT,
        access: AccessT,
    ) -> AcquireResult[GenerationT, AccessT, CapabilityT]:
        if expected is None:
            raise TypeError("expected cannot be None")
        if access is None:
            raise TypeError("access cannot be None")

        with self._lock:
            if expected != self._state.generation:
                return GenerationMismatch(self._state.generation)

        manager = self._resource_manager
        with self._lock:
            state = self._state
            if expected != state.generation:
                return GenerationMismatch(state.generation)
            if isinstance(state, Current):
                snapshot = self._snapshot_locked()
                if state.access == access:
                    return AcquireExisting(snapshot)
                return AcquireAccessMismatch(state.access)
            if isinstance(state, Preparing):
                return AcquireBusy()
            if not isinstance(state, Idle):
                raise RuntimeError("unsupported Managed state")

            attempt_id = manager.open_attempt()
            self._state = Preparing(
                state.generation,
                access,
                attempt_id,
            )

        try:
            result = manager.acquire(attempt_id, access)

            if isinstance(result, ResourceBlocked):
                abandoned, current_generation = self._abandon_if_current(attempt_id)
                if abandoned:
                    return AcquireBusy()
                return AcquireSuperseded(current_generation)

            if isinstance(result, ResourceFailed):
                abandoned, current_generation = self._abandon_if_current(attempt_id)
                if not abandoned:
                    manager.release(attempt_id)
                    return AcquireSuperseded(current_generation)
                manager.release(attempt_id)
                raise result.error

            if not isinstance(result, ResourceAcquired):
                raise RuntimeError("unsupported ResourceManagement acquire result")

            capability = self._capability_projection.project(
                access,
                result.resources,
            )
            if capability is None:
                raise TypeError("CapabilityProjection.project() cannot return None")

            with self._lock:
                state = self._state
                owns_authority = (
                    isinstance(state, Preparing)
                    and state.attempt_id is attempt_id
                )
                if owns_authority:
                    snapshot = Snapshot(state.generation, access, capability)
                    self._state = Current(
                        state.generation,
                        access,
                        capability,
                        attempt_id,
                    )
                    return AcquireCommitted(snapshot)
                current_generation = state.generation

            manager.release(attempt_id)
            return AcquireSuperseded(current_generation)

        except BaseException as exc:
            self._abandon_if_current(attempt_id)
            try:
                manager.release(attempt_id)
            except Exception as release_error:
                if isinstance(exc, Exception):
                    exc.add_note(f"resource release also failed: {release_error!r}")
            raise
        finally:
            manager.finish_acquire(attempt_id)

    def release(
        self,
        expected: GenerationT,
        access: AccessT,
    ) -> ReleaseResult[GenerationT, AccessT]:
        if expected is None:
            raise TypeError("expected cannot be None")
        if access is None:
            raise TypeError("access cannot be None")

        with self._lock:
            state = self._state
            if expected != state.generation:
                return GenerationMismatch(state.generation)
            if isinstance(state, Idle):
                return ReleaseInactive()

        next_generation: GenerationT | None = None
        attempt_id: AttemptId | None = None
        preparing = False

        with self._lock:
            state = self._state
            if expected != state.generation:
                return GenerationMismatch(state.generation)
            if isinstance(state, Idle):
                return ReleaseInactive()

            if isinstance(state, Preparing):
                if state.access != access:
                    return ReleaseAccessMismatch(state.access)
                next_generation = self._fresh_generation(state.generation)
                attempt_id = state.attempt_id
                preparing = True
            else:
                if not isinstance(state, Current):
                    raise RuntimeError("unsupported Managed state")
                if state.access != access:
                    return ReleaseAccessMismatch(state.access)
                next_generation = self._fresh_generation(state.generation)
                attempt_id = state.attempt_id

            self._state = Idle(next_generation)

        # Detach Managed authority before retiring the Resource attempt.  Physical
        # interruption and cleanup continue independently below this boundary.
        assert attempt_id is not None
        assert next_generation is not None
        self._resource_manager.release(attempt_id)
        if preparing:
            return ReleaseAcquisitionRevoked(next_generation)
        return ReleaseDetached(next_generation)

    def _snapshot_locked(self) -> Snapshot[GenerationT, AccessT, CapabilityT]:
        state = self._state
        if isinstance(state, Current):
            return Snapshot(state.generation, state.access, state.capability)
        return Snapshot(state.generation)

    def _fresh_generation(self, previous: GenerationT) -> GenerationT:
        next_generation = self._issue_generation()
        if next_generation == previous:
            raise RuntimeError("issue_generation must return a fresh generation")
        return next_generation

    def _abandon_if_current(
        self,
        attempt_id: AttemptId,
    ) -> tuple[bool, GenerationT]:
        with self._lock:
            state = self._state
            if isinstance(state, Preparing) and state.attempt_id is attempt_id:
                self._state = Idle(state.generation)
                return True, state.generation
            return False, state.generation


__all__ = ["ManagedCoordinator"]
