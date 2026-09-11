from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Generic, Hashable, TypeVar

from _attempt import AttemptToken
from _managed.adapter import Adapter
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
AccessKeyT = TypeVar("AccessKeyT", bound=Hashable)
ResourceT = TypeVar("ResourceT")
CapabilityT = TypeVar("CapabilityT")


class ManagedCoordinator(
    Generic[GenerationT, AccessT, AccessKeyT, ResourceT, CapabilityT]
):
    """Coordinate Managed authority while ResourceManagement owns Resources.

    Managed state contains only generation, logical Access identity, attempt
    authority, and published Capability.  Resource reservation, physical I/O,
    retention, interruption, retirement, and cleanup stay below the
    ResourceManagement boundary.
    """

    def __init__(
        self,
        issue_generation: Callable[[], GenerationT],
        adapter: Adapter[AccessT, AccessKeyT, ResourceT, CapabilityT],
    ) -> None:
        if not callable(issue_generation):
            raise TypeError("issue_generation must be callable")
        self._issue_generation = issue_generation
        self._adapter = adapter
        self._lock = Lock()
        self._state: ManagedState[GenerationT, AccessT, AccessKeyT, CapabilityT] = (
            Idle(issue_generation())
        )

    @property
    def adapter(self) -> Adapter[AccessT, AccessKeyT, ResourceT, CapabilityT]:
        return self._adapter

    @property
    def resource_manager(self) -> ResourceManagement[AccessT, ResourceT]:
        return self._adapter.resource_manager

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

        access_key = self._access_key(access)

        with self._lock:
            state = self._state
            if expected != state.generation:
                return GenerationMismatch(state.generation)
            if isinstance(state, Current):
                snapshot = self._snapshot_locked()
                if state.access_key == access_key:
                    return AcquireExisting(snapshot)
                return AcquireAccessMismatch(state.access)
            if isinstance(state, Preparing):
                return AcquireBusy()
            if not isinstance(state, Idle):
                raise RuntimeError("unsupported Managed state")

            attempt = AttemptToken()
            self._state = Preparing(
                state.generation,
                access,
                access_key,
                attempt,
            )

        manager = self._adapter.resource_manager
        try:
            result = manager.acquire(attempt, access)

            if attempt.cancelled:
                manager.release(attempt)
                return self._finish_superseded(attempt)

            if isinstance(result, ResourceBlocked):
                abandoned, current_generation = self._abandon_if_current(attempt)
                if abandoned:
                    return AcquireBusy()
                return AcquireSuperseded(current_generation)

            if isinstance(result, ResourceFailed):
                abandoned, current_generation = self._abandon_if_current(attempt)
                if attempt.cancelled or not abandoned:
                    manager.release(attempt)
                    return AcquireSuperseded(current_generation)
                attempt.cancel()
                manager.release(attempt)
                raise result.error

            if not isinstance(result, ResourceAcquired):
                raise RuntimeError("unsupported ResourceManagement acquire result")

            capability = self._adapter.capability_projection.project(
                access,
                result.resources,
            )
            if capability is None:
                raise TypeError("CapabilityProjection.project() cannot return None")

            with self._lock:
                state = self._state
                owns_authority = (
                    isinstance(state, Preparing)
                    and state.attempt is attempt
                    and not attempt.cancelled
                )
                if owns_authority:
                    snapshot = Snapshot(state.generation, access, capability)
                    self._state = Current(
                        state.generation,
                        access,
                        state.access_key,
                        capability,
                        attempt,
                    )
                    return AcquireCommitted(snapshot)
                current_generation = state.generation

            manager.release(attempt)
            return AcquireSuperseded(current_generation)

        except BaseException as exc:
            self._abandon_if_current(attempt)
            attempt.cancel()
            try:
                manager.release(attempt)
            except Exception as release_error:
                if isinstance(exc, Exception):
                    exc.add_note(f"resource release also failed: {release_error!r}")
            raise
        finally:
            manager.finish_acquire(attempt)

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

        access_key = self._access_key(access)
        next_generation: GenerationT | None = None
        attempt: AttemptToken | None = None
        preparing = False

        with self._lock:
            state = self._state
            if expected != state.generation:
                return GenerationMismatch(state.generation)
            if isinstance(state, Idle):
                return ReleaseInactive()

            if isinstance(state, Preparing):
                if state.access_key != access_key:
                    return ReleaseAccessMismatch(state.access)
                next_generation = self._fresh_generation(state.generation)
                attempt = state.attempt
                preparing = True
            else:
                if not isinstance(state, Current):
                    raise RuntimeError("unsupported Managed state")
                if state.access_key != access_key:
                    return ReleaseAccessMismatch(state.access)
                next_generation = self._fresh_generation(state.generation)
                attempt = state.attempt

            attempt.cancel()
            self._state = Idle(next_generation)

        # Detach Managed authority before physical interruption/cleanup.
        assert attempt is not None
        assert next_generation is not None
        self._adapter.resource_manager.release(attempt)
        if preparing:
            return ReleaseAcquisitionRevoked(next_generation)
        return ReleaseDetached(next_generation)

    def cleanup_retired(self, access: AccessT) -> bool:
        if access is None:
            raise TypeError("access cannot be None")
        return self._adapter.resource_manager.cleanup_retired(access)

    def _access_key(self, access: AccessT) -> AccessKeyT:
        access_key = self._adapter.access_identity.key(access)
        if access_key is None:
            raise TypeError("AccessIdentity.key() cannot return None")
        try:
            hash(access_key)
        except TypeError as exc:
            raise TypeError("AccessIdentity.key() must return a hashable value") from exc
        return access_key

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
        attempt: AttemptToken,
    ) -> tuple[bool, GenerationT]:
        with self._lock:
            state = self._state
            if isinstance(state, Preparing) and state.attempt is attempt:
                self._state = Idle(state.generation)
                return True, state.generation
            return False, state.generation

    def _finish_superseded(
        self,
        attempt: AttemptToken,
    ) -> AcquireSuperseded[GenerationT]:
        _abandoned, current_generation = self._abandon_if_current(attempt)
        return AcquireSuperseded(current_generation)


__all__ = ["ManagedCoordinator"]
