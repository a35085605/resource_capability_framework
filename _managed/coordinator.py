from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Generic, Hashable, TypeVar

from adb._managed.adapter import Adapter
from adb._managed.result import (
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
from adb._managed.snapshot import Snapshot
from adb._managed.state import Current, Idle, ManagedAttempt, ManagedState, Preparing
from adb._resource.driver import ResourceSet
from adb._resource.manager import ResourceAcquisition, ResourceManagement
from adb._resource.pool import ResourceLease


GenerationT = TypeVar("GenerationT")
AccessT = TypeVar("AccessT")
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")
CapabilityT = TypeVar("CapabilityT")


class ManagedCoordinator(
    Generic[GenerationT, AccessT, SpecT, ResourceT, CapabilityT]
):
    """Coordinate Access authority without owning physical-resource mechanics.

    Authority cancellation and physical interruption are independent. The
    coordinator owns generation/Access/Capability state and delegates the full
    resource transaction to ``ResourceManagement`` using opaque acquisition and
    lease handles.

    Simplified new-resource acquire sequence::

        Access -> ResourcePlan -> claim -> acquire -> Capability
        -> commit lease + Current

    SHARED reuse may return an existing lease directly from ``claim``.
    """

    def __init__(
        self,
        issue_generation: Callable[[], GenerationT],
        adapter: Adapter[AccessT, SpecT, ResourceT, CapabilityT],
    ) -> None:
        if not callable(issue_generation):
            raise TypeError("issue_generation must be callable")
        self._issue_generation = issue_generation
        self._adapter = adapter
        self._lock = Lock()
        self._state: ManagedState[
            GenerationT, AccessT, SpecT, ResourceT, CapabilityT
        ] = Idle(issue_generation())

    @property
    def adapter(self) -> Adapter[AccessT, SpecT, ResourceT, CapabilityT]:
        return self._adapter

    @property
    def resource_manager(self) -> ResourceManagement[SpecT, ResourceT]:
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

            attempt = ManagedAttempt(
                generation=state.generation,
                access=access,
            )
            self._state = Preparing(state.generation, access, attempt)

        manager = self._adapter.resource_manager
        acquisition: ResourceAcquisition[SpecT, ResourceT] | None = None
        lease: ResourceLease[Hashable, ResourceT] | None = None
        resources: ResourceSet[ResourceT] | None = None

        try:
            plan = self._adapter.access_model.resource_plan(access)

            if attempt.revoked:
                return self._finish_superseded(attempt)

            claim = manager.claim(plan)
            if claim is None:
                abandoned, current_generation = self._abandon_if_current(attempt)
                if abandoned:
                    return AcquireBusy()
                return AcquireSuperseded(current_generation)

            if isinstance(claim, ResourceLease):
                lease = claim
                resources = claim.resources
            else:
                acquisition = claim

                # Publish only the opaque acquisition handle into Preparing. If
                # authority was revoked between claim and this handoff, no physical
                # producer has started and the claim can be cancelled safely.
                with self._lock:
                    state = self._state
                    owns_preparing = (
                        isinstance(state, Preparing)
                        and state.attempt is attempt
                        and not attempt.revoked
                    )
                    if owns_preparing:
                        self._state = Preparing(
                            state.generation,
                            state.access,
                            state.attempt,
                            acquisition,
                        )
                    current_generation = state.generation

                if not owns_preparing:
                    manager.cancel(acquisition)
                    acquisition = None
                    return AcquireSuperseded(current_generation)

            if attempt.revoked:
                if lease is not None:
                    manager.release(lease)
                    lease = None
                elif acquisition is not None:
                    # No physical producer has started yet.
                    manager.cancel(acquisition)
                    acquisition = None
                return self._finish_superseded(attempt)

            if resources is None:
                assert acquisition is not None
                resources = manager.acquire(acquisition)

                if attempt.revoked:
                    current_generation = self._current_generation()
                    manager.abandon(acquisition, resources)
                    acquisition = None
                    return AcquireSuperseded(current_generation)

            capability = self._adapter.capability_projection.project(access, resources)
            if capability is None:
                raise TypeError("CapabilityProjection.project() cannot return None")

            with self._lock:
                state = self._state
                owns_authority = (
                    isinstance(state, Preparing)
                    and state.attempt is attempt
                    and not attempt.revoked
                )
                if owns_authority:
                    if lease is None:
                        assert acquisition is not None
                        assert resources is not None
                        lease = manager.commit(acquisition, resources)
                        acquisition = None
                    snapshot = Snapshot(state.generation, access, capability)
                    self._state = Current(
                        state.generation,
                        access,
                        capability,
                        lease,
                    )
                    return AcquireCommitted(snapshot)
                current_generation = state.generation

            if lease is not None:
                manager.release(lease)
                lease = None
            elif acquisition is not None:
                manager.abandon(acquisition, resources)
                acquisition = None
            return AcquireSuperseded(current_generation)

        except BaseException:
            self._abandon_if_current(attempt)
            if lease is not None:
                manager.release(lease)
            elif acquisition is not None:
                manager.abandon(acquisition, resources)
            raise
        finally:
            attempt.finish()

    def release(
        self,
        expected: GenerationT,
        access: AccessT,
    ) -> ReleaseResult[GenerationT, AccessT]:
        if expected is None:
            raise TypeError("expected cannot be None")
        if access is None:
            raise TypeError("access cannot be None")

        acquisition: ResourceAcquisition[SpecT, ResourceT] | None = None
        lease: ResourceLease[Hashable, ResourceT] | None = None
        next_generation: GenerationT | None = None
        revoked_acquisition = False

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
                state.attempt.revoke()
                acquisition = state.acquisition
                self._state = Idle(next_generation)
                revoked_acquisition = True
            else:
                if not isinstance(state, Current):
                    raise RuntimeError("unsupported Managed state")
                if state.access != access:
                    return ReleaseAccessMismatch(state.access)

                next_generation = self._fresh_generation(state.generation)
                lease = state.resource_lease
                self._state = Idle(next_generation)

        # Managed authority is detached before any physical operation. Backend
        # interruption/cleanup therefore cannot block reads or generation changes.
        assert next_generation is not None
        manager = self._adapter.resource_manager
        if revoked_acquisition:
            if acquisition is not None:
                manager.interrupt(acquisition)
            return ReleaseAcquisitionRevoked(next_generation)

        assert lease is not None
        manager.release(lease)
        return ReleaseDetached(next_generation)

    def cleanup_retired(self, access: AccessT) -> bool:
        """Retry cleanup for retired resources described by this Access."""

        if access is None:
            raise TypeError("access cannot be None")
        plan = self._adapter.access_model.resource_plan(access)
        return self._adapter.resource_manager.cleanup_retired(plan)

    def _snapshot_locked(self) -> Snapshot[GenerationT, AccessT, CapabilityT]:
        state = self._state
        if isinstance(state, Current):
            return Snapshot(state.generation, state.access, state.capability)
        return Snapshot(state.generation)

    def _current_generation(self) -> GenerationT:
        with self._lock:
            return self._state.generation

    def _fresh_generation(self, previous: GenerationT) -> GenerationT:
        next_generation = self._issue_generation()
        if next_generation == previous:
            raise RuntimeError("issue_generation must return a fresh generation")
        return next_generation

    def _abandon_if_current(
        self,
        attempt: ManagedAttempt[GenerationT, AccessT],
    ) -> tuple[bool, GenerationT]:
        with self._lock:
            state = self._state
            if isinstance(state, Preparing) and state.attempt is attempt:
                self._state = Idle(state.generation)
                return True, state.generation
            return False, state.generation

    def _finish_superseded(
        self,
        attempt: ManagedAttempt[GenerationT, AccessT],
    ) -> AcquireSuperseded[GenerationT]:
        _abandoned, current_generation = self._abandon_if_current(attempt)
        return AcquireSuperseded(current_generation)


__all__ = ["ManagedCoordinator"]
