from __future__ import annotations

from dataclasses import dataclass
from threading import Lock, Thread
from typing import Generic, Protocol, TypeVar

from _attempt import AttemptId
from _resource.driver import (
    PhysicalAcquired,
    PhysicalAcquisition,
    PhysicalFailed,
    PhysicalInterrupted,
    ResourceDriver,
    ResourceSet,
)
from _resource.pool import GLOBAL_RESOURCE_POOL, ResourcePool, RetiredResource
from _resource.requirement import ResourceRequirements
from _resource.result import (
    ResourceAcquireResult,
    ResourceAcquired,
    ResourceBlocked,
    ResourceFailed,
)


AccessT = TypeVar("AccessT")
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")


class ResourceRequirementsModel(Protocol[AccessT, SpecT]):
    """Resolve only the physical Resource requirements for one Access."""

    def requirements(self, access: AccessT) -> ResourceRequirements[SpecT]: ...


class ResourceManagement(Protocol[AccessT, ResourceT]):
    """Managed-facing boundary for the complete Resource acquisition lifecycle.

    ``open_attempt`` registers an opaque identity before Managed publishes it, so a
    concurrent ``release`` cannot be lost. ``release`` revokes retention authority
    and requests interruption of any current physical acquisition without waiting
    for that acquisition or cleanup to complete. Acquired Resources remain pinned
    until ``finish_acquire`` while Capability projection is using them.
    """

    def open_attempt(self) -> AttemptId: ...

    def acquire(
        self,
        attempt_id: AttemptId,
        access: AccessT,
    ) -> ResourceAcquireResult[ResourceT]: ...

    def release(self, attempt_id: AttemptId) -> None: ...

    def finish_acquire(self, attempt_id: AttemptId) -> None: ...


class ResourceAcquisitionCancelled(RuntimeError):
    """Internal operational failure used when an acquisition is cancelled."""


@dataclass(slots=True)
class _AttemptContext(Generic[AccessT, SpecT, ResourceT]):
    attempt_id: AttemptId
    access: AccessT
    requirements: ResourceRequirements[SpecT] | None = None
    resources: ResourceSet[ResourceT] = ()
    current_physical: PhysicalAcquisition[ResourceT] | None = None
    reserved: bool = False
    cancel_requested: bool = False


@dataclass(frozen=True, slots=True)
class _Pending:
    attempt_id: AttemptId


@dataclass(frozen=True, slots=True)
class _Cancelled:
    attempt_id: AttemptId


@dataclass(slots=True)
class _Acquiring(Generic[AccessT, SpecT, ResourceT]):
    context: _AttemptContext[AccessT, SpecT, ResourceT]


@dataclass(slots=True)
class _Pinned(Generic[AccessT, SpecT, ResourceT]):
    context: _AttemptContext[AccessT, SpecT, ResourceT]


@dataclass(slots=True)
class _Retained(Generic[AccessT, SpecT, ResourceT]):
    context: _AttemptContext[AccessT, SpecT, ResourceT]


@dataclass(slots=True)
class _RetiredPinned(Generic[AccessT, SpecT, ResourceT]):
    context: _AttemptContext[AccessT, SpecT, ResourceT]
    retired: RetiredResource[AccessT, SpecT, ResourceT]


@dataclass(slots=True)
class _Retired(Generic[AccessT, SpecT, ResourceT]):
    context: _AttemptContext[AccessT, SpecT, ResourceT] | None
    retired: RetiredResource[AccessT, SpecT, ResourceT]
    cleanup_in_progress: bool = False


type _AttemptState[A, S, R] = (
    _Pending
    | _Cancelled
    | _Acquiring[A, S, R]
    | _Pinned[A, S, R]
    | _Retained[A, S, R]
    | _RetiredPinned[A, S, R]
    | _Retired[A, S, R]
)


class ResourceManager(Generic[AccessT, SpecT, ResourceT]):
    """Own Resource reservation, terminal physical outcomes, and cleanup."""

    def __init__(
        self,
        requirements_model: ResourceRequirementsModel[AccessT, SpecT],
        driver: ResourceDriver[SpecT, ResourceT],
        *,
        resource_pool: ResourcePool[AccessT, SpecT, ResourceT] = GLOBAL_RESOURCE_POOL,
    ) -> None:
        self._requirements_model = requirements_model
        self._driver = driver
        if not isinstance(resource_pool, ResourcePool):
            raise TypeError("resource_pool must be ResourcePool")
        self._resource_pool = resource_pool
        self._lock = Lock()
        self._attempts: dict[
            AttemptId, _AttemptState[AccessT, SpecT, ResourceT]
        ] = {}

    @property
    def requirements_model(self) -> ResourceRequirementsModel[AccessT, SpecT]:
        return self._requirements_model

    @property
    def driver(self) -> ResourceDriver[SpecT, ResourceT]:
        return self._driver

    @property
    def resource_pool(self) -> ResourcePool[AccessT, SpecT, ResourceT]:
        return self._resource_pool

    def open_attempt(self) -> AttemptId:
        """Create and register one Pending acquisition identity."""

        attempt_id = AttemptId()
        with self._lock:
            self._attempts[attempt_id] = _Pending(attempt_id)
        return attempt_id

    def acquire(
        self,
        attempt_id: AttemptId,
        access: AccessT,
    ) -> ResourceAcquireResult[ResourceT]:
        self._validate_attempt_id(attempt_id)
        if access is None:
            raise TypeError("access cannot be None")

        with self._lock:
            state = self._attempts.get(attempt_id)
            if isinstance(state, _Cancelled):
                return ResourceFailed(
                    ResourceAcquisitionCancelled("attempt was cancelled")
                )
            if state is None:
                raise RuntimeError("attempt is not open in this manager")
            if not isinstance(state, _Pending):
                raise RuntimeError("attempt can begin only from Pending")

            context = _AttemptContext[AccessT, SpecT, ResourceT](attempt_id, access)
            acquiring = _Acquiring(context)
            self._attempts[attempt_id] = acquiring

        try:
            requirements = self._requirements_model.requirements(access)

            with self._lock:
                current = self._attempts.get(attempt_id)
                if current is not acquiring:
                    raise RuntimeError("resource attempt registry changed unexpectedly")
                context.requirements = requirements
                if context.cancel_requested:
                    self._attempts.pop(attempt_id, None)
                    return ResourceFailed(
                        ResourceAcquisitionCancelled("attempt was cancelled")
                    )

                reserved = self._resource_pool.reserve(
                    attempt_id,
                    access,
                    requirements,
                )
                if not reserved:
                    self._attempts.pop(attempt_id, None)
                    return ResourceBlocked()
                context.reserved = True

            failure = self._acquire_requirements(context)
            if failure is not None:
                self._fail_attempt(context, failure)
                return ResourceFailed(failure)

            return self._commit_acquired(context)

        except BaseException:
            # Operational physical failures are typed PhysicalFailed outcomes. Any
            # exception escaping acquire() is therefore a contract/invariant failure,
            # but ResourceManager still retires every Resource it knows it owns.
            self._fail_attempt(context, None)
            raise

    def release(self, attempt_id: AttemptId) -> None:
        """Revoke authority and request interruption without waiting for completion.

        If a physical ``acquire`` is in progress, ``interrupt`` is requested and this
        method returns. The acquisition thread itself remains blocked until the driver
        returns its terminal typed outcome. Cleanup is likewise detached from this
        call so Managed release never waits for backend shutdown or cleanup latency.
        """

        self._validate_attempt_id(attempt_id)

        retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None
        physical: PhysicalAcquisition[ResourceT] | None = None

        with self._lock:
            state = self._attempts.get(attempt_id)
            if state is None:
                return
            if isinstance(state, _Pending):
                self._attempts[attempt_id] = _Cancelled(attempt_id)
                return
            if isinstance(state, _Cancelled):
                return

            if isinstance(state, _Acquiring):
                context = state.context
                context.cancel_requested = True
                physical = context.current_physical
                if context.reserved:
                    self._resource_pool.release(attempt_id)
            elif isinstance(state, _Pinned):
                release = self._resource_pool.release(attempt_id)
                retired = release.retired
                if retired is None:
                    raise RuntimeError("pinned resource attempt was not retained")
                self._attempts[attempt_id] = _RetiredPinned(state.context, retired)
            elif isinstance(state, _Retained):
                release = self._resource_pool.release(attempt_id)
                retired = release.retired
                if retired is None:
                    raise RuntimeError("retained resource attempt was not retained")
                self._attempts[attempt_id] = _Retired(state.context, retired)
            elif isinstance(state, _RetiredPinned):
                return
            elif isinstance(state, _Retired):
                retired = state.retired
            else:
                raise RuntimeError("unsupported Resource attempt state")

        if physical is not None:
            try:
                physical.interrupt()
            except Exception:
                # Authority retirement cannot depend on a backend synchronously
                # accepting interruption. acquire() must still reach a terminal
                # outcome, possibly by completing naturally.
                pass

        if retired is not None:
            self._schedule_cleanup_retired_attempt(retired)

    def finish_acquire(self, attempt_id: AttemptId) -> None:
        """End the temporary ResourceSet borrow used for Capability projection."""

        self._validate_attempt_id(attempt_id)
        retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None
        with self._lock:
            state = self._attempts.get(attempt_id)
            if state is None:
                return
            if isinstance(state, _Cancelled):
                self._attempts.pop(attempt_id, None)
                return
            if isinstance(state, _Pinned):
                self._attempts[attempt_id] = _Retained(state.context)
                return
            if isinstance(state, _RetiredPinned):
                retired = state.retired
                self._attempts[attempt_id] = _Retired(state.context, retired)
            elif isinstance(state, _Acquiring):
                if state.context.cancel_requested and not state.context.reserved:
                    self._attempts.pop(attempt_id, None)

        if retired is not None:
            self._schedule_cleanup_retired_attempt(retired)

    def cleanup_retired(self, access: AccessT) -> bool:
        """Synchronously retry cleanup of retired entries for operational recovery.

        Normal Managed flows never need to call this; retirement schedules cleanup
        automatically. This method remains available for retrying a cleanup that a
        driver previously failed.
        """

        if access is None:
            raise TypeError("access cannot be None")

        with self._lock:
            attempt_ids = tuple(
                state.retired.attempt
                for state in self._attempts.values()
                if isinstance(state, (_RetiredPinned, _Retired))
                and state.retired.access == access
            )

        retired_records = tuple(
            retired
            for attempt_id in attempt_ids
            if (retired := self._resource_pool.retired(attempt_id)) is not None
        )
        if not retired_records:
            return False

        for retired in retired_records:
            error = self._cleanup_retired_attempt(retired)
            if error is not None:
                raise error
        return True

    def _acquire_requirements(
        self,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
    ) -> Exception | None:
        requirements = context.requirements
        if requirements is None:
            raise RuntimeError("resource requirements are not resolved")

        for requirement in requirements:
            if self._is_cancel_requested(context):
                return ResourceAcquisitionCancelled("attempt was cancelled")

            try:
                physical = self._driver.prepare(requirement.spec)
            except Exception as exc:
                return exc
            if physical is None:
                raise TypeError("ResourceDriver.prepare() cannot return None")

            with self._lock:
                state = self._attempts.get(context.attempt_id)
                if not isinstance(state, _Acquiring) or state.context is not context:
                    raise RuntimeError("resource attempt registry changed unexpectedly")
                cancelled = context.cancel_requested
                if not cancelled:
                    context.current_physical = physical

            if cancelled:
                try:
                    physical.interrupt()
                except Exception:
                    pass
                return ResourceAcquisitionCancelled("attempt was cancelled")

            try:
                outcome = physical.acquire()
            finally:
                with self._lock:
                    if context.current_physical is physical:
                        context.current_physical = None

            resources = self._validate_physical_outcome(outcome)
            with self._lock:
                state = self._attempts.get(context.attempt_id)
                if not isinstance(state, _Acquiring) or state.context is not context:
                    raise RuntimeError("resource attempt registry changed unexpectedly")
                context.resources += resources
                cancelled = context.cancel_requested

            if isinstance(outcome, PhysicalFailed):
                if not isinstance(outcome.error, Exception):
                    raise TypeError("PhysicalFailed.error must be an Exception")
                return outcome.error
            if isinstance(outcome, PhysicalInterrupted):
                return ResourceAcquisitionCancelled("physical acquisition was interrupted")
            if not isinstance(outcome, PhysicalAcquired):
                raise RuntimeError("unsupported PhysicalAcquireOutcome")
            if cancelled:
                return ResourceAcquisitionCancelled("attempt was cancelled")

        return None

    def _commit_acquired(
        self,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
    ) -> ResourceAcquireResult[ResourceT]:
        retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None
        with self._lock:
            state = self._attempts.get(context.attempt_id)
            if not isinstance(state, _Acquiring) or state.context is not context:
                raise RuntimeError("resource attempt registry changed unexpectedly")
            if not context.reserved:
                raise RuntimeError("resource attempt was not reserved")

            if context.cancel_requested:
                self._resource_pool.release(context.attempt_id)

            retired = self._resource_pool.finish(
                context.attempt_id,
                context.resources,
            )
            if retired is None and not context.cancel_requested:
                self._attempts[context.attempt_id] = _Pinned(context)
                return ResourceAcquired(context.resources)

            if retired is None:
                raise RuntimeError("cancelled resource attempt was not retired")
            self._attempts[context.attempt_id] = _Retired(context, retired)

        self._schedule_cleanup_retired_attempt(retired)
        return ResourceFailed(ResourceAcquisitionCancelled("attempt was cancelled"))

    def _fail_attempt(
        self,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
        primary_error: Exception | None,
    ) -> None:
        """Retire all terminally reported Resources and schedule detached cleanup."""

        retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None
        physical: PhysicalAcquisition[ResourceT] | None = None
        with self._lock:
            current = self._attempts.get(context.attempt_id)
            if not self._state_uses_context(current, context):
                return

            context.cancel_requested = True
            physical = context.current_physical

            if context.reserved:
                release = self._resource_pool.release(context.attempt_id)
                if release.processing:
                    retired = self._resource_pool.finish(
                        context.attempt_id,
                        context.resources,
                    )
                else:
                    retired = release.retired

                if retired is not None:
                    self._attempts[context.attempt_id] = _Retired(context, retired)
                else:
                    self._attempts.pop(context.attempt_id, None)
            else:
                self._attempts.pop(context.attempt_id, None)

        if physical is not None:
            try:
                physical.interrupt()
            except Exception as exc:
                if primary_error is not None:
                    primary_error.add_note(f"resource interrupt also failed: {exc!r}")

        if retired is not None:
            self._schedule_cleanup_retired_attempt(retired)

    def _schedule_cleanup_retired_attempt(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> None:
        """Run cleanup in a detached daemon thread if the attempt is eligible."""

        if not self._claim_cleanup(retired):
            return

        worker = Thread(
            target=self._cleanup_claimed_retired_attempt,
            args=(retired,),
            name=f"resource-cleanup-{id(retired.attempt):x}",
            daemon=True,
        )
        try:
            worker.start()
        except Exception:
            # Thread creation failure must not make Managed release fail. Keep the
            # retired entry available for a later release/cleanup_retired retry.
            self._reset_cleanup_claim(retired)

    def _cleanup_retired_attempt(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> Exception | None:
        """Synchronously cleanup one eligible retired entry."""

        if not self._claim_cleanup(retired):
            return None
        return self._cleanup_claimed_retired_attempt(retired)

    def _claim_cleanup(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> bool:
        with self._lock:
            state = self._attempts.get(retired.attempt)
            if isinstance(state, _RetiredPinned):
                return False
            if not isinstance(state, _Retired):
                return False
            if state.cleanup_in_progress:
                return False
            state.cleanup_in_progress = True
            return True

    def _reset_cleanup_claim(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> None:
        with self._lock:
            current = self._attempts.get(retired.attempt)
            if isinstance(current, _Retired):
                current.cleanup_in_progress = False

    def _cleanup_claimed_retired_attempt(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> Exception | None:
        """Cleanup an entry whose ``cleanup_in_progress`` claim is already held."""

        try:
            self._driver.cleanup(retired.resources)
            self._resource_pool.discard(retired)
        except Exception as exc:
            self._reset_cleanup_claim(retired)
            return exc

        with self._lock:
            current = self._attempts.get(retired.attempt)
            if isinstance(current, _Retired):
                self._attempts.pop(retired.attempt, None)
        return None

    def _is_cancel_requested(
        self,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
    ) -> bool:
        with self._lock:
            current = self._attempts.get(context.attempt_id)
            if not isinstance(current, _Acquiring) or current.context is not context:
                return True
            return context.cancel_requested

    @staticmethod
    def _state_uses_context(
        state: _AttemptState[AccessT, SpecT, ResourceT] | None,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
    ) -> bool:
        return isinstance(
            state,
            (_Acquiring, _Pinned, _Retained, _RetiredPinned, _Retired),
        ) and state.context is context

    @staticmethod
    def _validate_attempt_id(attempt_id: AttemptId) -> None:
        if not isinstance(attempt_id, AttemptId):
            raise TypeError("attempt_id must be AttemptId")

    @staticmethod
    def _validate_physical_outcome(
        outcome: object,
    ) -> ResourceSet[ResourceT]:
        if not isinstance(
            outcome,
            (PhysicalAcquired, PhysicalInterrupted, PhysicalFailed),
        ):
            raise TypeError(
                "PhysicalAcquisition.acquire() must return a PhysicalAcquireOutcome"
            )
        if not isinstance(outcome.resources, tuple):
            raise TypeError("physical outcome resources must be a ResourceSet tuple")
        return outcome.resources


__all__ = [
    "ResourceAcquisitionCancelled",
    "ResourceManagement",
    "ResourceManager",
    "ResourceRequirementsModel",
]
