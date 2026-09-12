from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Generic, Protocol, TypeVar

from _attempt import AttemptId
from _resource.cleanup import CleanupScheduler, DaemonThreadCleanupScheduler
from _resource.driver import (
    PhysicalAcquired,
    PhysicalAcquisition,
    PhysicalFailed,
    PhysicalInterrupted,
    ResourceDriver,
    PhysicalResourceSet,
)
from _resource.key import ResourceKey, ResourceKeyModel, ResourceKeys
from _resource.pool import (
    GLOBAL_RESOURCE_POOL,
    ResourceReservationTable,
)
from _resource.policy import ResourcePolicy
from _resource.requirement import ResourceRequirement, ResourceRequirements
from _resource.result import (
    ResourceAcquireResult,
    ResourceAcquired,
    ResourceBlocked,
    ResourceFailed,
)


RequestT = TypeVar("RequestT")
RequirementT = TypeVar("RequirementT", bound=ResourceRequirement)
PhysicalResourceT = TypeVar("PhysicalResourceT")


class ResourceRequirementsModel(Protocol[RequestT, RequirementT]):
    """Resolve only the physical-resource requirements for one Request."""

    def requirements(self, request: RequestT) -> ResourceRequirements[RequirementT]: ...


class ResourceManagement(Protocol[RequestT, PhysicalResourceT]):
    """Managed-facing boundary for the complete physical-resource lifecycle.

    ``open_attempt`` registers an opaque identity before Managed publishes it, so a
    concurrent ``release`` cannot be lost. ``release`` revokes Managed retention
    authority and requests interruption of any current physical acquisition without
    waiting for that acquisition or cleanup to complete. Logical reservations remain
    held until cleanup succeeds. Acquired PhysicalResources remain pinned until
    ``finish_acquire`` while Capability projection is using them.
    """

    def open_attempt(self) -> AttemptId: ...

    def acquire(
        self,
        attempt_id: AttemptId,
        request: RequestT,
    ) -> ResourceAcquireResult[PhysicalResourceT]: ...

    def release(self, attempt_id: AttemptId) -> None: ...

    def finish_acquire(self, attempt_id: AttemptId) -> None: ...


class ResourceAcquisitionCancelled(RuntimeError):
    """Internal operational failure used when an acquisition is cancelled."""


@dataclass(slots=True)
class _AttemptContext(Generic[RequestT, RequirementT, PhysicalResourceT]):
    attempt_id: AttemptId
    request: RequestT
    requirements: ResourceRequirements[RequirementT] | None = None
    resources: PhysicalResourceSet[PhysicalResourceT] = ()
    current_physical: PhysicalAcquisition[PhysicalResourceT] | None = None
    reserved: bool = False
    cancel_requested: bool = False


@dataclass(frozen=True, slots=True)
class _Pending:
    attempt_id: AttemptId


@dataclass(frozen=True, slots=True)
class _Cancelled:
    attempt_id: AttemptId


@dataclass(slots=True)
class _Acquiring(Generic[RequestT, RequirementT, PhysicalResourceT]):
    context: _AttemptContext[RequestT, RequirementT, PhysicalResourceT]


@dataclass(slots=True)
class _Pinned(Generic[RequestT, RequirementT, PhysicalResourceT]):
    context: _AttemptContext[RequestT, RequirementT, PhysicalResourceT]
    projection_pending: bool = True


@dataclass(slots=True)
class _Retired(Generic[RequestT, RequirementT, PhysicalResourceT]):
    context: _AttemptContext[RequestT, RequirementT, PhysicalResourceT]
    projection_pending: bool = False
    cleanup_in_progress: bool = False


type _AttemptState[A, S, R] = (
    _Pending
    | _Cancelled
    | _Acquiring[A, S, R]
    | _Pinned[A, S, R]
    | _Retired[A, S, R]
)


class ResourceManager(Generic[RequestT, RequirementT, PhysicalResourceT]):
    """Own physical acquisition, retirement, and cleanup around shared reservations."""

    def __init__(
        self,
        requirements_model: ResourceRequirementsModel[RequestT, RequirementT],
        key_model: ResourceKeyModel[RequirementT],
        driver: ResourceDriver[RequirementT, PhysicalResourceT],
        *,
        resource_pool: ResourceReservationTable[
            RequestT, RequirementT
        ] = GLOBAL_RESOURCE_POOL,
        cleanup_scheduler: CleanupScheduler | None = None,
    ) -> None:
        self._requirements_model = requirements_model
        if not callable(getattr(key_model, "key_for", None)):
            raise TypeError("key_model must provide key_for(requirement)")
        self._key_model = key_model
        self._driver = driver
        if not isinstance(resource_pool, ResourceReservationTable):
            raise TypeError("resource_pool must be ResourceReservationTable")
        self._reservation_table = resource_pool
        if cleanup_scheduler is None:
            cleanup_scheduler = DaemonThreadCleanupScheduler()
        if not callable(getattr(cleanup_scheduler, "schedule", None)):
            raise TypeError("cleanup_scheduler must provide schedule(job)")
        self._cleanup_scheduler = cleanup_scheduler
        self._lock = Lock()
        self._attempts: dict[
            AttemptId, _AttemptState[RequestT, RequirementT, PhysicalResourceT]
        ] = {}

    @property
    def requirements_model(self) -> ResourceRequirementsModel[RequestT, RequirementT]:
        return self._requirements_model

    @property
    def key_model(self) -> ResourceKeyModel[RequirementT]:
        return self._key_model

    @property
    def driver(self) -> ResourceDriver[RequirementT, PhysicalResourceT]:
        return self._driver

    @property
    def reservation_table(
        self,
    ) -> ResourceReservationTable[RequestT, RequirementT]:
        return self._reservation_table

    @property
    def resource_pool(self) -> ResourceReservationTable[RequestT, RequirementT]:
        """Compatibility alias for ``reservation_table``."""

        return self._reservation_table

    @property
    def cleanup_scheduler(self) -> CleanupScheduler:
        return self._cleanup_scheduler

    def open_attempt(self) -> AttemptId:
        """Create and register one Pending acquisition identity."""

        attempt_id = AttemptId()
        with self._lock:
            self._attempts[attempt_id] = _Pending(attempt_id)
        return attempt_id

    def acquire(
        self,
        attempt_id: AttemptId,
        request: RequestT,
    ) -> ResourceAcquireResult[PhysicalResourceT]:
        self._validate_attempt_id(attempt_id)
        if request is None:
            raise TypeError("request cannot be None")

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

            context = _AttemptContext[RequestT, RequirementT, PhysicalResourceT](
                attempt_id, request
            )
            acquiring = _Acquiring(context)
            self._attempts[attempt_id] = acquiring

        try:
            requirements = self._requirements_model.requirements(request)
            keys = self._resolve_keys(requirements)

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

                reserved = self._reservation_table.reserve(
                    attempt_id,
                    request,
                    requirements,
                    keys,
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
        method returns. Logical reservations remain held until detached physical cleanup
        succeeds, preventing a conflicting attempt from overlapping acquisition or cleanup.
        """

        self._validate_attempt_id(attempt_id)

        retired_attempt: AttemptId | None = None
        physical: PhysicalAcquisition[PhysicalResourceT] | None = None

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
            elif isinstance(state, _Pinned):
                self._attempts[attempt_id] = _Retired(
                    state.context,
                    projection_pending=state.projection_pending,
                )
                retired_attempt = attempt_id
            elif isinstance(state, _Retired):
                retired_attempt = attempt_id
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

        if retired_attempt is not None:
            self._schedule_cleanup_retired_attempt(retired_attempt)

    def finish_acquire(self, attempt_id: AttemptId) -> None:
        """End the temporary PhysicalResourceSet borrow used for Capability projection."""

        self._validate_attempt_id(attempt_id)
        retired_attempt: AttemptId | None = None
        with self._lock:
            state = self._attempts.get(attempt_id)
            if state is None:
                return
            if isinstance(state, _Cancelled):
                self._attempts.pop(attempt_id, None)
                return
            if isinstance(state, _Pinned):
                state.projection_pending = False
                return
            if isinstance(state, _Retired):
                if state.projection_pending:
                    state.projection_pending = False
                    retired_attempt = attempt_id
            elif isinstance(state, _Acquiring):
                if state.context.cancel_requested and not state.context.reserved:
                    self._attempts.pop(attempt_id, None)

        if retired_attempt is not None:
            self._schedule_cleanup_retired_attempt(retired_attempt)

    def cleanup_retired(self, request: RequestT) -> bool:
        """Synchronously retry cleanup of retired entries for operational recovery.

        Normal Managed flows never need to call this; retirement schedules cleanup
        automatically. This method remains available for retrying a cleanup that a
        driver previously failed.
        """

        if request is None:
            raise TypeError("request cannot be None")

        with self._lock:
            attempt_ids = tuple(
                attempt_id
                for attempt_id, state in self._attempts.items()
                if isinstance(state, _Retired)
                and state.context.request == request
            )

        if not attempt_ids:
            return False

        for attempt_id in attempt_ids:
            error = self._cleanup_retired_attempt(attempt_id)
            if error is not None:
                raise error
        return True

    def _resolve_keys(
        self,
        requirements: ResourceRequirements[RequirementT],
    ) -> ResourceKeys:
        if not isinstance(requirements, tuple):
            raise TypeError("ResourceRequirementsModel.requirements() must return a tuple")

        keys: list[ResourceKey] = []
        for requirement in requirements:
            if not isinstance(requirement, ResourceRequirement):
                raise TypeError(
                    "requirements must contain ResourceRequirement values"
                )
            if not isinstance(requirement.policy, ResourcePolicy):
                raise TypeError("resource policy must be ResourcePolicy")
            key = self._key_model.key_for(requirement)
            if not isinstance(key, ResourceKey):
                raise TypeError("ResourceKeyModel.key_for() must return ResourceKey")
            keys.append(key)
        return tuple(keys)

    def _acquire_requirements(
        self,
        context: _AttemptContext[RequestT, RequirementT, PhysicalResourceT],
    ) -> Exception | None:
        requirements = context.requirements
        if requirements is None:
            raise RuntimeError("resource requirements are not resolved")

        for requirement in requirements:
            if self._is_cancel_requested(context):
                return ResourceAcquisitionCancelled("attempt was cancelled")

            try:
                physical = self._driver.prepare(requirement)
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
        context: _AttemptContext[RequestT, RequirementT, PhysicalResourceT],
    ) -> ResourceAcquireResult[PhysicalResourceT]:
        retired_attempt: AttemptId | None = None
        with self._lock:
            state = self._attempts.get(context.attempt_id)
            if not isinstance(state, _Acquiring) or state.context is not context:
                raise RuntimeError("resource attempt registry changed unexpectedly")
            if not context.reserved:
                raise RuntimeError("resource attempt was not reserved")

            if not context.cancel_requested:
                self._attempts[context.attempt_id] = _Pinned(context)
                return ResourceAcquired(context.resources)

            self._attempts[context.attempt_id] = _Retired(context)
            retired_attempt = context.attempt_id

        self._schedule_cleanup_retired_attempt(retired_attempt)
        return ResourceFailed(ResourceAcquisitionCancelled("attempt was cancelled"))

    def _fail_attempt(
        self,
        context: _AttemptContext[RequestT, RequirementT, PhysicalResourceT],
        primary_error: Exception | None,
    ) -> None:
        """Retire known PhysicalResources and schedule detached cleanup."""

        retired_attempt: AttemptId | None = None
        physical: PhysicalAcquisition[PhysicalResourceT] | None = None
        with self._lock:
            current = self._attempts.get(context.attempt_id)
            if not self._state_uses_context(current, context):
                return

            context.cancel_requested = True
            physical = context.current_physical

            if context.reserved:
                self._attempts[context.attempt_id] = _Retired(context)
                retired_attempt = context.attempt_id
            else:
                self._attempts.pop(context.attempt_id, None)

        if physical is not None:
            try:
                physical.interrupt()
            except Exception as exc:
                if primary_error is not None:
                    primary_error.add_note(f"resource interrupt also failed: {exc!r}")

        if retired_attempt is not None:
            self._schedule_cleanup_retired_attempt(retired_attempt)

    def _schedule_cleanup_retired_attempt(self, attempt_id: AttemptId) -> None:
        """Submit detached cleanup if the retired attempt is eligible."""

        if not self._claim_cleanup(attempt_id):
            return

        def cleanup_job() -> None:
            self._cleanup_claimed_retired_attempt(attempt_id)

        try:
            scheduled = self._cleanup_scheduler.schedule(cleanup_job)
        except Exception:
            scheduled = False

        if not scheduled:
            # Submission failure must not make Managed release fail. Keep the retired
            # attempt and its reservation available for a later release/retry.
            self._reset_cleanup_claim(attempt_id)

    def _cleanup_retired_attempt(self, attempt_id: AttemptId) -> Exception | None:
        """Synchronously cleanup one eligible retired entry."""

        if not self._claim_cleanup(attempt_id):
            return None
        return self._cleanup_claimed_retired_attempt(attempt_id)

    def _claim_cleanup(self, attempt_id: AttemptId) -> bool:
        with self._lock:
            state = self._attempts.get(attempt_id)
            if not isinstance(state, _Retired):
                return False
            if state.projection_pending:
                return False
            if state.cleanup_in_progress:
                return False
            state.cleanup_in_progress = True
            return True

    def _reset_cleanup_claim(self, attempt_id: AttemptId) -> None:
        with self._lock:
            current = self._attempts.get(attempt_id)
            if isinstance(current, _Retired):
                current.cleanup_in_progress = False

    def _cleanup_claimed_retired_attempt(
        self,
        attempt_id: AttemptId,
    ) -> Exception | None:
        """Cleanup an entry whose ``cleanup_in_progress`` claim is already held."""

        with self._lock:
            state = self._attempts.get(attempt_id)
            if not isinstance(state, _Retired) or not state.cleanup_in_progress:
                raise RuntimeError("retired resource cleanup claim was lost")
            context = state.context

        try:
            self._driver.cleanup(context.resources)
            self._reservation_table.release_reservation(attempt_id)
        except Exception as exc:
            self._reset_cleanup_claim(attempt_id)
            return exc

        with self._lock:
            current = self._attempts.get(attempt_id)
            if current is state:
                context.reserved = False
                self._attempts.pop(attempt_id, None)
        return None

    def _is_cancel_requested(
        self,
        context: _AttemptContext[RequestT, RequirementT, PhysicalResourceT],
    ) -> bool:
        with self._lock:
            current = self._attempts.get(context.attempt_id)
            if not isinstance(current, _Acquiring) or current.context is not context:
                return True
            return context.cancel_requested

    @staticmethod
    def _state_uses_context(
        state: _AttemptState[RequestT, RequirementT, PhysicalResourceT] | None,
        context: _AttemptContext[RequestT, RequirementT, PhysicalResourceT],
    ) -> bool:
        return isinstance(
            state,
            (_Acquiring, _Pinned, _Retired),
        ) and state.context is context

    @staticmethod
    def _validate_attempt_id(attempt_id: AttemptId) -> None:
        if not isinstance(attempt_id, AttemptId):
            raise TypeError("attempt_id must be AttemptId")

    @staticmethod
    def _validate_physical_outcome(
        outcome: object,
    ) -> PhysicalResourceSet[PhysicalResourceT]:
        if not isinstance(
            outcome,
            (PhysicalAcquired, PhysicalInterrupted, PhysicalFailed),
        ):
            raise TypeError(
                "PhysicalAcquisition.acquire() must return a PhysicalAcquireOutcome"
            )
        if not isinstance(outcome.resources, tuple):
            raise TypeError("physical outcome resources must be a PhysicalResourceSet tuple")
        return outcome.resources


__all__ = [
    "ResourceAcquisitionCancelled",
    "ResourceManagement",
    "ResourceManager",
    "ResourceRequirementsModel",
]
