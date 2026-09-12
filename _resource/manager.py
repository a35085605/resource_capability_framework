from __future__ import annotations

from collections.abc import Callable
from enum import Enum, auto
from threading import Lock
from typing import Generic, Protocol, TypeVar

from _attempt import AttemptId
from _resource.claim import ResourceClaim, ResourceClaims
from _resource.cleanup import CleanupScheduler, DaemonThreadCleanupScheduler
from _resource.driver import (
    PhysicalAcquired,
    PhysicalAcquisition,
    PhysicalFailed,
    PhysicalInterrupted,
    PhysicalResourceSet,
    ResourceDriver,
)
from _resource.key import ResourceKey, ResourceKeyModel
from _resource.policy import ResourcePolicy
from _resource.pool import GLOBAL_RESOURCE_POOL, ResourceReservationTable
from _resource.requirement import ResourceRequirement, ResourceRequirements
from _resource.result import (
    ResourceAcquireResult,
    ResourceBlocked,
    ResourceFailed,
    ResourceReady,
)


RequestT = TypeVar("RequestT")
RequirementT = TypeVar("RequirementT", bound=ResourceRequirement)
PhysicalResourceT = TypeVar("PhysicalResourceT")
ValueT = TypeVar("ValueT")


class ResourceRequirementsModel(Protocol[RequestT, RequirementT]):
    """Resolve only the physical-resource requirements for one Request."""

    def requirements(self, request: RequestT) -> ResourceRequirements[RequirementT]: ...


class ResourceAttempt(Protocol[RequestT, PhysicalResourceT]):
    """Managed-facing handle for one Resource acquisition lifecycle.

    The handle owns the attempt lifecycle. ``release`` revokes retention authority and
    may synchronously request interruption of the current physical operation, but it
    never waits for acquisition to reach a terminal outcome or for cleanup to finish.
    """

    @property
    def id(self) -> AttemptId: ...

    def acquire(
        self,
        request: RequestT,
        use_resources: Callable[[PhysicalResourceSet[PhysicalResourceT]], ValueT],
    ) -> ResourceAcquireResult[ValueT]: ...

    def release(self) -> None: ...


class ResourceManagement(Protocol[RequestT, PhysicalResourceT]):
    """Managed-facing boundary for the complete physical-resource lifecycle.

    ``open_attempt`` registers and returns an opaque lifecycle handle before Managed
    publishes it, so a concurrent ``release`` cannot be lost. The manager retains a
    strong reference to each live handle so retired resources remain recoverable after
    Managed has detached from them.
    """

    def open_attempt(self) -> ResourceAttempt[RequestT, PhysicalResourceT]: ...


class ResourceAcquisitionCancelled(RuntimeError):
    """Internal operational failure used when an acquisition is cancelled."""


class _AttemptPhase(Enum):
    OPEN = auto()
    ACQUIRING = auto()
    ACTIVE = auto()
    RETIRED = auto()
    DONE = auto()


class _ResourceAttempt(Generic[RequestT, RequirementT, PhysicalResourceT]):
    """Own the lifecycle, resources, and cleanup state for one acquisition attempt."""

    def __init__(
        self,
        attempt_id: AttemptId,
        requirements_model: ResourceRequirementsModel[RequestT, RequirementT],
        key_model: ResourceKeyModel[RequirementT],
        driver: ResourceDriver[RequirementT, PhysicalResourceT],
        reservation_table: ResourceReservationTable[RequestT, RequirementT],
        cleanup_scheduler: CleanupScheduler,
        on_done: Callable[["_ResourceAttempt[RequestT, RequirementT, PhysicalResourceT]"], None],
    ) -> None:
        self._id = attempt_id
        self._requirements_model = requirements_model
        self._key_model = key_model
        self._driver = driver
        self._reservation_table = reservation_table
        self._cleanup_scheduler = cleanup_scheduler
        self._on_done = on_done

        self._lock = Lock()
        self._phase = _AttemptPhase.OPEN
        self._cancel_requested = False
        self._request: RequestT | None = None
        self._claims: ResourceClaims[RequirementT] | None = None
        self._resources: PhysicalResourceSet[PhysicalResourceT] = ()
        self._current_physical: PhysicalAcquisition[PhysicalResourceT] | None = None
        self._reserved = False
        self._cleanup_in_progress = False

    @property
    def id(self) -> AttemptId:
        return self._id

    def acquire(
        self,
        request: RequestT,
        use_resources: Callable[[PhysicalResourceSet[PhysicalResourceT]], ValueT],
    ) -> ResourceAcquireResult[ValueT]:
        if request is None:
            raise TypeError("request cannot be None")
        if not callable(use_resources):
            raise TypeError("use_resources must be callable")

        cancelled_before_start = False
        with self._lock:
            if self._phase is not _AttemptPhase.OPEN:
                raise RuntimeError("attempt can begin only from OPEN")
            self._request = request
            if self._cancel_requested:
                self._phase = _AttemptPhase.DONE
                cancelled_before_start = True
            else:
                self._phase = _AttemptPhase.ACQUIRING

        if cancelled_before_start:
            self._notify_done()
            return ResourceFailed(ResourceAcquisitionCancelled("attempt was cancelled"))

        try:
            requirements = self._requirements_model.requirements(request)
            claims = self._resolve_claims(requirements)

            with self._lock:
                self._require_phase_locked(_AttemptPhase.ACQUIRING)
                self._claims = claims
                cancelled = self._cancel_requested

            if cancelled:
                self._finish_unreserved()
                return ResourceFailed(ResourceAcquisitionCancelled("attempt was cancelled"))

            reserved = self._reservation_table.reserve(
                self._id,
                request,
                claims,
            )
            if not reserved:
                self._finish_unreserved()
                return ResourceBlocked()

            with self._lock:
                self._require_phase_locked(_AttemptPhase.ACQUIRING)
                self._reserved = True
                cancelled = self._cancel_requested

            if cancelled:
                self._retire_and_schedule_cleanup()
                return ResourceFailed(ResourceAcquisitionCancelled("attempt was cancelled"))

            failure = self._acquire_requirements()
            if failure is not None:
                self._fail_attempt(failure)
                return ResourceFailed(failure)

            with self._lock:
                self._require_phase_locked(_AttemptPhase.ACQUIRING)
                resources = self._resources

            # Projection is part of ACQUIRING. A concurrent release can request
            # cancellation here, but cleanup cannot begin until this callback returns.
            value = use_resources(resources)
            return self._commit_ready(value)

        except BaseException:
            # Operational physical failures are typed PhysicalFailed outcomes. Any
            # exception escaping acquire(), including one raised by use_resources, keeps
            # its original semantics while the attempt still retires owned Resources.
            self._fail_attempt(None)
            raise

    def release(self) -> None:
        """Revoke authority without waiting for terminal acquisition or cleanup.

        ``interrupt`` is invoked synchronously when a physical operation is currently
        exposed by the driver. Cleanup, when eligible, is submitted to the configured
        scheduler and never runs while acquisition/projection is still in ACQUIRING.
        """

        physical: PhysicalAcquisition[PhysicalResourceT] | None = None
        schedule_cleanup = False

        with self._lock:
            phase = self._phase
            if phase is _AttemptPhase.DONE:
                return
            if phase is _AttemptPhase.OPEN:
                self._cancel_requested = True
                return
            if phase is _AttemptPhase.ACQUIRING:
                self._cancel_requested = True
                physical = self._current_physical
            elif phase is _AttemptPhase.ACTIVE:
                self._cancel_requested = True
                self._phase = _AttemptPhase.RETIRED
                schedule_cleanup = True
            elif phase is _AttemptPhase.RETIRED:
                schedule_cleanup = True
            else:
                raise RuntimeError("unsupported Resource attempt phase")

        if physical is not None:
            try:
                physical.interrupt()
            except Exception:
                # Revoking authority cannot depend on the backend synchronously
                # accepting interruption. acquire() must still reach a terminal outcome.
                pass

        if schedule_cleanup:
            self._schedule_cleanup()

    def matches_retired_request(self, request: RequestT) -> bool:
        """Return whether this attempt is a retired retry candidate for ``request``."""

        with self._lock:
            if self._phase is not _AttemptPhase.RETIRED:
                return False
            current_request = self._request
        return current_request == request

    def retry_cleanup(self) -> Exception | None:
        """Synchronously retry cleanup when this attempt is retired and unclaimed."""

        if not self._claim_cleanup():
            return None
        return self._cleanup_claimed()

    def _resolve_claims(
        self,
        requirements: ResourceRequirements[RequirementT],
    ) -> ResourceClaims[RequirementT]:
        if not isinstance(requirements, tuple):
            raise TypeError("ResourceRequirementsModel.requirements() must return a tuple")

        claims: list[ResourceClaim[RequirementT]] = []
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
            claims.append(ResourceClaim(requirement, key))
        return tuple(claims)

    def _acquire_requirements(self) -> Exception | None:
        with self._lock:
            self._require_phase_locked(_AttemptPhase.ACQUIRING)
            claims = self._claims
        if claims is None:
            raise RuntimeError("resource claims are not resolved")

        for claim in claims:
            requirement = claim.requirement
            if self._is_cancel_requested():
                return ResourceAcquisitionCancelled("attempt was cancelled")

            try:
                physical = self._driver.prepare(requirement)
            except Exception as exc:
                return exc
            if physical is None:
                raise TypeError("ResourceDriver.prepare() cannot return None")

            with self._lock:
                self._require_phase_locked(_AttemptPhase.ACQUIRING)
                cancelled = self._cancel_requested
                if not cancelled:
                    self._current_physical = physical

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
                    if self._current_physical is physical:
                        self._current_physical = None

            resources = self._validate_physical_outcome(outcome)
            with self._lock:
                self._require_phase_locked(_AttemptPhase.ACQUIRING)
                self._resources += resources
                cancelled = self._cancel_requested

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

    def _commit_ready(self, value: ValueT) -> ResourceAcquireResult[ValueT]:
        schedule_cleanup = False
        with self._lock:
            self._require_phase_locked(_AttemptPhase.ACQUIRING)
            if not self._reserved:
                raise RuntimeError("resource attempt was not reserved")

            if not self._cancel_requested:
                self._phase = _AttemptPhase.ACTIVE
                return ResourceReady(value)

            self._phase = _AttemptPhase.RETIRED
            schedule_cleanup = True

        if schedule_cleanup:
            self._schedule_cleanup()
        return ResourceFailed(ResourceAcquisitionCancelled("attempt was cancelled"))

    def _fail_attempt(self, primary_error: Exception | None) -> None:
        """Retire known PhysicalResources or finish an unreserved failed attempt."""

        physical: PhysicalAcquisition[PhysicalResourceT] | None = None
        schedule_cleanup = False
        notify_done = False

        with self._lock:
            phase = self._phase
            if phase is _AttemptPhase.DONE:
                return

            self._cancel_requested = True
            physical = self._current_physical

            if self._reserved:
                if phase is not _AttemptPhase.RETIRED:
                    self._phase = _AttemptPhase.RETIRED
                schedule_cleanup = True
            else:
                self._phase = _AttemptPhase.DONE
                notify_done = True

        if physical is not None:
            try:
                physical.interrupt()
            except Exception as exc:
                if primary_error is not None:
                    primary_error.add_note(f"resource interrupt also failed: {exc!r}")

        if schedule_cleanup:
            self._schedule_cleanup()
        if notify_done:
            self._notify_done()

    def _finish_unreserved(self) -> None:
        notify_done = False
        with self._lock:
            self._require_phase_locked(_AttemptPhase.ACQUIRING)
            if self._reserved:
                raise RuntimeError("reserved attempt cannot finish without cleanup")
            self._phase = _AttemptPhase.DONE
            notify_done = True

        if notify_done:
            self._notify_done()

    def _retire_and_schedule_cleanup(self) -> None:
        with self._lock:
            self._require_phase_locked(_AttemptPhase.ACQUIRING)
            if not self._reserved:
                raise RuntimeError("unreserved attempt cannot be retired")
            self._phase = _AttemptPhase.RETIRED

        self._schedule_cleanup()

    def _schedule_cleanup(self) -> None:
        """Submit detached cleanup if this retired attempt is eligible."""

        if not self._claim_cleanup():
            return

        def cleanup_job() -> None:
            self._cleanup_claimed()

        try:
            scheduled = self._cleanup_scheduler.schedule(cleanup_job)
        except Exception:
            scheduled = False

        if not scheduled:
            # Submission failure must not make Managed release fail. Keep the retired
            # attempt and its reservation available for a later release/retry.
            self._reset_cleanup_claim()

    def _claim_cleanup(self) -> bool:
        with self._lock:
            if self._phase is not _AttemptPhase.RETIRED:
                return False
            if self._cleanup_in_progress:
                return False
            self._cleanup_in_progress = True
            return True

    def _reset_cleanup_claim(self) -> None:
        with self._lock:
            if self._phase is _AttemptPhase.RETIRED:
                self._cleanup_in_progress = False

    def _cleanup_claimed(self) -> Exception | None:
        """Cleanup this attempt after its cleanup claim has been acquired."""

        with self._lock:
            if (
                self._phase is not _AttemptPhase.RETIRED
                or not self._cleanup_in_progress
            ):
                raise RuntimeError("retired resource cleanup claim was lost")
            resources = self._resources
            reserved = self._reserved

        if not reserved:
            self._reset_cleanup_claim()
            raise RuntimeError("retired resource attempt is not reserved")

        try:
            self._driver.cleanup(resources)
            self._reservation_table.release_reservation(self._id)
        except Exception as exc:
            self._reset_cleanup_claim()
            return exc

        with self._lock:
            if (
                self._phase is not _AttemptPhase.RETIRED
                or not self._cleanup_in_progress
            ):
                raise RuntimeError("retired resource cleanup claim was lost")
            self._reserved = False
            self._cleanup_in_progress = False
            self._phase = _AttemptPhase.DONE

        self._notify_done()
        return None

    def _is_cancel_requested(self) -> bool:
        with self._lock:
            if self._phase is not _AttemptPhase.ACQUIRING:
                return True
            return self._cancel_requested

    def _notify_done(self) -> None:
        # Manager registry callbacks are intentionally invoked without the attempt lock.
        self._on_done(self)

    def _require_phase_locked(self, expected: _AttemptPhase) -> None:
        if self._phase is not expected:
            raise RuntimeError(
                f"resource attempt phase changed unexpectedly: "
                f"expected {expected.name}, got {self._phase.name}"
            )

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


class ResourceManager(Generic[RequestT, RequirementT, PhysicalResourceT]):
    """Own live ResourceAttempt handles and provide retired cleanup recovery."""

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
            AttemptId, _ResourceAttempt[RequestT, RequirementT, PhysicalResourceT]
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

    def open_attempt(self) -> ResourceAttempt[RequestT, PhysicalResourceT]:
        """Create, register, and return one Resource acquisition handle."""

        attempt = _ResourceAttempt[RequestT, RequirementT, PhysicalResourceT](
            AttemptId(),
            self._requirements_model,
            self._key_model,
            self._driver,
            self._reservation_table,
            self._cleanup_scheduler,
            self._remove_done_attempt,
        )
        with self._lock:
            self._attempts[attempt.id] = attempt
        return attempt

    def cleanup_retired(self, request: RequestT) -> bool:
        """Synchronously retry cleanup of retired entries for operational recovery.

        The registry is snapshotted before querying or invoking attempts, so the manager
        lock is never nested with an attempt lock.
        """

        if request is None:
            raise TypeError("request cannot be None")

        with self._lock:
            attempts = tuple(self._attempts.values())

        retired = tuple(
            attempt for attempt in attempts if attempt.matches_retired_request(request)
        )
        if not retired:
            return False

        for attempt in retired:
            error = attempt.retry_cleanup()
            if error is not None:
                raise error
        return True

    def _remove_done_attempt(
        self,
        attempt: _ResourceAttempt[RequestT, RequirementT, PhysicalResourceT],
    ) -> None:
        with self._lock:
            current = self._attempts.get(attempt.id)
            if current is attempt:
                self._attempts.pop(attempt.id, None)


__all__ = [
    "ResourceAcquisitionCancelled",
    "ResourceAttempt",
    "ResourceManagement",
    "ResourceManager",
    "ResourceRequirementsModel",
]
