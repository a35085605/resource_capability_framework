from __future__ import annotations

from collections.abc import Callable
from enum import Enum, auto
from typing import Generic, Protocol, TypeVar

from _attempt import AttemptId
from _resource.claim import ResourceClaim, ResourceClaims
from _resource.driver import (
    PhysicalAcquired,
    PhysicalFailed,
    PhysicalResourceSet,
    ResourceDriver,
)
from _resource.key import ResourceKey, ResourceKeyModel
from _resource.policy import ResourcePolicy
from _resource.pool import GLOBAL_RESOURCE_RESERVATION_TABLE, ResourceReservationTable
from _resource.requirement import ResourceRequirement, ResourceRequirements
from _resource.result import ResourceAcquireResult, ResourceBlocked, ResourceReady


RequestT = TypeVar("RequestT")
RequirementT = TypeVar("RequirementT", bound=ResourceRequirement)
PhysicalResourceT = TypeVar("PhysicalResourceT")
ValueT = TypeVar("ValueT")


class ResourceCleanupPendingError(RuntimeError):
    """Acquisition/projection failed and rollback cleanup also failed.

    The attempt still owns its reservation and remaining resources. The caller must
    explicitly retry ``release`` before the reservation can be considered available.
    """

    def __init__(self, primary_error: Exception, cleanup_error: Exception) -> None:
        if not isinstance(primary_error, Exception):
            raise TypeError("primary_error must be an Exception")
        if not isinstance(cleanup_error, Exception):
            raise TypeError("cleanup_error must be an Exception")
        self.primary_error = primary_error
        self.cleanup_error = cleanup_error
        super().__init__(
            f"{primary_error}; rollback resource cleanup also failed: {cleanup_error}"
        )


class ResourceRequirementsModel(Protocol[RequestT, RequirementT]):
    """Resolve only the physical-resource requirements for one Request."""

    def requirements(self, request: RequestT) -> ResourceRequirements[RequirementT]: ...


class ResourceAttempt(Protocol[RequestT, PhysicalResourceT]):
    """Serial managed-facing handle for one Resource acquisition lifecycle.

    ``acquire`` performs physical acquisition and capability projection synchronously.
    It returns only ready/blocked results; acquisition and projection failures are raised.
    ``release`` performs cleanup synchronously and returns only after cleanup succeeds.
    If cleanup fails, the attempt retains its reservation and resources for an explicit
    retry through ``release``.
    """

    @property
    def id(self) -> AttemptId: ...

    @property
    def cleanup_pending(self) -> bool: ...

    def acquire(
        self,
        request: RequestT,
        use_resources: Callable[[PhysicalResourceSet[PhysicalResourceT]], ValueT],
    ) -> ResourceAcquireResult[ValueT]: ...

    def release(self) -> None: ...


class ResourceManagement(Protocol[RequestT, PhysicalResourceT]):
    """Managed-facing boundary for synchronous physical-resource lifecycles."""

    def open_attempt(self) -> ResourceAttempt[RequestT, PhysicalResourceT]: ...


class _AttemptPhase(Enum):
    OPEN = auto()
    ACTIVE = auto()
    CLEANUP_PENDING = auto()
    DONE = auto()


class _ResourceAttempt(Generic[RequestT, RequirementT, PhysicalResourceT]):
    """Own one serial acquisition, retention, and cleanup lifecycle."""

    def __init__(
        self,
        attempt_id: AttemptId,
        requirements_model: ResourceRequirementsModel[RequestT, RequirementT],
        key_model: ResourceKeyModel[RequirementT],
        driver: ResourceDriver[RequirementT, PhysicalResourceT],
        reservation_table: ResourceReservationTable[RequestT, RequirementT],
    ) -> None:
        self._id = attempt_id
        self._requirements_model = requirements_model
        self._key_model = key_model
        self._driver = driver
        self._reservation_table = reservation_table

        self._phase = _AttemptPhase.OPEN
        self._resources: PhysicalResourceSet[PhysicalResourceT] = ()
        self._reserved = False

    @property
    def id(self) -> AttemptId:
        return self._id

    @property
    def cleanup_pending(self) -> bool:
        return self._phase is _AttemptPhase.CLEANUP_PENDING

    def acquire(
        self,
        request: RequestT,
        use_resources: Callable[[PhysicalResourceSet[PhysicalResourceT]], ValueT],
    ) -> ResourceAcquireResult[ValueT]:
        if request is None:
            raise TypeError("request cannot be None")
        if not callable(use_resources):
            raise TypeError("use_resources must be callable")
        if self._phase is not _AttemptPhase.OPEN:
            raise RuntimeError("attempt can begin only from OPEN")

        # Until reservation succeeds there is nothing to retain or clean up.
        self._phase = _AttemptPhase.DONE

        try:
            requirements = self._requirements_model.requirements(request)
            claims = self._resolve_claims(requirements)

            reserved = self._reservation_table.reserve(
                self._id,
                request,
                claims,
            )
            if not reserved:
                return ResourceBlocked()

            self._reserved = True
            # From this point onward every unsuccessful exit must preserve cleanup
            # ownership until synchronous cleanup succeeds.
            self._phase = _AttemptPhase.CLEANUP_PENDING

            self._acquire_requirements(claims)

            value = use_resources(self._resources)
            self._phase = _AttemptPhase.ACTIVE
            return ResourceReady(value)

        except BaseException as exc:
            if self._reserved:
                cleanup_error = self._cleanup_reserved()
                if cleanup_error is not None:
                    if isinstance(exc, Exception):
                        raise ResourceCleanupPendingError(exc, cleanup_error) from exc
                    exc.add_note(f"resource cleanup also failed: {cleanup_error!r}")
            else:
                self._phase = _AttemptPhase.DONE
            raise

    def release(self) -> None:
        """Synchronously clean up this attempt, retrying pending cleanup when needed."""

        if self._phase is _AttemptPhase.DONE:
            return
        if self._phase is _AttemptPhase.ACTIVE:
            self._phase = _AttemptPhase.CLEANUP_PENDING
        elif self._phase is not _AttemptPhase.CLEANUP_PENDING:
            raise RuntimeError("attempt can be released only after acquisition")

        cleanup_error = self._cleanup_reserved()
        if cleanup_error is not None:
            raise cleanup_error

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

    def _acquire_requirements(
        self,
        claims: ResourceClaims[RequirementT],
    ) -> None:
        for claim in claims:
            outcome = self._driver.acquire(claim.requirement)
            resources = self._validate_physical_outcome(outcome)
            self._resources += resources

            if isinstance(outcome, PhysicalFailed):
                if not isinstance(outcome.error, Exception):
                    raise TypeError("PhysicalFailed.error must be an Exception")
                raise outcome.error
            if not isinstance(outcome, PhysicalAcquired):
                raise RuntimeError("unsupported PhysicalAcquireOutcome")

    def _cleanup_reserved(self) -> Exception | None:
        """Attempt synchronous cleanup while preserving ownership on failure."""

        if not self._reserved:
            self._phase = _AttemptPhase.DONE
            return None

        self._phase = _AttemptPhase.CLEANUP_PENDING
        if self._resources:
            try:
                self._driver.cleanup(self._resources)
            except Exception as exc:
                return exc

        # Physical cleanup has completed. Clear the resource set before releasing the
        # logical reservation so a reservation-table failure cannot make a later retry
        # clean already-retired resources again.
        self._resources = ()
        try:
            self._reservation_table.release_reservation(self._id)
        except Exception as exc:
            return exc

        self._reserved = False
        self._phase = _AttemptPhase.DONE
        return None

    @staticmethod
    def _validate_physical_outcome(
        outcome: object,
    ) -> PhysicalResourceSet[PhysicalResourceT]:
        if not isinstance(outcome, (PhysicalAcquired, PhysicalFailed)):
            raise TypeError(
                "ResourceDriver.acquire() must return a PhysicalAcquireOutcome"
            )
        if not isinstance(outcome.resources, tuple):
            raise TypeError("physical outcome resources must be a PhysicalResourceSet tuple")
        return outcome.resources


class ResourceManager(Generic[RequestT, RequirementT, PhysicalResourceT]):
    """Create serial ResourceAttempt handles over a shared reservation table."""

    def __init__(
        self,
        requirements_model: ResourceRequirementsModel[RequestT, RequirementT],
        key_model: ResourceKeyModel[RequirementT],
        driver: ResourceDriver[RequirementT, PhysicalResourceT],
        *,
        reservation_table: ResourceReservationTable[
            RequestT, RequirementT
        ] = GLOBAL_RESOURCE_RESERVATION_TABLE,
    ) -> None:
        self._requirements_model = requirements_model
        if not callable(getattr(key_model, "key_for", None)):
            raise TypeError("key_model must provide key_for(requirement)")
        self._key_model = key_model
        self._driver = driver
        if not isinstance(reservation_table, ResourceReservationTable):
            raise TypeError("reservation_table must be ResourceReservationTable")
        self._reservation_table = reservation_table

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

    def open_attempt(self) -> ResourceAttempt[RequestT, PhysicalResourceT]:
        """Create one serial Resource acquisition handle."""

        return _ResourceAttempt[RequestT, RequirementT, PhysicalResourceT](
            AttemptId(),
            self._requirements_model,
            self._key_model,
            self._driver,
            self._reservation_table,
        )


__all__ = [
    "ResourceCleanupPendingError",
    "ResourceAttempt",
    "ResourceManagement",
    "ResourceManager",
    "ResourceRequirementsModel",
]
