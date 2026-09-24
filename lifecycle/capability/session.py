from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

from lifecycle.capability.projection import CapabilityProjector
from lifecycle.resource.contract import ResourceProvider
from lifecycle.resource.driver import PhysicalResources
from lifecycle.resource.result import (
    ResourceAcquireFailed,
    ResourceAcquireInterrupted,
    ResourceAcquireSucceeded,
    ResourceCleanupResult,
    ResourceCleanupStatus,
)


RequestT = TypeVar("RequestT")
PhysicalResourceT = TypeVar("PhysicalResourceT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class CleanupReport:
    """Lifecycle-facing cleanup report without exposing physical resources."""

    status: ResourceCleanupStatus
    errors: tuple[BaseException, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.status, ResourceCleanupStatus):
            raise TypeError("status must be ResourceCleanupStatus")
        if not isinstance(self.errors, tuple):
            raise TypeError("errors must be a tuple")
        if not all(isinstance(error, BaseException) for error in self.errors):
            raise TypeError("errors must contain only BaseException values")


class SessionOwner(Protocol):
    """Own one prepared capability instance's retained physical responsibilities."""

    @property
    def has_ownership(self) -> bool: ...

    def cleanup(self) -> CleanupReport: ...


@dataclass(frozen=True, slots=True)
class PreparedSession(Generic[CapabilityT]):
    capability: CapabilityT
    owner: SessionOwner

    def __post_init__(self) -> None:
        if self.capability is None:
            raise TypeError("capability cannot be None")
        if not callable(getattr(self.owner, "cleanup", None)):
            raise TypeError("owner must provide cleanup()")


@dataclass(frozen=True, slots=True)
class PreparationFailed:
    error: BaseException
    owner: SessionOwner
    cleanup_errors: tuple[BaseException, ...] = ()
    interruption: BaseException | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.error, BaseException):
            raise TypeError("error must be a BaseException")
        if not callable(getattr(self.owner, "cleanup", None)):
            raise TypeError("owner must provide cleanup()")
        if not isinstance(self.cleanup_errors, tuple):
            raise TypeError("cleanup_errors must be a tuple")
        if not all(isinstance(error, BaseException) for error in self.cleanup_errors):
            raise TypeError("cleanup_errors must contain only BaseException values")
        if self.interruption is not None:
            if isinstance(self.interruption, Exception) or not isinstance(
                self.interruption, BaseException
            ):
                raise TypeError(
                    "interruption must be a non-Exception BaseException or None"
                )


type SessionPreparationResult[T] = PreparedSession[T] | PreparationFailed


class CapabilitySessionFactory(Protocol[RequestT, CapabilityT]):
    def prepare(self, request: RequestT) -> SessionPreparationResult[CapabilityT]: ...


class ResourceSessionOwner(Generic[RequestT, PhysicalResourceT]):
    """Default owner that keeps remaining ResourceProvider ownership private."""

    __slots__ = ("_provider", "_resources")

    def __init__(
        self,
        provider: ResourceProvider[RequestT, PhysicalResourceT],
        resources: PhysicalResources[PhysicalResourceT],
    ) -> None:
        if not callable(getattr(provider, "cleanup", None)):
            raise TypeError("provider must provide cleanup()")
        if not isinstance(resources, tuple):
            raise TypeError("resources must be a PhysicalResources tuple")
        self._provider = provider
        self._resources = resources

    @property
    def has_ownership(self) -> bool:
        return bool(self._resources)

    def cleanup(self) -> CleanupReport:
        if not self._resources:
            return CleanupReport(ResourceCleanupStatus.COMPLETE)

        resources = self._resources
        outcome = self._provider.cleanup(resources)
        if not isinstance(outcome, ResourceCleanupResult):
            return CleanupReport(
                ResourceCleanupStatus.BLOCKED,
                (TypeError("ResourceProvider.cleanup() must return ResourceCleanupResult"),),
            )

        self._resources = outcome.remaining_resources
        return CleanupReport(outcome.status, outcome.errors)


class _BlockedSessionOwner:
    """Conservative owner used when a custom factory violates the preparation contract."""

    __slots__ = ("_error",)

    def __init__(self, error: BaseException) -> None:
        self._error = error

    @property
    def has_ownership(self) -> bool:
        return True

    def cleanup(self) -> CleanupReport:
        return CleanupReport(ResourceCleanupStatus.BLOCKED, (self._error,))


class DefaultCapabilitySessionFactory(
    Generic[RequestT, PhysicalResourceT, CapabilityT]
):
    """Prepare resource ownership first, then project the consumer capability view."""

    __slots__ = ("_projector", "_provider")

    def __init__(
        self,
        provider: ResourceProvider[RequestT, PhysicalResourceT],
        projector: CapabilityProjector[RequestT, PhysicalResourceT, CapabilityT],
    ) -> None:
        if not callable(getattr(provider, "acquire", None)):
            raise TypeError("provider must provide acquire()")
        if not callable(getattr(provider, "cleanup", None)):
            raise TypeError("provider must provide cleanup()")
        if not callable(getattr(projector, "project", None)):
            raise TypeError("projector must provide project()")
        self._provider = provider
        self._projector = projector

    def prepare(self, request: RequestT) -> SessionPreparationResult[CapabilityT]:
        try:
            acquisition = self._provider.acquire(request)
        except BaseException as exc:
            # The provider contract transfers ownership only through its return value.
            # A throwing provider therefore gives us no concrete ownership to clean.
            owner = ResourceSessionOwner(self._provider, ())
            return PreparationFailed(
                exc,
                owner,
                interruption=None if isinstance(exc, Exception) else exc,
            )

        if isinstance(acquisition, ResourceAcquireInterrupted):
            owner = ResourceSessionOwner(self._provider, acquisition.resources)
            primary_error = acquisition.operation_error or acquisition.error
            return PreparationFailed(
                primary_error,
                owner,
                acquisition.cleanup_errors,
                acquisition.error,
            )

        if isinstance(acquisition, ResourceAcquireFailed):
            resources = acquisition.resources
            if not isinstance(resources, tuple):
                error = TypeError(
                    "ResourceAcquireFailed.resources must be a PhysicalResources tuple"
                )
                return PreparationFailed(error, _BlockedSessionOwner(error))
            owner = ResourceSessionOwner(self._provider, resources)
            error = acquisition.error
            if not isinstance(error, BaseException):
                error = TypeError("ResourceAcquireFailed.error must be a BaseException")
            return PreparationFailed(error, owner, acquisition.cleanup_errors)

        if not isinstance(acquisition, ResourceAcquireSucceeded):
            error = TypeError("ResourceProvider.acquire() must return a ResourceAcquireResult")
            return PreparationFailed(error, _BlockedSessionOwner(error))
        if not isinstance(acquisition.resources, tuple):
            error = TypeError(
                "ResourceAcquireSucceeded.resources must be a PhysicalResources tuple"
            )
            return PreparationFailed(error, _BlockedSessionOwner(error))

        # Ownership is wrapped before projection, closing the leak window if projection
        # fails after all physical resources were acquired.
        owner = ResourceSessionOwner(self._provider, acquisition.resources)
        try:
            capability = self._projector.project(request, acquisition.resources)
            if capability is None:
                raise TypeError("CapabilityProjector.project() cannot return None")
        except BaseException as exc:
            return PreparationFailed(
                exc,
                owner,
                interruption=None if isinstance(exc, Exception) else exc,
            )
        return PreparedSession(capability, owner)


__all__ = [
    "CapabilitySessionFactory",
    "CleanupReport",
    "DefaultCapabilitySessionFactory",
    "PreparationFailed",
    "PreparedSession",
    "ResourceSessionOwner",
    "SessionOwner",
    "SessionPreparationResult",
]
