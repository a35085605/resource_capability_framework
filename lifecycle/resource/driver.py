from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

from lifecycle.resource.result import ResourceCleanupResult

RequirementT = TypeVar("RequirementT", contravariant=True)
PhysicalResourceT = TypeVar("PhysicalResourceT")


type PhysicalResources[T] = tuple[T, ...]


@dataclass(frozen=True, slots=True)
class RequirementAcquireSucceeded(Generic[PhysicalResourceT]):
    """Report successful acquisition for one physical-resource requirement."""

    resources: PhysicalResources[PhysicalResourceT]

    def __post_init__(self) -> None:
        if not isinstance(self.resources, tuple):
            raise TypeError("resources must be a PhysicalResources tuple")


@dataclass(frozen=True, slots=True)
class RequirementAcquireFailed(Generic[PhysicalResourceT]):
    """Report terminal acquisition failure for one requirement."""

    error: Exception
    resources: PhysicalResources[PhysicalResourceT]
    cleanup_errors: tuple[BaseException, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.error, Exception):
            raise TypeError("error must be an Exception")
        if not isinstance(self.resources, tuple):
            raise TypeError("resources must be a PhysicalResources tuple")
        if not isinstance(self.cleanup_errors, tuple):
            raise TypeError("cleanup_errors must be a tuple")
        if not all(isinstance(error, BaseException) for error in self.cleanup_errors):
            raise TypeError("cleanup_errors must contain only BaseException values")


@dataclass(frozen=True, slots=True)
class RequirementAcquireInterrupted(Generic[PhysicalResourceT]):
    """Preserve ownership while a non-Exception control-flow interruption propagates."""

    error: BaseException
    resources: PhysicalResources[PhysicalResourceT]
    cleanup_errors: tuple[BaseException, ...] = ()
    operation_error: BaseException | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.resources, tuple):
            raise TypeError("resources must be a PhysicalResources tuple")
        if not isinstance(self.cleanup_errors, tuple):
            raise TypeError("cleanup_errors must be a tuple")
        if not all(isinstance(error, BaseException) for error in self.cleanup_errors):
            raise TypeError("cleanup_errors must contain only BaseException values")
        if isinstance(self.error, Exception) or not isinstance(self.error, BaseException):
            raise TypeError("RequirementAcquireInterrupted.error must be a non-Exception BaseException")
        if self.operation_error is not None and not isinstance(
            self.operation_error, BaseException
        ):
            raise TypeError("operation_error must be a BaseException or None")


type RequirementAcquireResult[T] = (
    RequirementAcquireSucceeded[T]
    | RequirementAcquireFailed[T]
    | RequirementAcquireInterrupted[T]
)


class ResourceDriver(Protocol[RequirementT, PhysicalResourceT]):
    """Perform synchronous physical I/O for individual requirements.

    Cleanup reports ownership explicitly. Expected cleanup failures must be represented
    by ``ResourceCleanupResult`` instead of inferred from exceptions; a driver may still
    raise for programmer errors or control-flow interruption.
    """

    def acquire(
        self,
        requirement: RequirementT,
    ) -> RequirementAcquireResult[PhysicalResourceT]: ...

    def cleanup(
        self,
        resources: PhysicalResources[PhysicalResourceT],
    ) -> ResourceCleanupResult[PhysicalResourceT]: ...


__all__ = [
    "RequirementAcquireFailed",
    "RequirementAcquireInterrupted",
    "RequirementAcquireResult",
    "RequirementAcquireSucceeded",
    "PhysicalResources",
    "ResourceDriver",
]
