from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeAlias, TypeVar

PhysicalResourceT = TypeVar("PhysicalResourceT")


@dataclass(frozen=True, slots=True)
class ResourceAcquireSucceeded(Generic[PhysicalResourceT]):
    """Report that all requirements were acquired successfully."""

    resources: tuple[PhysicalResourceT, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.resources, tuple):
            raise TypeError("resources must be a PhysicalResources tuple")


@dataclass(frozen=True, slots=True)
class ResourceAcquireFailed(Generic[PhysicalResourceT]):
    """Report acquisition failure while retaining all reported resources.

    The caller owns ``resources`` until an explicit cleanup result proves otherwise.
    Control-flow interruption uses ``ResourceAcquireInterrupted`` instead.
    """

    error: Exception
    resources: tuple[PhysicalResourceT, ...]
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
class ResourceAcquireInterrupted(Generic[PhysicalResourceT]):
    """Preserve ownership and an interruption separately from the operation failure."""

    error: BaseException
    resources: tuple[PhysicalResourceT, ...]
    cleanup_errors: tuple[BaseException, ...] = ()
    operation_error: BaseException | None = None

    def __post_init__(self) -> None:
        if isinstance(self.error, Exception) or not isinstance(self.error, BaseException):
            raise TypeError("error must be a non-Exception BaseException")
        if not isinstance(self.resources, tuple):
            raise TypeError("resources must be a PhysicalResources tuple")
        if not isinstance(self.cleanup_errors, tuple) or not all(
            isinstance(error, BaseException) for error in self.cleanup_errors
        ):
            raise TypeError("cleanup_errors must be a tuple of BaseException values")
        if self.operation_error is not None and not isinstance(
            self.operation_error, BaseException
        ):
            raise TypeError("operation_error must be a BaseException or None")


ResourceAcquireResult: TypeAlias = (
    ResourceAcquireSucceeded[PhysicalResourceT]
    | ResourceAcquireFailed[PhysicalResourceT]
    | ResourceAcquireInterrupted[PhysicalResourceT]
)


class ResourceCleanupStatus(Enum):
    """Whether one cleanup attempt discharged its ownership responsibility."""

    COMPLETE = "complete"
    RETRYABLE = "retryable"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class ResourceCleanupResult(Generic[PhysicalResourceT]):
    """Explicit ownership result from one resource cleanup attempt.

    ``remaining_resources`` is the complete set of physical resources whose cleanup
    responsibility remains owned by the caller after this attempt. ``errors`` contains
    every diagnostic produced by cleanup work that actually ran. A COMPLETE result may
    still contain errors when the adapter can prove that ownership was relinquished in
    spite of an error report.
    """

    status: ResourceCleanupStatus
    remaining_resources: tuple[PhysicalResourceT, ...] = ()
    errors: tuple[BaseException, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.status, ResourceCleanupStatus):
            raise TypeError("status must be ResourceCleanupStatus")
        if not isinstance(self.remaining_resources, tuple):
            raise TypeError("remaining_resources must be a PhysicalResources tuple")
        if not isinstance(self.errors, tuple):
            raise TypeError("errors must be a tuple")
        if not all(isinstance(error, BaseException) for error in self.errors):
            raise TypeError("errors must contain only BaseException values")
        if self.status is ResourceCleanupStatus.COMPLETE:
            if self.remaining_resources:
                raise ValueError("COMPLETE cleanup cannot retain resources")
        elif not self.remaining_resources:
            raise ValueError("incomplete cleanup must retain at least one resource")

    @classmethod
    def complete(
        cls,
        *,
        errors: tuple[BaseException, ...] = (),
    ) -> "ResourceCleanupResult[PhysicalResourceT]":
        return cls(ResourceCleanupStatus.COMPLETE, (), errors)

    @classmethod
    def retryable(
        cls,
        resources: tuple[PhysicalResourceT, ...],
        *,
        errors: tuple[BaseException, ...] = (),
    ) -> "ResourceCleanupResult[PhysicalResourceT]":
        return cls(ResourceCleanupStatus.RETRYABLE, resources, errors)

    @classmethod
    def blocked(
        cls,
        resources: tuple[PhysicalResourceT, ...],
        *,
        errors: tuple[BaseException, ...] = (),
    ) -> "ResourceCleanupResult[PhysicalResourceT]":
        return cls(ResourceCleanupStatus.BLOCKED, resources, errors)


__all__ = [
    "ResourceAcquireFailed",
    "ResourceAcquireInterrupted",
    "ResourceAcquireResult",
    "ResourceAcquireSucceeded",
    "ResourceCleanupResult",
    "ResourceCleanupStatus",
]
