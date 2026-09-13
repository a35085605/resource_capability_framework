from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _lifecycle_new.resource.driver import PhysicalResources


PhysicalResourceT = TypeVar("PhysicalResourceT")


@dataclass(frozen=True, slots=True)
class ResourceAcquireSucceeded(Generic[PhysicalResourceT]):
    """Report that all requirements were acquired successfully."""

    resources: PhysicalResources[PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class ResourceAcquireFailed(Generic[PhysicalResourceT]):
    """Report acquisition failure while retaining all reported resources.

    The caller is responsible for passing ``resources`` to release when lifecycle
    cleanup is required.
    """

    error: BaseException
    resources: PhysicalResources[PhysicalResourceT]


ResourceAcquireResult: TypeAlias = (
    ResourceAcquireSucceeded[PhysicalResourceT]
    | ResourceAcquireFailed[PhysicalResourceT]
)


__all__ = [
    "ResourceAcquireFailed",
    "ResourceAcquireResult",
    "ResourceAcquireSucceeded",
]
