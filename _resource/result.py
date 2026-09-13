from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _resource.driver import PhysicalResourceSet


PhysicalResourceT = TypeVar("PhysicalResourceT")


@dataclass(frozen=True, slots=True)
class ResourceReady(Generic[PhysicalResourceT]):
    """All requirements were acquired successfully."""

    resources: PhysicalResourceSet[PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class ResourceFailed(Generic[PhysicalResourceT]):
    """Acquisition failed while retaining every resource obtained so far."""

    error: BaseException
    resources: PhysicalResourceSet[PhysicalResourceT]


ResourceAcquireResult: TypeAlias = (
    ResourceReady[PhysicalResourceT] | ResourceFailed[PhysicalResourceT]
)


__all__ = [
    "ResourceAcquireResult",
    "ResourceFailed",
    "ResourceReady",
]
