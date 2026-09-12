from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _resource.driver import PhysicalResourceSet


PhysicalResourceT = TypeVar("PhysicalResourceT")


@dataclass(frozen=True, slots=True)
class ResourceAcquired(Generic[PhysicalResourceT]):
    resources: PhysicalResourceSet[PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class ResourceBlocked:
    reason: str = "resource key conflict"


@dataclass(frozen=True, slots=True)
class ResourceFailed:
    error: Exception


ResourceAcquireResult: TypeAlias = (
    ResourceAcquired[PhysicalResourceT] | ResourceBlocked | ResourceFailed
)


__all__ = [
    "ResourceAcquireResult",
    "ResourceAcquired",
    "ResourceBlocked",
    "ResourceFailed",
]
