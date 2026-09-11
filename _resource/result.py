from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _resource.driver import ResourceSet


ResourceT = TypeVar("ResourceT")


@dataclass(frozen=True, slots=True)
class ResourceAcquired(Generic[ResourceT]):
    resources: ResourceSet[ResourceT]


@dataclass(frozen=True, slots=True)
class ResourceBlocked:
    reason: str = "resource conflict"


@dataclass(frozen=True, slots=True)
class ResourceFailed:
    error: Exception


ResourceAcquireResult: TypeAlias = (
    ResourceAcquired[ResourceT] | ResourceBlocked | ResourceFailed
)


__all__ = [
    "ResourceAcquireResult",
    "ResourceAcquired",
    "ResourceBlocked",
    "ResourceFailed",
]
