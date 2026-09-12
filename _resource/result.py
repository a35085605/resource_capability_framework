from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

ValueT = TypeVar("ValueT")


@dataclass(frozen=True, slots=True)
class ResourceReady(Generic[ValueT]):
    value: ValueT


@dataclass(frozen=True, slots=True)
class ResourceBlocked:
    reason: str = "resource key conflict"


ResourceAcquireResult: TypeAlias = ResourceReady[ValueT] | ResourceBlocked


__all__ = [
    "ResourceAcquireResult",
    "ResourceBlocked",
    "ResourceReady",
]
