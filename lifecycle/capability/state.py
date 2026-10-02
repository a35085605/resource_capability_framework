from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from lifecycle.resource import ResourceHandle


RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class Idle:
    generation: int


@dataclass(frozen=True, slots=True)
class Acquiring(Generic[RequestT]):
    generation: int
    request: RequestT


@dataclass(frozen=True, slots=True)
class Active(Generic[RequestT, CapabilityT]):
    generation: int
    request: RequestT
    handle: ResourceHandle[CapabilityT]


@dataclass(frozen=True, slots=True)
class Releasing(Generic[RequestT, CapabilityT]):
    generation: int
    request: RequestT
    handle: ResourceHandle[CapabilityT]


LifecycleState: TypeAlias = (
    Idle
    | Acquiring[RequestT]
    | Active[RequestT, CapabilityT]
    | Releasing[RequestT, CapabilityT]
)


__all__ = ["Acquiring", "Active", "Idle", "LifecycleState", "Releasing"]
