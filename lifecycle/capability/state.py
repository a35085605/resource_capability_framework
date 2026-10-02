from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from lifecycle.resource import ResourceScope


RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class Idle:
    generation: int


@dataclass(frozen=True, slots=True)
class Acquiring(Generic[RequestT]):
    generation: int
    request: RequestT
    scope: ResourceScope


@dataclass(frozen=True, slots=True)
class Active(Generic[RequestT, CapabilityT]):
    generation: int
    request: RequestT
    capability: CapabilityT
    scope: ResourceScope


@dataclass(frozen=True, slots=True)
class Releasing(Generic[RequestT]):
    generation: int
    request: RequestT
    scope: ResourceScope


LifecycleState: TypeAlias = (
    Idle
    | Acquiring[RequestT]
    | Active[RequestT, CapabilityT]
    | Releasing[RequestT]
)


__all__ = ["Acquiring", "Active", "Idle", "LifecycleState", "Releasing"]
