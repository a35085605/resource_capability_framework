from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, TypeVar


RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


class LifecyclePhase(Enum):
    """Observable phase of a synchronous capability lifecycle."""

    IDLE = "idle"
    ACQUIRING = "acquiring"
    ACTIVE = "active"
    RELEASING = "releasing"


@dataclass(frozen=True, slots=True)
class LifecycleSnapshot(Generic[RequestT, CapabilityT]):
    """Consistent point-in-time lifecycle state without exposing resource ownership."""

    generation: int
    request: RequestT | None = None
    capability: CapabilityT | None = None
    phase: LifecyclePhase = field(default=LifecyclePhase.IDLE, kw_only=True)

    def __post_init__(self) -> None:
        if not isinstance(self.generation, int) or self.generation < 0:
            raise TypeError("generation must be a non-negative int")
        if not isinstance(self.phase, LifecyclePhase):
            raise TypeError("phase must be LifecyclePhase")
        if (self.request is None) != (self.phase is LifecyclePhase.IDLE):
            raise ValueError("request must be present exactly when phase is not IDLE")
        if (self.capability is not None) != (self.phase is LifecyclePhase.ACTIVE):
            raise ValueError("capability must be present exactly when phase is ACTIVE")


__all__ = ["LifecyclePhase", "LifecycleSnapshot"]
