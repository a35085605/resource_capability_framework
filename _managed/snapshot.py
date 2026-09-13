from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, TypeVar


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


class ManagedPhase(Enum):
    """Observable progress of a synchronous Managed lifecycle."""

    IDLE = "idle"
    ACQUIRING = "acquiring"
    CURRENT = "current"
    RELEASING = "releasing"
    CLEANUP_PENDING = "cleanup_pending"


@dataclass(frozen=True, slots=True)
class Snapshot(Generic[GenerationT, RequestT, CapabilityT]):
    """Point-in-time lifecycle observation; it does not lease Capability.

    ``request`` identifies the lifecycle in every phase except IDLE. Only CURRENT
    exposes ``capability``. ACQUIRING includes physical acquisition and projection.
    RELEASING includes physical cleanup and generation advancement.
    CLEANUP_PENDING retains the request and ``last_error`` until an explicit release
    completes, including acquisition/projection failure, cleanup failure, or generation
    advancement failure after cleanup has already succeeded.

    Observations can become stale immediately. Pass ``generation`` and ``request``
    back to the coordinator to validate an operation; reading is not a reservation.
    """

    generation: GenerationT
    request: RequestT | None = None
    capability: CapabilityT | None = None
    phase: ManagedPhase = field(default=ManagedPhase.IDLE, kw_only=True)
    last_error: BaseException | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        if self.generation is None:
            raise TypeError("generation cannot be None")
        if not isinstance(self.phase, ManagedPhase):
            raise TypeError("phase must be ManagedPhase")
        if (self.request is None) != (self.phase is ManagedPhase.IDLE):
            raise ValueError("request must be present exactly when phase is not IDLE")
        if (self.capability is not None) != (self.phase is ManagedPhase.CURRENT):
            raise ValueError("capability must be present exactly when phase is CURRENT")
        if self.last_error is not None and not isinstance(self.last_error, BaseException):
            raise TypeError("last_error must be a BaseException")
        if (self.last_error is not None) != (self.phase is ManagedPhase.CLEANUP_PENDING):
            raise ValueError(
                "last_error must be present exactly when phase is CLEANUP_PENDING"
            )


__all__ = ["ManagedPhase", "Snapshot"]
