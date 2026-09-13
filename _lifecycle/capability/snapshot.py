from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, TypeVar


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


class LifecyclePhase(Enum):
    """Observable phase of a synchronous capability lifecycle."""

    IDLE = "idle"
    ACQUIRING = "acquiring"
    ACTIVE = "active"
    RELEASING = "releasing"
    RELEASE_REQUIRED = "release_required"


@dataclass(frozen=True, slots=True)
class LifecycleSnapshot(Generic[GenerationT, RequestT, CapabilityT]):
    """Describe one consistent point-in-time view of a capability lifecycle.

    IDLE exposes only ``generation``. ACQUIRING and RELEASING also expose ``request``.
    ACTIVE additionally exposes ``capability``. RELEASE_REQUIRED exposes ``request`` and
    ``last_error`` and requires an explicit release attempt before the generation can
    complete.

    A snapshot can become stale immediately. Pass its ``generation`` and the matching
    ``request`` back to the coordinator to validate a subsequent lifecycle operation.
    """

    generation: GenerationT
    request: RequestT | None = None
    capability: CapabilityT | None = None
    phase: LifecyclePhase = field(default=LifecyclePhase.IDLE, kw_only=True)
    last_error: BaseException | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        if self.generation is None:
            raise TypeError("generation cannot be None")
        if not isinstance(self.phase, LifecyclePhase):
            raise TypeError("phase must be LifecyclePhase")
        if (self.request is None) != (self.phase is LifecyclePhase.IDLE):
            raise ValueError("request must be present exactly when phase is not IDLE")
        if (self.capability is not None) != (self.phase is LifecyclePhase.ACTIVE):
            raise ValueError("capability must be present exactly when phase is ACTIVE")
        if self.last_error is not None and not isinstance(self.last_error, BaseException):
            raise TypeError("last_error must be a BaseException")
        if (self.last_error is not None) != (self.phase is LifecyclePhase.RELEASE_REQUIRED):
            raise ValueError(
                "last_error must be present exactly when phase is RELEASE_REQUIRED"
            )


__all__ = ["LifecyclePhase", "LifecycleSnapshot"]
