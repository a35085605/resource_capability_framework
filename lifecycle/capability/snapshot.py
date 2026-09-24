from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, TypeVar

from lifecycle.capability.diagnostics import LifecycleDiagnostics
from lifecycle.resource.result import ResourceCleanupStatus


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


class CleanupOrigin(Enum):
    ACQUIRE = "acquire"
    RELEASE = "release"


class LifecyclePhase(Enum):
    """Observable phase of a synchronous capability lifecycle."""

    IDLE = "idle"
    ACQUIRING = "acquiring"
    ACTIVE = "active"
    RELEASING = "releasing"
    CLEANUP_PENDING = "cleanup_pending"
    FINALIZATION_PENDING = "finalization_pending"
    RECOVERING = "recovering"


@dataclass(frozen=True, slots=True)
class LifecycleSnapshot(Generic[GenerationT, RequestT, CapabilityT]):
    """Consistent point-in-time lifecycle state without exposing physical ownership."""

    generation: GenerationT
    request: RequestT | None = None
    capability: CapabilityT | None = None
    phase: LifecyclePhase = field(default=LifecyclePhase.IDLE, kw_only=True)
    origin: CleanupOrigin | None = field(default=None, kw_only=True)
    cleanup_status: ResourceCleanupStatus | None = field(default=None, kw_only=True)
    diagnostics: LifecycleDiagnostics | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        if self.generation is None:
            raise TypeError("generation cannot be None")
        if not isinstance(self.phase, LifecyclePhase):
            raise TypeError("phase must be LifecyclePhase")
        if (self.request is None) != (self.phase is LifecyclePhase.IDLE):
            raise ValueError("request must be present exactly when phase is not IDLE")
        if (self.capability is not None) != (self.phase is LifecyclePhase.ACTIVE):
            raise ValueError("capability must be present exactly when phase is ACTIVE")

        pending_or_recovering = self.phase in (
            LifecyclePhase.CLEANUP_PENDING,
            LifecyclePhase.FINALIZATION_PENDING,
            LifecyclePhase.RECOVERING,
        )
        if (self.origin is not None) != pending_or_recovering:
            raise ValueError("origin must be present exactly for pending/recovering phases")
        if self.origin is not None and not isinstance(self.origin, CleanupOrigin):
            raise TypeError("origin must be CleanupOrigin")

        if self.cleanup_status is not None and not isinstance(
            self.cleanup_status, ResourceCleanupStatus
        ):
            raise TypeError("cleanup_status must be ResourceCleanupStatus or None")
        if self.phase is LifecyclePhase.CLEANUP_PENDING and self.cleanup_status not in (
            ResourceCleanupStatus.RETRYABLE,
            ResourceCleanupStatus.BLOCKED,
        ):
            raise ValueError("CLEANUP_PENDING requires RETRYABLE or BLOCKED cleanup status")
        if self.phase not in (
            LifecyclePhase.CLEANUP_PENDING,
            LifecyclePhase.RECOVERING,
        ) and self.cleanup_status is not None:
            raise ValueError("cleanup_status is only valid for cleanup pending/recovery")

        if pending_or_recovering:
            if not isinstance(self.diagnostics, LifecycleDiagnostics):
                raise TypeError("pending/recovering snapshots require LifecycleDiagnostics")
        elif self.diagnostics is not None:
            raise ValueError("diagnostics are exposed only while work remains pending")


__all__ = ["CleanupOrigin", "LifecyclePhase", "LifecycleSnapshot"]
