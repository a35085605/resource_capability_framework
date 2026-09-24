from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from lifecycle.capability.result import LifecycleDiagnostics
from lifecycle.capability.session import SessionOwner
from lifecycle.capability.snapshot import CleanupOrigin
from lifecycle.resource.result import ResourceCleanupStatus


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class Idle(Generic[GenerationT]):
    generation: GenerationT


@dataclass(frozen=True, slots=True)
class Acquiring(Generic[GenerationT, RequestT]):
    generation: GenerationT
    request: RequestT


@dataclass(frozen=True, slots=True)
class Active(Generic[GenerationT, RequestT, CapabilityT]):
    generation: GenerationT
    request: RequestT
    capability: CapabilityT
    owner: SessionOwner


@dataclass(frozen=True, slots=True)
class Releasing(Generic[GenerationT, RequestT]):
    generation: GenerationT
    request: RequestT
    owner: SessionOwner


@dataclass(frozen=True, slots=True)
class CleanupPending(Generic[GenerationT, RequestT]):
    generation: GenerationT
    request: RequestT
    owner: SessionOwner
    origin: CleanupOrigin
    cleanup_status: ResourceCleanupStatus
    diagnostics: LifecycleDiagnostics

    def __post_init__(self) -> None:
        if self.cleanup_status not in (
            ResourceCleanupStatus.RETRYABLE,
            ResourceCleanupStatus.BLOCKED,
        ):
            raise ValueError("CleanupPending requires RETRYABLE or BLOCKED status")


@dataclass(frozen=True, slots=True)
class FinalizationPending(Generic[GenerationT, RequestT]):
    generation: GenerationT
    request: RequestT
    origin: CleanupOrigin
    diagnostics: LifecycleDiagnostics


@dataclass(frozen=True, slots=True)
class Recovering(Generic[GenerationT, RequestT]):
    generation: GenerationT
    request: RequestT
    origin: CleanupOrigin
    diagnostics: LifecycleDiagnostics
    owner: SessionOwner | None = None
    cleanup_status: ResourceCleanupStatus | None = None


LifecycleState: TypeAlias = (
    Idle[GenerationT]
    | Acquiring[GenerationT, RequestT]
    | Active[GenerationT, RequestT, CapabilityT]
    | Releasing[GenerationT, RequestT]
    | CleanupPending[GenerationT, RequestT]
    | FinalizationPending[GenerationT, RequestT]
    | Recovering[GenerationT, RequestT]
)


__all__ = [
    "Acquiring",
    "Active",
    "CleanupPending",
    "FinalizationPending",
    "Idle",
    "LifecycleState",
    "Recovering",
    "Releasing",
]
