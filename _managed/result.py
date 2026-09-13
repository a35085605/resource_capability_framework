from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _managed.snapshot import ManagedPhase, Snapshot


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class GenerationMismatch(Generic[GenerationT]):
    current_generation: GenerationT


@dataclass(frozen=True, slots=True)
class Busy:
    """Another synchronous lifecycle operation is currently performing I/O."""

    phase: ManagedPhase

    def __post_init__(self) -> None:
        if self.phase not in (ManagedPhase.ACQUIRING, ManagedPhase.RELEASING):
            raise ValueError("Busy phase must be ACQUIRING or RELEASING")


@dataclass(frozen=True, slots=True)
class AcquireExisting(Generic[GenerationT, RequestT, CapabilityT]):
    snapshot: Snapshot[GenerationT, RequestT, CapabilityT]


@dataclass(frozen=True, slots=True)
class AcquireRequestMismatch(Generic[RequestT]):
    current_request: RequestT


@dataclass(frozen=True, slots=True)
class AcquireReleaseRequired(Generic[GenerationT, RequestT, CapabilityT]):
    """The current generation failed earlier and must be released before acquisition."""

    snapshot: Snapshot[GenerationT, RequestT, CapabilityT]


@dataclass(frozen=True, slots=True)
class AcquireFailed(Generic[GenerationT, RequestT, CapabilityT]):
    """Acquisition or capability projection failed and now requires release."""

    snapshot: Snapshot[GenerationT, RequestT, CapabilityT]


@dataclass(frozen=True, slots=True)
class AcquireCommitted(Generic[GenerationT, RequestT, CapabilityT]):
    snapshot: Snapshot[GenerationT, RequestT, CapabilityT]


AcquireResult: TypeAlias = (
    GenerationMismatch[GenerationT]
    | Busy
    | AcquireExisting[GenerationT, RequestT, CapabilityT]
    | AcquireRequestMismatch[RequestT]
    | AcquireReleaseRequired[GenerationT, RequestT, CapabilityT]
    | AcquireFailed[GenerationT, RequestT, CapabilityT]
    | AcquireCommitted[GenerationT, RequestT, CapabilityT]
)


@dataclass(frozen=True, slots=True)
class ReleaseRequestMismatch(Generic[RequestT]):
    current_request: RequestT


@dataclass(frozen=True, slots=True)
class ReleaseInactive:
    """Current generation has no active or cleanup-pending Request."""


@dataclass(frozen=True, slots=True)
class ReleaseFailed(Generic[GenerationT, RequestT, CapabilityT]):
    """Release cleanup or generation advancement failed and may be retried."""

    snapshot: Snapshot[GenerationT, RequestT, CapabilityT]


@dataclass(frozen=True, slots=True)
class ReleaseDetached(Generic[GenerationT]):
    """Synchronous cleanup completed and Managed returned to Idle."""

    next_generation: GenerationT


ReleaseResult: TypeAlias = (
    GenerationMismatch[GenerationT]
    | Busy
    | ReleaseRequestMismatch[RequestT]
    | ReleaseInactive
    | ReleaseFailed[GenerationT, RequestT, CapabilityT]
    | ReleaseDetached[GenerationT]
)


__all__ = [
    "AcquireCommitted",
    "AcquireExisting",
    "AcquireFailed",
    "AcquireReleaseRequired",
    "AcquireRequestMismatch",
    "AcquireResult",
    "Busy",
    "GenerationMismatch",
    "ReleaseDetached",
    "ReleaseFailed",
    "ReleaseInactive",
    "ReleaseRequestMismatch",
    "ReleaseResult",
]
