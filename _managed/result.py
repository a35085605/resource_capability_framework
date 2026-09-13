from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _managed.snapshot import ManagedPhase, ManagedSnapshot


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class GenerationMismatch(Generic[GenerationT]):
    """Report that the expected generation is stale."""

    current_generation: GenerationT


@dataclass(frozen=True, slots=True)
class Busy:
    """Report that another acquire or release operation is still in progress.

    ACQUIRING covers resource acquisition and capability projection. RELEASING covers
    physical cleanup and next-generation issuance.
    """

    phase: ManagedPhase

    def __post_init__(self) -> None:
        if self.phase not in (ManagedPhase.ACQUIRING, ManagedPhase.RELEASING):
            raise ValueError("Busy phase must be ACQUIRING or RELEASING")


@dataclass(frozen=True, slots=True)
class AcquireExisting(Generic[GenerationT, RequestT, CapabilityT]):
    """Report that the requested lifecycle is already active."""

    snapshot: ManagedSnapshot[GenerationT, RequestT, CapabilityT]


@dataclass(frozen=True, slots=True)
class AcquireRequestMismatch(Generic[RequestT]):
    """Report that another request is active for the expected generation."""

    current_request: RequestT


@dataclass(frozen=True, slots=True)
class AcquireReleaseRequired(Generic[GenerationT, RequestT, CapabilityT]):
    """Report that a prior failure must be completed through release first."""

    snapshot: ManagedSnapshot[GenerationT, RequestT, CapabilityT]


@dataclass(frozen=True, slots=True)
class AcquireFailed(Generic[GenerationT, RequestT, CapabilityT]):
    """Report acquisition or projection failure that now requires release."""

    snapshot: ManagedSnapshot[GenerationT, RequestT, CapabilityT]


@dataclass(frozen=True, slots=True)
class AcquireSucceeded(Generic[GenerationT, RequestT, CapabilityT]):
    """Report successful acquisition of an active capability."""

    snapshot: ManagedSnapshot[GenerationT, RequestT, CapabilityT]


AcquireResult: TypeAlias = (
    GenerationMismatch[GenerationT]
    | Busy
    | AcquireExisting[GenerationT, RequestT, CapabilityT]
    | AcquireRequestMismatch[RequestT]
    | AcquireReleaseRequired[GenerationT, RequestT, CapabilityT]
    | AcquireFailed[GenerationT, RequestT, CapabilityT]
    | AcquireSucceeded[GenerationT, RequestT, CapabilityT]
)


@dataclass(frozen=True, slots=True)
class ReleaseRequestMismatch(Generic[RequestT]):
    """Report that release targeted a different request in the current generation."""

    current_request: RequestT


@dataclass(frozen=True, slots=True)
class ReleaseAlreadyIdle:
    """Report that the current generation has no request requiring release."""


@dataclass(frozen=True, slots=True)
class ReleaseFailed(Generic[GenerationT, RequestT, CapabilityT]):
    """Report release failure that can be retried for the same generation."""

    snapshot: ManagedSnapshot[GenerationT, RequestT, CapabilityT]


@dataclass(frozen=True, slots=True)
class ReleaseSucceeded(Generic[GenerationT]):
    """Report completed release and the newly issued idle generation."""

    next_generation: GenerationT


ReleaseResult: TypeAlias = (
    GenerationMismatch[GenerationT]
    | Busy
    | ReleaseRequestMismatch[RequestT]
    | ReleaseAlreadyIdle
    | ReleaseFailed[GenerationT, RequestT, CapabilityT]
    | ReleaseSucceeded[GenerationT]
)


__all__ = [
    "AcquireExisting",
    "AcquireFailed",
    "AcquireReleaseRequired",
    "AcquireRequestMismatch",
    "AcquireResult",
    "AcquireSucceeded",
    "Busy",
    "GenerationMismatch",
    "ReleaseAlreadyIdle",
    "ReleaseFailed",
    "ReleaseRequestMismatch",
    "ReleaseResult",
    "ReleaseSucceeded",
]
