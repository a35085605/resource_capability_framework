from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _managed.snapshot import Snapshot


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class GenerationMismatch(Generic[GenerationT]):
    current_generation: GenerationT


@dataclass(frozen=True, slots=True)
class AcquireBusy:
    """The resource pool or pending cleanup prevents acquisition."""


@dataclass(frozen=True, slots=True)
class AcquireExisting(Generic[GenerationT, RequestT, CapabilityT]):
    snapshot: Snapshot[GenerationT, RequestT, CapabilityT]


@dataclass(frozen=True, slots=True)
class AcquireRequestMismatch(Generic[RequestT]):
    current_request: RequestT


@dataclass(frozen=True, slots=True)
class AcquireCommitted(Generic[GenerationT, RequestT, CapabilityT]):
    snapshot: Snapshot[GenerationT, RequestT, CapabilityT]


AcquireResult: TypeAlias = (
    GenerationMismatch[GenerationT]
    | AcquireBusy
    | AcquireExisting[GenerationT, RequestT, CapabilityT]
    | AcquireRequestMismatch[RequestT]
    | AcquireCommitted[GenerationT, RequestT, CapabilityT]
)


@dataclass(frozen=True, slots=True)
class ReleaseRequestMismatch(Generic[RequestT]):
    current_request: RequestT


@dataclass(frozen=True, slots=True)
class ReleaseInactive:
    """Current generation has no active or cleanup-pending Request."""


@dataclass(frozen=True, slots=True)
class ReleaseDetached(Generic[GenerationT]):
    """Synchronous cleanup completed and Managed returned to Idle."""

    next_generation: GenerationT


ReleaseResult: TypeAlias = (
    GenerationMismatch[GenerationT]
    | ReleaseRequestMismatch[RequestT]
    | ReleaseInactive
    | ReleaseDetached[GenerationT]
)


__all__ = [
    "AcquireRequestMismatch",
    "AcquireBusy",
    "AcquireCommitted",
    "AcquireExisting",
    "AcquireResult",
    "GenerationMismatch",
    "ReleaseRequestMismatch",
    "ReleaseDetached",
    "ReleaseInactive",
    "ReleaseResult",
]
