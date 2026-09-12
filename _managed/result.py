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
    """The coordinator or Resource pool is already preparing/retaining resources."""


@dataclass(frozen=True, slots=True)
class AcquireExisting(Generic[GenerationT, RequestT, CapabilityT]):
    snapshot: Snapshot[GenerationT, RequestT, CapabilityT]


@dataclass(frozen=True, slots=True)
class AcquireRequestMismatch(Generic[RequestT]):
    current_request: RequestT


@dataclass(frozen=True, slots=True)
class AcquireCommitted(Generic[GenerationT, RequestT, CapabilityT]):
    snapshot: Snapshot[GenerationT, RequestT, CapabilityT]


@dataclass(frozen=True, slots=True)
class AcquireSuperseded(Generic[GenerationT]):
    current_generation: GenerationT


AcquireResult: TypeAlias = (
    GenerationMismatch[GenerationT]
    | AcquireBusy
    | AcquireExisting[GenerationT, RequestT, CapabilityT]
    | AcquireRequestMismatch[RequestT]
    | AcquireCommitted[GenerationT, RequestT, CapabilityT]
    | AcquireSuperseded[GenerationT]
)


@dataclass(frozen=True, slots=True)
class ReleaseRequestMismatch(Generic[RequestT]):
    current_request: RequestT


@dataclass(frozen=True, slots=True)
class ReleaseInactive:
    """Current generation has no active Request to detach."""


@dataclass(frozen=True, slots=True)
class ReleaseAcquisitionRevoked(Generic[GenerationT]):
    """An in-flight Managed attempt lost commit authority and may be draining."""

    next_generation: GenerationT


@dataclass(frozen=True, slots=True)
class ReleaseDetached(Generic[GenerationT]):
    next_generation: GenerationT


ReleaseResult: TypeAlias = (
    GenerationMismatch[GenerationT]
    | ReleaseRequestMismatch[RequestT]
    | ReleaseInactive
    | ReleaseAcquisitionRevoked[GenerationT]
    | ReleaseDetached[GenerationT]
)


__all__ = [
    "AcquireRequestMismatch",
    "AcquireBusy",
    "AcquireCommitted",
    "AcquireExisting",
    "AcquireResult",
    "AcquireSuperseded",
    "GenerationMismatch",
    "ReleaseRequestMismatch",
    "ReleaseAcquisitionRevoked",
    "ReleaseDetached",
    "ReleaseInactive",
    "ReleaseResult",
]
