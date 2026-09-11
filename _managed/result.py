from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from adb._managed.snapshot import Snapshot


GenerationT = TypeVar("GenerationT")
AccessT = TypeVar("AccessT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class GenerationMismatch(Generic[GenerationT]):
    current_generation: GenerationT


@dataclass(frozen=True, slots=True)
class AcquireBusy:
    """The coordinator or Access-keyed pool is already preparing/retaining resources."""


@dataclass(frozen=True, slots=True)
class AcquireExisting(Generic[GenerationT, AccessT, CapabilityT]):
    snapshot: Snapshot[GenerationT, AccessT, CapabilityT]


@dataclass(frozen=True, slots=True)
class AcquireAccessMismatch(Generic[AccessT]):
    current_access: AccessT


@dataclass(frozen=True, slots=True)
class AcquireCommitted(Generic[GenerationT, AccessT, CapabilityT]):
    snapshot: Snapshot[GenerationT, AccessT, CapabilityT]


@dataclass(frozen=True, slots=True)
class AcquireSuperseded(Generic[GenerationT]):
    current_generation: GenerationT


AcquireResult: TypeAlias = (
    GenerationMismatch[GenerationT]
    | AcquireBusy
    | AcquireExisting[GenerationT, AccessT, CapabilityT]
    | AcquireAccessMismatch[AccessT]
    | AcquireCommitted[GenerationT, AccessT, CapabilityT]
    | AcquireSuperseded[GenerationT]
)


@dataclass(frozen=True, slots=True)
class ReleaseAccessMismatch(Generic[AccessT]):
    current_access: AccessT


@dataclass(frozen=True, slots=True)
class ReleaseInactive:
    """Current generation has no Access authority to detach."""


@dataclass(frozen=True, slots=True)
class ReleaseAcquisitionRevoked(Generic[GenerationT]):
    """An in-flight Managed attempt lost commit authority and may be draining."""

    next_generation: GenerationT


@dataclass(frozen=True, slots=True)
class ReleaseDetached(Generic[GenerationT]):
    next_generation: GenerationT


ReleaseResult: TypeAlias = (
    GenerationMismatch[GenerationT]
    | ReleaseAccessMismatch[AccessT]
    | ReleaseInactive
    | ReleaseAcquisitionRevoked[GenerationT]
    | ReleaseDetached[GenerationT]
)


__all__ = [
    "AcquireAccessMismatch",
    "AcquireBusy",
    "AcquireCommitted",
    "AcquireExisting",
    "AcquireResult",
    "AcquireSuperseded",
    "GenerationMismatch",
    "ReleaseAccessMismatch",
    "ReleaseAcquisitionRevoked",
    "ReleaseDetached",
    "ReleaseInactive",
    "ReleaseResult",
]
