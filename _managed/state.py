from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _resource.driver import PhysicalResourceSet


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
PhysicalResourceT = TypeVar("PhysicalResourceT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class Idle(Generic[GenerationT]):
    generation: GenerationT


@dataclass(frozen=True, slots=True)
class Acquiring(Generic[GenerationT, RequestT]):
    generation: GenerationT
    request: RequestT


@dataclass(frozen=True, slots=True)
class Current(Generic[GenerationT, RequestT, PhysicalResourceT, CapabilityT]):
    generation: GenerationT
    request: RequestT
    capability: CapabilityT
    resources: PhysicalResourceSet[PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class Releasing(Generic[GenerationT, RequestT, PhysicalResourceT]):
    generation: GenerationT
    request: RequestT
    resources: PhysicalResourceSet[PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class CleanupPending(Generic[GenerationT, RequestT, PhysicalResourceT]):
    """Capability is unavailable and release must be retried for this generation."""

    generation: GenerationT
    request: RequestT
    resources: PhysicalResourceSet[PhysicalResourceT]
    last_error: BaseException


ManagedState: TypeAlias = (
    Idle[GenerationT]
    | Acquiring[GenerationT, RequestT]
    | Current[GenerationT, RequestT, PhysicalResourceT, CapabilityT]
    | Releasing[GenerationT, RequestT, PhysicalResourceT]
    | CleanupPending[GenerationT, RequestT, PhysicalResourceT]
)


__all__ = ["Acquiring", "CleanupPending", "Current", "Idle", "ManagedState", "Releasing"]
