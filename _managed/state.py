from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _resource.manager import ResourceAttempt


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
    attempt: ResourceAttempt[RequestT, PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class Releasing(Generic[GenerationT, RequestT, PhysicalResourceT]):
    generation: GenerationT
    request: RequestT
    attempt: ResourceAttempt[RequestT, PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class CleanupPending(Generic[GenerationT, RequestT, PhysicalResourceT]):
    """Capability is unavailable and the failed release/rollback awaits retry."""

    generation: GenerationT
    request: RequestT
    attempt: ResourceAttempt[RequestT, PhysicalResourceT]
    last_error: BaseException


ManagedState: TypeAlias = (
    Idle[GenerationT]
    | Acquiring[GenerationT, RequestT]
    | Current[GenerationT, RequestT, PhysicalResourceT, CapabilityT]
    | Releasing[GenerationT, RequestT, PhysicalResourceT]
    | CleanupPending[GenerationT, RequestT, PhysicalResourceT]
)


__all__ = ["Acquiring", "CleanupPending", "Current", "Idle", "ManagedState", "Releasing"]
