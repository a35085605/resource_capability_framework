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
class Preparing(Generic[GenerationT, RequestT, PhysicalResourceT]):
    generation: GenerationT
    request: RequestT
    attempt: ResourceAttempt[RequestT, PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class Current(Generic[GenerationT, RequestT, PhysicalResourceT, CapabilityT]):
    generation: GenerationT
    request: RequestT
    capability: CapabilityT
    attempt: ResourceAttempt[RequestT, PhysicalResourceT]


ManagedState: TypeAlias = (
    Idle[GenerationT]
    | Preparing[GenerationT, RequestT, PhysicalResourceT]
    | Current[GenerationT, RequestT, PhysicalResourceT, CapabilityT]
)


__all__ = ["Current", "Idle", "ManagedState", "Preparing"]
