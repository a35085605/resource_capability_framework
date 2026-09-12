from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _attempt import AttemptId


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class Idle(Generic[GenerationT]):
    generation: GenerationT


@dataclass(frozen=True, slots=True)
class Preparing(Generic[GenerationT, RequestT]):
    generation: GenerationT
    request: RequestT
    attempt_id: AttemptId


@dataclass(frozen=True, slots=True)
class Current(Generic[GenerationT, RequestT, CapabilityT]):
    generation: GenerationT
    request: RequestT
    capability: CapabilityT
    attempt_id: AttemptId


ManagedState: TypeAlias = (
    Idle[GenerationT]
    | Preparing[GenerationT, RequestT]
    | Current[GenerationT, RequestT, CapabilityT]
)


__all__ = ["Current", "Idle", "ManagedState", "Preparing"]
