from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _attempt import AttemptId


GenerationT = TypeVar("GenerationT")
AccessT = TypeVar("AccessT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class Idle(Generic[GenerationT]):
    generation: GenerationT


@dataclass(frozen=True, slots=True)
class Preparing(Generic[GenerationT, AccessT]):
    generation: GenerationT
    access: AccessT
    attempt_id: AttemptId


@dataclass(frozen=True, slots=True)
class Current(Generic[GenerationT, AccessT, CapabilityT]):
    generation: GenerationT
    access: AccessT
    capability: CapabilityT
    attempt_id: AttemptId


ManagedState: TypeAlias = (
    Idle[GenerationT]
    | Preparing[GenerationT, AccessT]
    | Current[GenerationT, AccessT, CapabilityT]
)


__all__ = ["Current", "Idle", "ManagedState", "Preparing"]
