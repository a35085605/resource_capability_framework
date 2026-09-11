from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _attempt import AttemptToken


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
    attempt: AttemptToken


@dataclass(frozen=True, slots=True)
class Current(Generic[GenerationT, AccessT, CapabilityT]):
    generation: GenerationT
    access: AccessT
    capability: CapabilityT
    attempt: AttemptToken


ManagedState: TypeAlias = (
    Idle[GenerationT]
    | Preparing[GenerationT, AccessT]
    | Current[GenerationT, AccessT, CapabilityT]
)


__all__ = ["Current", "Idle", "ManagedState", "Preparing"]
