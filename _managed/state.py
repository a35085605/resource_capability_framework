from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Hashable, TypeAlias, TypeVar

from _attempt import AttemptToken


GenerationT = TypeVar("GenerationT")
AccessT = TypeVar("AccessT")
AccessKeyT = TypeVar("AccessKeyT", bound=Hashable)
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class Idle(Generic[GenerationT]):
    generation: GenerationT


@dataclass(frozen=True, slots=True)
class Preparing(Generic[GenerationT, AccessT, AccessKeyT]):
    generation: GenerationT
    access: AccessT
    access_key: AccessKeyT
    attempt: AttemptToken


@dataclass(frozen=True, slots=True)
class Current(Generic[GenerationT, AccessT, AccessKeyT, CapabilityT]):
    generation: GenerationT
    access: AccessT
    access_key: AccessKeyT
    capability: CapabilityT
    attempt: AttemptToken


ManagedState: TypeAlias = (
    Idle[GenerationT]
    | Preparing[GenerationT, AccessT, AccessKeyT]
    | Current[GenerationT, AccessT, AccessKeyT, CapabilityT]
)


__all__ = ["Current", "Idle", "ManagedState", "Preparing"]
