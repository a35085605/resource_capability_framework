from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class Snapshot(Generic[GenerationT, RequestT, CapabilityT]):
    """Point-in-time Managed authority snapshot; it does not lease Capability."""

    generation: GenerationT
    request: RequestT | None = None
    capability: CapabilityT | None = None

    def __post_init__(self) -> None:
        if self.generation is None:
            raise TypeError("generation cannot be None")
        if (self.request is None) != (self.capability is None):
            raise ValueError("request and capability must both be present or both be None")


__all__ = ["Snapshot"]
