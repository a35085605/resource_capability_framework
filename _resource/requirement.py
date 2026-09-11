from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from _resource.policy import ResourcePolicy


SpecT = TypeVar("SpecT")


@dataclass(frozen=True, slots=True)
class ResourceRequirement(Generic[SpecT]):
    """One implementation-specific requirement for a physical Resource spec."""

    spec: SpecT
    policy: ResourcePolicy

    def __post_init__(self) -> None:
        if self.spec is None:
            raise TypeError("resource spec cannot be None")
        if not isinstance(self.policy, ResourcePolicy):
            raise TypeError("resource policy must be ResourcePolicy")


type ResourceRequirements[T] = tuple[ResourceRequirement[T], ...]


__all__ = ["ResourceRequirement", "ResourceRequirements"]
