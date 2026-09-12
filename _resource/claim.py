from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from _resource.key import ResourceKey
from _resource.requirement import ResourceRequirement


RequirementT = TypeVar("RequirementT", bound=ResourceRequirement)


@dataclass(frozen=True, slots=True)
class ResourceClaim(Generic[RequirementT]):
    """One resource requirement paired with its canonical logical key."""

    requirement: RequirementT
    key: ResourceKey


type ResourceClaims[T: ResourceRequirement] = tuple[ResourceClaim[T], ...]


__all__ = ["ResourceClaim", "ResourceClaims"]
