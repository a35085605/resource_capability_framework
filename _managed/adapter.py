from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Hashable, Protocol, TypeVar

from _capability.projection import CapabilityProjection
from _resource.manager import ResourceManagement
from _resource.requirement import ResourceRequirement, ResourceRequirements


AccessT = TypeVar("AccessT")
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class AccessPlan(Generic[SpecT]):
    """Implementation-specific resource semantics for one logical Access identity."""

    key: Hashable
    requirements: ResourceRequirements[SpecT]

    def __post_init__(self) -> None:
        if self.key is None:
            raise TypeError("access plan key cannot be None")
        try:
            hash(self.key)
        except TypeError as exc:
            raise TypeError("access plan key must be hashable") from exc
        if not isinstance(self.requirements, tuple):
            raise TypeError("access plan requirements must be a tuple")

        seen_specs: list[SpecT] = []
        for requirement in self.requirements:
            if not isinstance(requirement, ResourceRequirement):
                raise TypeError(
                    "access plan requirements must contain ResourceRequirement values"
                )
            if any(existing == requirement.spec for existing in seen_specs):
                raise ValueError("access plan cannot contain duplicate resource specs")
            seen_specs.append(requirement.spec)


class AccessModel(Protocol[AccessT, SpecT]):
    """Project an Access through this implementation's resource semantics."""

    def plan(self, access: AccessT) -> AccessPlan[SpecT]: ...


@dataclass(frozen=True, slots=True)
class Adapter(Generic[AccessT, SpecT, ResourceT, CapabilityT]):
    access_model: AccessModel[AccessT, SpecT]
    resource_manager: ResourceManagement[SpecT, ResourceT]
    capability_projection: CapabilityProjection[AccessT, ResourceT, CapabilityT]


__all__ = ["AccessModel", "AccessPlan", "Adapter", "ResourceRequirement"]
