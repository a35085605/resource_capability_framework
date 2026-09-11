from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Hashable, TypeVar

from adb._resource.requirement import ResourceRequirement


SpecT = TypeVar("SpecT")


@dataclass(frozen=True, slots=True)
class ResourcePlan(Generic[SpecT]):
    """Complete inputs for one resource-management claim.

    ``scope`` defines the conflict/reuse namespace. ``requirement`` defines the
    identity and coexistence policy inside that scope. ``spec`` is the only value
    passed to the physical ResourceDriver.
    """

    scope: Hashable
    requirement: ResourceRequirement
    spec: SpecT

    def __post_init__(self) -> None:
        if self.scope is None:
            raise TypeError("resource plan scope cannot be None")
        try:
            hash(self.scope)
        except TypeError as exc:
            raise TypeError("resource plan scope must be hashable") from exc
        if not isinstance(self.requirement, ResourceRequirement):
            raise TypeError("resource plan requirement must be ResourceRequirement")
        if self.spec is None:
            raise TypeError("resource plan spec cannot be None")


__all__ = ["ResourcePlan"]
