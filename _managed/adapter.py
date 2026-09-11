from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

from adb._capability.projection import CapabilityProjection
from adb._resource.manager import ResourceManagement
from adb._resource.plan import ResourcePlan


AccessT = TypeVar("AccessT")
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")
CapabilityT = TypeVar("CapabilityT")


class AccessModel(Protocol[AccessT, SpecT]):
    """Translate managed Access into a lower-layer resource plan."""

    def resource_plan(self, access: AccessT) -> ResourcePlan[SpecT]: ...


@dataclass(frozen=True, slots=True)
class Adapter(Generic[AccessT, SpecT, ResourceT, CapabilityT]):
    access_model: AccessModel[AccessT, SpecT]
    resource_manager: ResourceManagement[SpecT, ResourceT]
    capability_projection: CapabilityProjection[AccessT, ResourceT, CapabilityT]


__all__ = ["AccessModel", "Adapter"]
