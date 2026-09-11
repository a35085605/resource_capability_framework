from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Generic, Hashable, Protocol, TypeVar

from _capability.projection import CapabilityProjection
from _resource.manager import ResourceManagement
from _resource.policy import ResourcePolicy


AccessT = TypeVar("AccessT")
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class AccessResources(Generic[SpecT]):
    """Resource mapping for one logical Access identity."""

    key: Hashable
    resources: Mapping[SpecT, ResourcePolicy]

    def __post_init__(self) -> None:
        if self.key is None:
            raise TypeError("access resource key cannot be None")
        try:
            hash(self.key)
        except TypeError as exc:
            raise TypeError("access resource key must be hashable") from exc
        if not isinstance(self.resources, Mapping):
            raise TypeError("access resources must be a Mapping")

        resources = dict(self.resources)
        for spec, policy in resources.items():
            if spec is None:
                raise TypeError("resource spec cannot be None")
            if not isinstance(policy, ResourcePolicy):
                raise TypeError("resource policy must be ResourcePolicy")

        object.__setattr__(self, "resources", MappingProxyType(resources))


class AccessModel(Protocol[AccessT, SpecT]):
    """Map managed Access to its logical key and required physical Resources."""

    def resources(self, access: AccessT) -> AccessResources[SpecT]: ...


@dataclass(frozen=True, slots=True)
class Adapter(Generic[AccessT, SpecT, ResourceT, CapabilityT]):
    access_model: AccessModel[AccessT, SpecT]
    resource_manager: ResourceManagement[SpecT, ResourceT]
    capability_projection: CapabilityProjection[AccessT, ResourceT, CapabilityT]


__all__ = ["AccessModel", "AccessResources", "Adapter"]
