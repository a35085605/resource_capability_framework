from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Hashable, TypeVar

from _access import AccessIdentity
from _capability.projection import CapabilityProjection
from _resource.manager import ResourceManagement


AccessT = TypeVar("AccessT")
AccessKeyT = TypeVar("AccessKeyT", bound=Hashable)
ResourceT = TypeVar("ResourceT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class Adapter(Generic[AccessT, AccessKeyT, ResourceT, CapabilityT]):
    """Managed-facing dependencies with resource semantics kept below the boundary."""

    access_identity: AccessIdentity[AccessT, AccessKeyT]
    resource_manager: ResourceManagement[AccessT, ResourceT]
    capability_projection: CapabilityProjection[AccessT, ResourceT, CapabilityT]


__all__ = ["AccessIdentity", "Adapter"]
