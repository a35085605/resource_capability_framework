from __future__ import annotations

from typing import Protocol, TypeVar

from adb._resource.driver import ResourceSet


AccessT = TypeVar("AccessT", contravariant=True)
ResourceT = TypeVar("ResourceT", contravariant=True)
CapabilityT = TypeVar("CapabilityT", covariant=True)


class CapabilityProjection(Protocol[AccessT, ResourceT, CapabilityT]):
    """Pure, stateless projection from Access + ResourceSet to Capability."""

    def project(
        self,
        access: AccessT,
        resources: ResourceSet[ResourceT],
    ) -> CapabilityT: ...


__all__ = ["CapabilityProjection"]
