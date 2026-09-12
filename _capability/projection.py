from __future__ import annotations

from typing import Protocol, TypeVar

from _resource.driver import ResourceSet


RequestT = TypeVar("RequestT", contravariant=True)
ResourceT = TypeVar("ResourceT", contravariant=True)
CapabilityT = TypeVar("CapabilityT", covariant=True)


class CapabilityProjection(Protocol[RequestT, ResourceT, CapabilityT]):
    """Pure, stateless projection from Request + ResourceSet to Capability."""

    def project(
        self,
        request: RequestT,
        resources: ResourceSet[ResourceT],
    ) -> CapabilityT: ...


__all__ = ["CapabilityProjection"]
