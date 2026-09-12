from __future__ import annotations

from typing import Protocol, TypeVar

from _resource.driver import PhysicalResourceSet


RequestT = TypeVar("RequestT", contravariant=True)
PhysicalResourceT = TypeVar("PhysicalResourceT", contravariant=True)
CapabilityT = TypeVar("CapabilityT", covariant=True)


class CapabilityProjection(Protocol[RequestT, PhysicalResourceT, CapabilityT]):
    """Pure, stateless projection from Request + PhysicalResourceSet to Capability."""

    def project(
        self,
        request: RequestT,
        resources: PhysicalResourceSet[PhysicalResourceT],
    ) -> CapabilityT: ...


__all__ = ["CapabilityProjection"]
