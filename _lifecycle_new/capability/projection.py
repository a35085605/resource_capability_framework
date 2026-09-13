from __future__ import annotations

from typing import Protocol, TypeVar

from _lifecycle_new.resource.driver import PhysicalResources


RequestT = TypeVar("RequestT", contravariant=True)
PhysicalResourceT = TypeVar("PhysicalResourceT", contravariant=True)
CapabilityT = TypeVar("CapabilityT", covariant=True)


class CapabilityProjector(Protocol[RequestT, PhysicalResourceT, CapabilityT]):
    """Project a capability from a request and acquired physical resources.

    Implementations are pure and stateless: ``project`` derives the capability only
    from its inputs and does not perform physical resource I/O.
    """

    def project(
        self,
        request: RequestT,
        resources: PhysicalResources[PhysicalResourceT],
    ) -> CapabilityT: ...


__all__ = ["CapabilityProjector"]
