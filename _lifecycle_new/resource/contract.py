from __future__ import annotations

from typing import Protocol, TypeVar

from _lifecycle_new.resource.driver import PhysicalResources
from _lifecycle_new.resource.result import ResourceAcquireResult


RequestT = TypeVar("RequestT")
RequirementT = TypeVar("RequirementT")
PhysicalResourceT = TypeVar("PhysicalResourceT")


class ResourceRequirementsResolver(Protocol[RequestT, RequirementT]):
    """Resolve the ordered physical-resource requirements for one request."""

    def resolve(self, request: RequestT) -> tuple[RequirementT, ...]: ...


class ResourceProvider(Protocol[RequestT, PhysicalResourceT]):
    """Provide request-level physical-resource acquisition and release.

    ``acquire`` must report every acquired resource that still requires cleanup,
    including resources created before a terminal failure. A caller may retain those
    resources and release them later rather than rolling them back immediately.

    ``release`` may be retried with the same resource tuple after raising. Implementations
    must therefore tolerate resources that were already cleaned up by an earlier attempt
    and only return after the supplied resources no longer require cleanup.
    """

    def acquire(self, request: RequestT) -> ResourceAcquireResult[PhysicalResourceT]: ...

    def release(self, resources: PhysicalResources[PhysicalResourceT]) -> None: ...


__all__ = ["ResourceProvider", "ResourceRequirementsResolver"]
