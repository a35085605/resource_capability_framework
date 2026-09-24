from __future__ import annotations

from typing import Protocol, TypeVar

from lifecycle.resource.driver import PhysicalResources
from lifecycle.resource.result import ResourceAcquireResult, ResourceCleanupResult


RequestT = TypeVar("RequestT")
RequirementT = TypeVar("RequirementT")
PhysicalResourceT = TypeVar("PhysicalResourceT")


class ResourceRequirementsResolver(Protocol[RequestT, RequirementT]):
    """Resolve the ordered physical-resource requirements for one request."""

    def resolve(self, request: RequestT) -> tuple[RequirementT, ...]: ...


class ResourceProvider(Protocol[RequestT, PhysicalResourceT]):
    """Provide request-level physical-resource acquisition and cleanup.

    Acquisition transfers every reported resource to the caller. Cleanup returns the
    exact remaining ownership rather than requiring callers to infer it from exceptions.
    """

    def acquire(self, request: RequestT) -> ResourceAcquireResult[PhysicalResourceT]: ...

    def cleanup(
        self,
        resources: PhysicalResources[PhysicalResourceT],
    ) -> ResourceCleanupResult[PhysicalResourceT]: ...


__all__ = ["ResourceProvider", "ResourceRequirementsResolver"]
