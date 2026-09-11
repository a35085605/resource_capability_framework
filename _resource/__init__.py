"""Physical resource coordination, I/O, and cleanup primitives."""

from _resource.driver import PhysicalAcquisition, ResourceDriver, ResourceSet
from _resource.manager import ResourceAcquisition, ResourceManagement, ResourceManager
from _resource.policy import ResourcePolicy
from _resource.requirement import ResourceRequirement, ResourceRequirements
from _resource.pool import (
    GLOBAL_RESOURCE_POOL,
    RequestId,
    RequestInterruption,
    ResourceLease,
    ResourcePool,
    ResourceRecord,
    ResourceRequest,
    ResourceRequestRecord,
    RetiredResource,
)


__all__ = [
    "GLOBAL_RESOURCE_POOL",
    "RequestId",
    "PhysicalAcquisition",
    "RequestInterruption",
    "ResourceAcquisition",
    "ResourceDriver",
    "ResourceLease",
    "ResourceManagement",
    "ResourceManager",
    "ResourceRequirement",
    "ResourceRequirements",
    "ResourcePolicy",
    "ResourcePool",
    "ResourceRecord",
    "ResourceRequest",
    "ResourceRequestRecord",
    "ResourceSet",
    "RetiredResource",
]
