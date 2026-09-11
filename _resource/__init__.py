"""Physical resource coordination, I/O, and cleanup primitives."""

from _attempt import AttemptToken
from _resource.driver import PhysicalAcquisition, ResourceDriver, ResourceSet
from _resource.manager import (
    ResourceAcquisitionCancelled,
    ResourceManagement,
    ResourceManager,
    ResourceRequirementsModel,
)
from _resource.policy import ResourcePolicy
from _resource.pool import (
    AttemptRelease,
    GLOBAL_RESOURCE_POOL,
    ResourcePool,
    ResourceRecord,
    ResourceRequestRecord,
    RetiredResource,
)
from _resource.requirement import ResourceRequirement, ResourceRequirements
from _resource.result import (
    ResourceAcquireResult,
    ResourceAcquired,
    ResourceBlocked,
    ResourceFailed,
)


__all__ = [
    "AttemptRelease",
    "AttemptToken",
    "GLOBAL_RESOURCE_POOL",
    "PhysicalAcquisition",
    "ResourceAcquisitionCancelled",
    "ResourceAcquireResult",
    "ResourceAcquired",
    "ResourceBlocked",
    "ResourceDriver",
    "ResourceFailed",
    "ResourceManagement",
    "ResourceManager",
    "ResourcePool",
    "ResourceRecord",
    "ResourceRequestRecord",
    "ResourceRequirement",
    "ResourceRequirements",
    "ResourceRequirementsModel",
    "ResourcePolicy",
    "ResourceSet",
    "RetiredResource",
]
