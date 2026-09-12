"""Physical resource coordination, I/O, and cleanup primitives."""

from _attempt import AttemptId
from _resource.driver import (
    PhysicalAcquireOutcome,
    PhysicalAcquired,
    PhysicalAcquisition,
    PhysicalFailed,
    PhysicalInterrupted,
    ResourceDriver,
    PhysicalResourceSet,
)
from _resource.key import ResourceKey, ResourceKeyModel, ResourceKeys
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
    PhysicalResourceRecord,
    ResourceRequestRecord,
    RetiredPhysicalResource,
)
from _resource.requirement import ResourceRequirement, ResourceRequirements
from _resource.result import (
    ResourceAcquireResult,
    ResourceAcquired,
    ResourceBlocked,
    ResourceFailed,
)


__all__ = [
    "AttemptId",
    "AttemptRelease",
    "GLOBAL_RESOURCE_POOL",
    "PhysicalAcquireOutcome",
    "PhysicalAcquired",
    "PhysicalAcquisition",
    "PhysicalFailed",
    "PhysicalInterrupted",
    "PhysicalResourceRecord",
    "PhysicalResourceSet",
    "ResourceAcquisitionCancelled",
    "ResourceKey",
    "ResourceKeyModel",
    "ResourceKeys",
    "ResourceAcquireResult",
    "ResourceAcquired",
    "ResourceBlocked",
    "ResourceDriver",
    "ResourceFailed",
    "ResourceManagement",
    "ResourceManager",
    "ResourcePool",
    "ResourceRequestRecord",
    "ResourceRequirement",
    "ResourceRequirements",
    "ResourceRequirementsModel",
    "ResourcePolicy",
    "RetiredPhysicalResource",
]
