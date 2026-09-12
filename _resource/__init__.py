"""Physical resource coordination, I/O, and cleanup primitives."""

from _attempt import AttemptId
from _resource.claim import ResourceClaim, ResourceClaims
from _resource.driver import (
    PhysicalAcquireOutcome,
    PhysicalAcquired,
    PhysicalFailed,
    ResourceDriver,
    PhysicalResourceSet,
)
from _resource.key import ResourceKey, ResourceKeyModel, ResourceKeys
from _resource.manager import (
    ResourceAttempt,
    ResourceManagement,
    ResourceManager,
    ResourceRequirementsModel,
)
from _resource.policy import ResourcePolicy
from _resource.pool import (
    GLOBAL_RESOURCE_POOL,
    GLOBAL_RESOURCE_RESERVATION_TABLE,
    ResourcePool,
    ResourceReservationRecord,
    ResourceReservationTable,
)
from _resource.requirement import ResourceRequirement, ResourceRequirements
from _resource.result import (
    ResourceAcquireResult,
    ResourceBlocked,
    ResourceFailed,
    ResourceReady,
)


__all__ = [
    "AttemptId",
    "ResourceClaim",
    "ResourceClaims",
    "GLOBAL_RESOURCE_POOL",
    "GLOBAL_RESOURCE_RESERVATION_TABLE",
    "PhysicalAcquireOutcome",
    "PhysicalAcquired",
    "PhysicalFailed",
    "PhysicalResourceSet",
    "ResourceAttempt",
    "ResourceKey",
    "ResourceKeyModel",
    "ResourceKeys",
    "ResourceAcquireResult",
    "ResourceBlocked",
    "ResourceDriver",
    "ResourceFailed",
    "ResourceManagement",
    "ResourceManager",
    "ResourcePool",
    "ResourceReservationRecord",
    "ResourceReservationTable",
    "ResourceRequirement",
    "ResourceRequirements",
    "ResourceRequirementsModel",
    "ResourceReady",
    "ResourcePolicy",
]
