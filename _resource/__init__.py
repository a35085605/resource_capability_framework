"""Physical resource coordination, I/O, and cleanup primitives."""

from _attempt import AttemptId
from _resource.claim import ResourceClaim, ResourceClaims
from _resource.cleanup import CleanupScheduler, DaemonThreadCleanupScheduler
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
    "CleanupScheduler",
    "ResourceClaim",
    "ResourceClaims",
    "DaemonThreadCleanupScheduler",
    "GLOBAL_RESOURCE_POOL",
    "GLOBAL_RESOURCE_RESERVATION_TABLE",
    "PhysicalAcquireOutcome",
    "PhysicalAcquired",
    "PhysicalAcquisition",
    "PhysicalFailed",
    "PhysicalInterrupted",
    "PhysicalResourceSet",
    "ResourceAcquisitionCancelled",
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
