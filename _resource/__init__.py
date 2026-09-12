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
from _resource.key import ResourceKey, ResourceKeyModel
from _resource.manager import (
    ResourceCleanupPendingError,
    ResourceAttempt,
    ResourceManagement,
    ResourceManager,
    ResourceRequirementsModel,
)
from _resource.policy import ResourcePolicy
from _resource.pool import (
    GLOBAL_RESOURCE_RESERVATION_TABLE,
    ResourceReservationRecord,
    ResourceReservationTable,
)
from _resource.requirement import ResourceRequirement, ResourceRequirements
from _resource.result import ResourceAcquireResult, ResourceBlocked, ResourceReady


__all__ = [
    "AttemptId",
    "ResourceClaim",
    "ResourceClaims",
    "GLOBAL_RESOURCE_RESERVATION_TABLE",
    "PhysicalAcquireOutcome",
    "PhysicalAcquired",
    "PhysicalFailed",
    "PhysicalResourceSet",
    "ResourceCleanupPendingError",
    "ResourceAttempt",
    "ResourceKey",
    "ResourceKeyModel",
    "ResourceAcquireResult",
    "ResourceBlocked",
    "ResourceDriver",
    "ResourceManagement",
    "ResourceManager",
    "ResourceReservationRecord",
    "ResourceReservationTable",
    "ResourceRequirement",
    "ResourceRequirements",
    "ResourceRequirementsModel",
    "ResourceReady",
    "ResourcePolicy",
]
