"""Physical resource planning, coordination, I/O, and cleanup primitives."""

from _resource.driver import PhysicalAcquisition, ResourceDriver, ResourceSet
from _resource.manager import ResourceAcquisition, ResourceManagement, ResourceManager
from _resource.plan import ResourcePlan
from _resource.pool import (
    GLOBAL_RESOURCE_POOL,
    RequestId,
    RequestInterruption,
    ResourceLease,
    ResourcePool,
    ResourceRecord,
    ResourceRequest,
    ResourceRequestRecord,
    ResourceReservation,
    RetiredResource,
)
from _resource.requirement import ResourcePolicy, ResourceRequirement


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
    "ResourcePlan",
    "ResourcePolicy",
    "ResourcePool",
    "ResourceRecord",
    "ResourceRequest",
    "ResourceRequestRecord",
    "ResourceRequirement",
    "ResourceReservation",
    "ResourceSet",
    "RetiredResource",
]
