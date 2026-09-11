"""Physical resource planning, coordination, I/O, and cleanup primitives."""

from adb._resource.driver import AcquisitionContext, ResourceDriver, ResourceSet
from adb._resource.manager import ResourceAcquisition, ResourceManagement, ResourceManager
from adb._resource.plan import ResourcePlan
from adb._resource.pool import (
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
from adb._resource.requirement import ResourcePolicy, ResourceRequirement


__all__ = [
    "AcquisitionContext",
    "GLOBAL_RESOURCE_POOL",
    "RequestId",
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
