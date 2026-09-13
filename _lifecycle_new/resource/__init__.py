"""Contracts and result models for synchronous physical-resource management."""

from _lifecycle_new.resource.contract import ResourceProvider, ResourceRequirementsResolver
from _lifecycle_new.resource.driver import (
    RequirementAcquireFailed,
    RequirementAcquireResult,
    RequirementAcquireSucceeded,
    PhysicalResources,
    ResourceDriver,
)
from _lifecycle_new.resource.result import (
    ResourceAcquireFailed,
    ResourceAcquireResult,
    ResourceAcquireSucceeded,
)


__all__ = [
    "RequirementAcquireFailed",
    "RequirementAcquireResult",
    "RequirementAcquireSucceeded",
    "PhysicalResources",
    "ResourceAcquireFailed",
    "ResourceAcquireResult",
    "ResourceAcquireSucceeded",
    "ResourceDriver",
    "ResourceProvider",
    "ResourceRequirementsResolver",
]
