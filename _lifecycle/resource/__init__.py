"""Contracts and result models for synchronous physical-resource management."""

from _lifecycle.resource.contract import ResourceProvider, ResourceRequirementsResolver
from _lifecycle.resource.driver import (
    RequirementAcquireFailed,
    RequirementAcquireResult,
    RequirementAcquireSucceeded,
    PhysicalResources,
    ResourceDriver,
)
from _lifecycle.resource.result import (
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
