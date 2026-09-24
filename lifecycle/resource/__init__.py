"""Contracts and result models for synchronous physical-resource management."""

from lifecycle.resource.cleanup import cleanup_reverse
from lifecycle.resource.contract import ResourceProvider, ResourceRequirementsResolver
from lifecycle.resource.driver import (
    RequirementAcquireFailed,
    RequirementAcquireInterrupted,
    RequirementAcquireResult,
    RequirementAcquireSucceeded,
    PhysicalResources,
    ResourceDriver,
)
from lifecycle.resource.provider import ResolvedResourceProvider
from lifecycle.resource.result import (
    ResourceAcquireFailed,
    ResourceAcquireInterrupted,
    ResourceAcquireResult,
    ResourceAcquireSucceeded,
    ResourceCleanupResult,
    ResourceCleanupStatus,
)


__all__ = [
    "cleanup_reverse",
    "RequirementAcquireFailed",
    "RequirementAcquireInterrupted",
    "RequirementAcquireResult",
    "RequirementAcquireSucceeded",
    "PhysicalResources",
    "ResourceAcquireFailed",
    "ResourceAcquireInterrupted",
    "ResourceAcquireResult",
    "ResourceAcquireSucceeded",
    "ResourceCleanupResult",
    "ResourceCleanupStatus",
    "ResolvedResourceProvider",
    "ResourceDriver",
    "ResourceProvider",
    "ResourceRequirementsResolver",
]
