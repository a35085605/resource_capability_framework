"""Synchronous physical-resource acquisition and release primitives."""

from _resource.driver import (
    PhysicalAcquireFailed,
    PhysicalAcquireResult,
    PhysicalAcquireSucceeded,
    PhysicalResources,
    ResourceDriver,
)
from _resource.manager import (
    ResourceManager,
    ResourceProvider,
    ResourceRequirementsResolver,
)
from _resource.result import (
    ResourceAcquireFailed,
    ResourceAcquireResult,
    ResourceAcquireSucceeded,
)


__all__ = [
    "PhysicalAcquireFailed",
    "PhysicalAcquireResult",
    "PhysicalAcquireSucceeded",
    "PhysicalResources",
    "ResourceAcquireFailed",
    "ResourceAcquireResult",
    "ResourceAcquireSucceeded",
    "ResourceDriver",
    "ResourceManager",
    "ResourceProvider",
    "ResourceRequirementsResolver",
]
