"""Contracts and result models for synchronous physical-resource management."""

from _resource.contract import ResourceProvider, ResourceRequirementsResolver
from _resource.driver import (
    PhysicalAcquireFailed,
    PhysicalAcquireResult,
    PhysicalAcquireSucceeded,
    PhysicalResources,
    ResourceDriver,
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
    "ResourceProvider",
    "ResourceRequirementsResolver",
]
