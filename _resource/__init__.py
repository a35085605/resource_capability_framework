"""Synchronous physical resource I/O primitives."""

from _resource.driver import (
    PhysicalAcquireOutcome,
    PhysicalAcquired,
    PhysicalFailed,
    ResourceDriver,
    PhysicalResourceSet,
)
from _resource.manager import (
    ResourceManagement,
    ResourceManager,
    ResourceRequirementsModel,
)
from _resource.requirement import ResourceRequirement, ResourceRequirements
from _resource.result import ResourceAcquireResult, ResourceFailed, ResourceReady


__all__ = [
    "PhysicalAcquireOutcome",
    "PhysicalAcquired",
    "PhysicalFailed",
    "PhysicalResourceSet",
    "ResourceAcquireResult",
    "ResourceDriver",
    "ResourceFailed",
    "ResourceManagement",
    "ResourceManager",
    "ResourceRequirement",
    "ResourceRequirements",
    "ResourceRequirementsModel",
    "ResourceReady",
]
