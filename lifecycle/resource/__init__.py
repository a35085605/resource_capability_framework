"""Structured synchronous resource ownership and finalization."""

from lifecycle.resource.recovery import RecoveryEntry, ResourceRecoveryPool
from lifecycle.resource.resource import Resource, ResourceHandle
from lifecycle.resource.result import ReleaseReport, ResourceAllocationError


__all__ = [
    "RecoveryEntry",
    "ReleaseReport",
    "Resource",
    "ResourceAllocationError",
    "ResourceHandle",
    "ResourceRecoveryPool",
]
