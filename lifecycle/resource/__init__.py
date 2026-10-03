"""Structured resource ownership and finalization described with synchronous IO."""

from lifecycle.resource.recovery import RecoveryEntry, ResourceRecoveryPool
from lifecycle.resource.resource import Resource, ResourceScope
from lifecycle.resource.result import ReleaseReport, ResourceAllocationError, ScopeClosedError


__all__ = [
    "RecoveryEntry",
    "ReleaseReport",
    "Resource",
    "ResourceAllocationError",
    "ResourceRecoveryPool",
    "ResourceScope",
    "ScopeClosedError",
]
