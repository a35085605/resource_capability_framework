"""Managed Request and capability coordination primitives."""

from _attempt import AttemptId
from _managed.coordinator import ManagedCoordinator
from _managed.result import (
    AcquireRequestMismatch,
    AcquireBusy,
    AcquireCommitted,
    AcquireExisting,
    AcquireResult,
    GenerationMismatch,
    ReleaseRequestMismatch,
    ReleaseDetached,
    ReleaseInactive,
    ReleaseResult,
)
from _managed.snapshot import Snapshot
from _managed.state import CleanupPending, Current, Idle, ManagedState
from _resource.manager import ResourceCleanupPendingError


__all__ = [
    "AcquireRequestMismatch",
    "AcquireBusy",
    "AcquireCommitted",
    "AcquireExisting",
    "AcquireResult",
    "AttemptId",
    "CleanupPending",
    "Current",
    "GenerationMismatch",
    "Idle",
    "ManagedCoordinator",
    "ManagedState",
    "ReleaseRequestMismatch",
    "ReleaseDetached",
    "ReleaseInactive",
    "ReleaseResult",
    "ResourceCleanupPendingError",
    "Snapshot",
]
