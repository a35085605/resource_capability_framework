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
from _managed.snapshot import ManagedPhase, Snapshot
from _managed.state import (
    Acquiring,
    CleanupPending,
    Current,
    Idle,
    ManagedState,
    Releasing,
)
from _resource.manager import ResourceCleanupPendingError


__all__ = [
    "AcquireRequestMismatch",
    "AcquireBusy",
    "AcquireCommitted",
    "AcquireExisting",
    "AcquireResult",
    "Acquiring",
    "AttemptId",
    "CleanupPending",
    "Current",
    "GenerationMismatch",
    "Idle",
    "ManagedCoordinator",
    "ManagedPhase",
    "ManagedState",
    "ReleaseRequestMismatch",
    "ReleaseDetached",
    "ReleaseInactive",
    "ReleaseResult",
    "Releasing",
    "ResourceCleanupPendingError",
    "Snapshot",
]
