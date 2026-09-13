"""Managed Request and capability coordination primitives."""

from _managed.coordinator import ManagedCoordinator
from _managed.result import (
    AcquireCommitted,
    AcquireExisting,
    AcquireFailed,
    AcquireReleaseRequired,
    AcquireRequestMismatch,
    AcquireResult,
    Busy,
    GenerationMismatch,
    ReleaseDetached,
    ReleaseFailed,
    ReleaseInactive,
    ReleaseRequestMismatch,
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


__all__ = [
    "AcquireCommitted",
    "AcquireExisting",
    "AcquireFailed",
    "AcquireReleaseRequired",
    "AcquireRequestMismatch",
    "AcquireResult",
    "Acquiring",
    "Busy",
    "CleanupPending",
    "Current",
    "GenerationMismatch",
    "Idle",
    "ManagedCoordinator",
    "ManagedPhase",
    "ManagedState",
    "ReleaseDetached",
    "ReleaseFailed",
    "ReleaseInactive",
    "ReleaseRequestMismatch",
    "ReleaseResult",
    "Releasing",
    "Snapshot",
]
