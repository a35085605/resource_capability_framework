"""Managed request and capability lifecycle primitives."""

from _managed.coordinator import ManagedCoordinator
from _managed.result import (
    AcquireExisting,
    AcquireFailed,
    AcquireReleaseRequired,
    AcquireRequestMismatch,
    AcquireResult,
    AcquireSucceeded,
    Busy,
    GenerationMismatch,
    ReleaseAlreadyIdle,
    ReleaseFailed,
    ReleaseRequestMismatch,
    ReleaseResult,
    ReleaseSucceeded,
)
from _managed.snapshot import ManagedPhase, ManagedSnapshot
from _managed.state import (
    Acquiring,
    Active,
    Idle,
    ManagedState,
    ReleasePending,
    Releasing,
)


__all__ = [
    "AcquireExisting",
    "AcquireFailed",
    "AcquireReleaseRequired",
    "AcquireRequestMismatch",
    "AcquireResult",
    "AcquireSucceeded",
    "Acquiring",
    "Active",
    "Busy",
    "GenerationMismatch",
    "Idle",
    "ManagedCoordinator",
    "ManagedPhase",
    "ManagedSnapshot",
    "ManagedState",
    "ReleaseAlreadyIdle",
    "ReleaseFailed",
    "ReleasePending",
    "ReleaseRequestMismatch",
    "ReleaseResult",
    "ReleaseSucceeded",
    "Releasing",
]
