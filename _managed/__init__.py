"""Managed Access authority and capability coordination primitives."""

from _managed.adapter import AccessModel, Adapter
from _managed.coordinator import ManagedCoordinator
from _managed.result import (
    AcquireAccessMismatch,
    AcquireBusy,
    AcquireCommitted,
    AcquireExisting,
    AcquireResult,
    AcquireSuperseded,
    GenerationMismatch,
    ReleaseAccessMismatch,
    ReleaseAcquisitionRevoked,
    ReleaseDetached,
    ReleaseInactive,
    ReleaseResult,
)
from _managed.snapshot import Snapshot
from _managed.state import Current, Idle, ManagedAttempt, ManagedState, Preparing


__all__ = [
    "AccessModel",
    "AcquireAccessMismatch",
    "AcquireBusy",
    "AcquireCommitted",
    "AcquireExisting",
    "AcquireResult",
    "AcquireSuperseded",
    "Adapter",
    "Current",
    "GenerationMismatch",
    "Idle",
    "ManagedAttempt",
    "ManagedCoordinator",
    "ManagedState",
    "Preparing",
    "ReleaseAccessMismatch",
    "ReleaseAcquisitionRevoked",
    "ReleaseDetached",
    "ReleaseInactive",
    "ReleaseResult",
    "Snapshot",
]
