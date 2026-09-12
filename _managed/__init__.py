"""Managed Request and capability coordination primitives."""

from _attempt import AttemptId
from _managed.coordinator import ManagedCoordinator
from _managed.result import (
    AcquireRequestMismatch,
    AcquireBusy,
    AcquireCommitted,
    AcquireExisting,
    AcquireResult,
    AcquireSuperseded,
    GenerationMismatch,
    ReleaseRequestMismatch,
    ReleaseAcquisitionRevoked,
    ReleaseDetached,
    ReleaseInactive,
    ReleaseResult,
)
from _managed.snapshot import Snapshot
from _managed.state import Current, Idle, ManagedState, Preparing


__all__ = [
    "AcquireRequestMismatch",
    "AcquireBusy",
    "AcquireCommitted",
    "AcquireExisting",
    "AcquireResult",
    "AcquireSuperseded",
    "AttemptId",
    "Current",
    "GenerationMismatch",
    "Idle",
    "ManagedCoordinator",
    "ManagedState",
    "Preparing",
    "ReleaseRequestMismatch",
    "ReleaseAcquisitionRevoked",
    "ReleaseDetached",
    "ReleaseInactive",
    "ReleaseResult",
    "Snapshot",
]
