"""Managed Access authority and capability coordination primitives."""

from _access import AccessIdentity
from _attempt import AttemptToken
from _managed.adapter import Adapter
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
from _managed.state import Current, Idle, ManagedState, Preparing


__all__ = [
    "AccessIdentity",
    "AcquireAccessMismatch",
    "AcquireBusy",
    "AcquireCommitted",
    "AcquireExisting",
    "AcquireResult",
    "AcquireSuperseded",
    "Adapter",
    "AttemptToken",
    "Current",
    "GenerationMismatch",
    "Idle",
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
