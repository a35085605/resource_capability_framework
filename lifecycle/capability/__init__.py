"""Capability lifecycle fencing over structured Resource ownership."""

from lifecycle.capability.coordinator import CapabilityLifecycleCoordinator
from lifecycle.capability.diagnostics import LifecycleDiagnostics
from lifecycle.capability.lifecycle import CapabilityLifecycle, LifecycleSnapshotReader
from lifecycle.capability.result import (
    AcquireResult,
    LifecycleOutcome,
    LifecycleResult,
    ReleaseResult,
)
from lifecycle.capability.snapshot import LifecyclePhase, LifecycleSnapshot


__all__ = [
    "AcquireResult",
    "CapabilityLifecycle",
    "CapabilityLifecycleCoordinator",
    "LifecycleDiagnostics",
    "LifecycleOutcome",
    "LifecyclePhase",
    "LifecycleResult",
    "LifecycleSnapshot",
    "LifecycleSnapshotReader",
    "ReleaseResult",
]
