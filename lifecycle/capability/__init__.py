"""Contracts and result models for capability lifecycles."""

from lifecycle.capability.lifecycle import CapabilityLifecycle, LifecycleSnapshotReader
from lifecycle.capability.projection import CapabilityProjector
from lifecycle.capability.result import (
    AcquireResult,
    LifecycleDiagnostics,
    LifecycleOutcome,
    LifecycleResult,
    RecoveryResult,
    ReleaseResult,
)
from lifecycle.capability.session import (
    CapabilitySessionFactory,
    CleanupReport,
    DefaultCapabilitySessionFactory,
    PreparationFailed,
    PreparedSession,
    SessionOwner,
)
from lifecycle.capability.snapshot import CleanupOrigin, LifecyclePhase, LifecycleSnapshot


__all__ = [
    "AcquireResult",
    "CapabilityLifecycle",
    "CapabilityProjector",
    "CapabilitySessionFactory",
    "CleanupOrigin",
    "CleanupReport",
    "DefaultCapabilitySessionFactory",
    "LifecycleDiagnostics",
    "LifecycleOutcome",
    "LifecyclePhase",
    "LifecycleResult",
    "LifecycleSnapshot",
    "LifecycleSnapshotReader",
    "PreparationFailed",
    "PreparedSession",
    "RecoveryResult",
    "ReleaseResult",
    "SessionOwner",
]
