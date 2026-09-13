"""Contracts and result models for capability lifecycles."""

from _lifecycle_new.capability.projection import CapabilityProjector
from _lifecycle_new.capability.result import (
    AcquireAlreadyActive,
    AcquireFailed,
    AcquireReleaseRequired,
    AcquireRequestMismatch,
    AcquireResult,
    AcquireSucceeded,
    LifecycleBusy,
    GenerationMismatch,
    ReleaseAlreadyIdle,
    ReleaseFailed,
    ReleaseRequestMismatch,
    ReleaseResult,
    ReleaseSucceeded,
)
from _lifecycle_new.capability.snapshot import LifecyclePhase, LifecycleSnapshot


__all__ = [
    "CapabilityProjector",
    "AcquireAlreadyActive",
    "AcquireFailed",
    "AcquireReleaseRequired",
    "AcquireRequestMismatch",
    "AcquireResult",
    "AcquireSucceeded",
    "LifecycleBusy",
    "GenerationMismatch",
    "LifecyclePhase",
    "LifecycleSnapshot",
    "ReleaseAlreadyIdle",
    "ReleaseFailed",
    "ReleaseRequestMismatch",
    "ReleaseResult",
    "ReleaseSucceeded",
]
