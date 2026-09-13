"""Contracts and result models for capability lifecycles."""

from _lifecycle.capability.projection import CapabilityProjector
from _lifecycle.capability.result import (
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
from _lifecycle.capability.snapshot import LifecyclePhase, LifecycleSnapshot


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
