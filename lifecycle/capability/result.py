from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeAlias, TypeVar

from lifecycle.capability.diagnostics import LifecycleDiagnostics
from lifecycle.capability.snapshot import CleanupOrigin, LifecycleSnapshot


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


class LifecycleOutcome(Enum):
    """Result of the operation represented by one lifecycle call."""

    NOT_EXECUTED = "not_executed"
    ACQUIRE_SUCCEEDED = "acquire_succeeded"
    ACQUIRE_FAILED = "acquire_failed"
    RELEASE_COMPLETED = "release_completed"
    RELEASE_INCOMPLETE = "release_incomplete"
    RECOVERY_COMPLETED = "recovery_completed"
    RECOVERY_INCOMPLETE = "recovery_incomplete"


@dataclass(frozen=True, slots=True)
class LifecycleResult(Generic[GenerationT, RequestT, CapabilityT]):
    """Return the committed snapshot and explicit outcome of one lifecycle call."""

    snapshot: LifecycleSnapshot[GenerationT, RequestT, CapabilityT]
    outcome: LifecycleOutcome
    diagnostics: LifecycleDiagnostics = LifecycleDiagnostics()
    origin: CleanupOrigin | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot, LifecycleSnapshot):
            raise TypeError("snapshot must be LifecycleSnapshot")
        if not isinstance(self.outcome, LifecycleOutcome):
            raise TypeError("outcome must be LifecycleOutcome")
        if not isinstance(self.diagnostics, LifecycleDiagnostics):
            raise TypeError("diagnostics must be LifecycleDiagnostics")
        if self.origin is not None and not isinstance(self.origin, CleanupOrigin):
            raise TypeError("origin must be CleanupOrigin or None")
        if self.outcome in (
            LifecycleOutcome.ACQUIRE_SUCCEEDED,
            LifecycleOutcome.ACQUIRE_FAILED,
        ):
            if self.origin is not CleanupOrigin.ACQUIRE:
                raise ValueError("acquire outcomes require ACQUIRE origin")
        elif self.outcome in (
            LifecycleOutcome.RELEASE_COMPLETED,
            LifecycleOutcome.RELEASE_INCOMPLETE,
        ):
            if self.origin is not CleanupOrigin.RELEASE:
                raise ValueError("release outcomes require RELEASE origin")
        elif self.outcome in (
            LifecycleOutcome.RECOVERY_COMPLETED,
            LifecycleOutcome.RECOVERY_INCOMPLETE,
        ):
            if self.origin is None:
                raise ValueError("recovery outcomes require the pending origin")
        elif self.origin is not None:
            raise ValueError("NOT_EXECUTED cannot have an operation origin")

    @property
    def execution_started(self) -> bool:
        """Compatibility-shaped derived fact; outcome remains the source of truth."""

        return self.outcome is not LifecycleOutcome.NOT_EXECUTED

    @classmethod
    def not_executed(
        cls,
        snapshot: LifecycleSnapshot[GenerationT, RequestT, CapabilityT],
    ) -> "LifecycleResult[GenerationT, RequestT, CapabilityT]":
        return cls(snapshot, LifecycleOutcome.NOT_EXECUTED)


AcquireResult: TypeAlias = LifecycleResult[GenerationT, RequestT, CapabilityT]
ReleaseResult: TypeAlias = LifecycleResult[GenerationT, RequestT, CapabilityT]
RecoveryResult: TypeAlias = LifecycleResult[GenerationT, RequestT, CapabilityT]


__all__ = [
    "AcquireResult",
    "LifecycleDiagnostics",
    "LifecycleOutcome",
    "LifecycleResult",
    "RecoveryResult",
    "ReleaseResult",
]
