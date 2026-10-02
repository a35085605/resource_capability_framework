from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeAlias, TypeVar

from lifecycle.capability.diagnostics import LifecycleDiagnostics
from lifecycle.capability.snapshot import LifecycleSnapshot


RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


class LifecycleOutcome(Enum):
    """Result of the operation represented by one lifecycle call."""

    NOT_EXECUTED = "not_executed"
    ACQUIRE_SUCCEEDED = "acquire_succeeded"
    ACQUIRE_FAILED = "acquire_failed"
    RELEASE_COMPLETED = "release_completed"


@dataclass(frozen=True, slots=True)
class LifecycleResult(Generic[RequestT, CapabilityT]):
    """Return the committed snapshot and explicit outcome of one lifecycle call."""

    snapshot: LifecycleSnapshot[RequestT, CapabilityT]
    outcome: LifecycleOutcome
    diagnostics: LifecycleDiagnostics = LifecycleDiagnostics()

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot, LifecycleSnapshot):
            raise TypeError("snapshot must be LifecycleSnapshot")
        if not isinstance(self.outcome, LifecycleOutcome):
            raise TypeError("outcome must be LifecycleOutcome")
        if not isinstance(self.diagnostics, LifecycleDiagnostics):
            raise TypeError("diagnostics must be LifecycleDiagnostics")

    @property
    def execution_started(self) -> bool:
        return self.outcome is not LifecycleOutcome.NOT_EXECUTED

    @classmethod
    def not_executed(
        cls,
        snapshot: LifecycleSnapshot[RequestT, CapabilityT],
    ) -> "LifecycleResult[RequestT, CapabilityT]":
        return cls(snapshot, LifecycleOutcome.NOT_EXECUTED)


AcquireResult: TypeAlias = LifecycleResult[RequestT, CapabilityT]
ReleaseResult: TypeAlias = LifecycleResult[RequestT, CapabilityT]


__all__ = [
    "AcquireResult",
    "LifecycleDiagnostics",
    "LifecycleOutcome",
    "LifecycleResult",
    "ReleaseResult",
]
