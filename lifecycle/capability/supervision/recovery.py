from __future__ import annotations

from collections.abc import Callable
from enum import Enum, auto
from time import monotonic, sleep
from typing import Generic, TypeAlias, TypeVar

from lifecycle.capability.lifecycle import CapabilityLifecycle
from lifecycle.capability.result import LifecycleOutcome, LifecycleResult
from lifecycle.capability.snapshot import LifecyclePhase
from lifecycle.capability.supervision.control import (
    CancellationSignal,
    Clock,
    SupervisionStopped,
    SupervisionStopReason,
    normalize_supervision_timeout,
    retry_delay,
    stop_reason,
    validate_cancellation,
)
from lifecycle.capability.supervision.policy import RecoverySupervisionPolicy
from lifecycle.resource.result import ResourceCleanupStatus


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")
_Sleeper = Callable[[float], None]


class RecoveryDisposition(Enum):
    COMPLETED = auto()
    RETRYABLE = auto()
    HOST_REQUIRED = auto()
    NOT_PENDING = auto()
    GENERATION_MISMATCH = auto()
    REQUEST_MISMATCH = auto()
    BUSY = auto()


def classify_recovery_result(
    result: LifecycleResult[GenerationT, RequestT, CapabilityT],
    generation: GenerationT,
    request: RequestT,
) -> RecoveryDisposition:
    if not isinstance(result, LifecycleResult):
        raise TypeError("result must be LifecycleResult")
    if result.outcome is LifecycleOutcome.RECOVERY_COMPLETED:
        return RecoveryDisposition.COMPLETED
    if result.outcome is LifecycleOutcome.RECOVERY_INCOMPLETE:
        snapshot = result.snapshot
        if (
            snapshot.phase is LifecyclePhase.CLEANUP_PENDING
            and snapshot.cleanup_status is ResourceCleanupStatus.BLOCKED
        ):
            return RecoveryDisposition.HOST_REQUIRED
        if snapshot.phase in (
            LifecyclePhase.CLEANUP_PENDING,
            LifecyclePhase.FINALIZATION_PENDING,
        ):
            return RecoveryDisposition.RETRYABLE
        raise RuntimeError("incomplete recovery must remain pending")
    if result.outcome is not LifecycleOutcome.NOT_EXECUTED:
        raise RuntimeError("recovery classifier received a non-recovery executed outcome")

    snapshot = result.snapshot
    if snapshot.generation != generation:
        return RecoveryDisposition.GENERATION_MISMATCH
    if snapshot.phase in (
        LifecyclePhase.ACQUIRING,
        LifecyclePhase.RELEASING,
        LifecyclePhase.RECOVERING,
    ):
        return RecoveryDisposition.BUSY
    if snapshot.phase in (
        LifecyclePhase.CLEANUP_PENDING,
        LifecyclePhase.FINALIZATION_PENDING,
    ):
        if snapshot.request != request:
            return RecoveryDisposition.REQUEST_MISMATCH
        # A matching pending state should have executed unless BLOCKED; BLOCKED itself
        # returns an executed RECOVERY_INCOMPLETE result.
        raise RuntimeError("matching pending recovery was unexpectedly not executed")
    return RecoveryDisposition.NOT_PENDING


RecoverySupervisionResult: TypeAlias = (
    LifecycleResult[GenerationT, RequestT, CapabilityT] | SupervisionStopped
)


class RecoverySupervisor(Generic[GenerationT, RequestT, CapabilityT]):
    """Continue existing cleanup/finalization only within the original generation."""

    def __init__(
        self,
        lifecycle: CapabilityLifecycle[GenerationT, RequestT, CapabilityT],
        *,
        policy: RecoverySupervisionPolicy = RecoverySupervisionPolicy(),
        _sleeper: _Sleeper = sleep,
        _clock: Clock = monotonic,
    ) -> None:
        if not isinstance(lifecycle, CapabilityLifecycle):
            raise TypeError("lifecycle must satisfy CapabilityLifecycle")
        if not isinstance(policy, RecoverySupervisionPolicy):
            raise TypeError("policy must be RecoverySupervisionPolicy")
        if not callable(_sleeper) or not callable(_clock):
            raise TypeError("_sleeper and _clock must be callable")
        self._lifecycle = lifecycle
        self._policy = policy
        self._sleep = _sleeper
        self._clock = _clock

    def supervise(
        self,
        generation: GenerationT,
        request: RequestT,
        *,
        timeout_seconds: float | None = None,
        cancellation: CancellationSignal | None = None,
    ) -> RecoverySupervisionResult[GenerationT, RequestT, CapabilityT]:
        if generation is None:
            raise TypeError("generation cannot be None")
        if request is None:
            raise TypeError("request cannot be None")
        timeout = normalize_supervision_timeout(
            timeout_seconds,
            field_name="recovery supervision timeout",
        )
        validate_cancellation(cancellation)
        deadline = None if timeout is None else self._clock() + timeout
        attempts = 0

        while True:
            reason = stop_reason(deadline=deadline, cancellation=cancellation, clock=self._clock)
            if reason is not None:
                return SupervisionStopped(reason, attempts)
            attempts += 1
            result = self._lifecycle.recover(generation, request)
            disposition = classify_recovery_result(result, generation, request)
            if disposition is RecoveryDisposition.HOST_REQUIRED:
                return SupervisionStopped(SupervisionStopReason.HOST_REQUIRED, attempts)
            if disposition not in (RecoveryDisposition.RETRYABLE, RecoveryDisposition.BUSY):
                return result
            reason = stop_reason(deadline=deadline, cancellation=cancellation, clock=self._clock)
            if reason is not None:
                return SupervisionStopped(reason, attempts)
            delay = retry_delay(self._policy.retry_seconds, deadline=deadline, clock=self._clock)
            if delay <= 0.0:
                return SupervisionStopped(SupervisionStopReason.TIMED_OUT, attempts)
            self._sleep(delay)


__all__ = [
    "RecoveryDisposition",
    "RecoverySupervisionResult",
    "RecoverySupervisor",
    "classify_recovery_result",
]
