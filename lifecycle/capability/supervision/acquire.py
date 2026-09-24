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
from lifecycle.capability.supervision.policy import AcquireSupervisionPolicy


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")
_Sleeper = Callable[[float], None]


class AcquireDisposition(Enum):
    SUCCEEDED = auto()
    ALREADY_ACTIVE = auto()
    FAILED = auto()
    RECOVERY_REQUIRED = auto()
    GENERATION_MISMATCH = auto()
    REQUEST_MISMATCH = auto()
    BUSY = auto()


def classify_acquire_result(
    result: LifecycleResult[GenerationT, RequestT, CapabilityT],
    generation: GenerationT,
    request: RequestT,
) -> AcquireDisposition:
    if not isinstance(result, LifecycleResult):
        raise TypeError("result must be LifecycleResult")

    if result.outcome is LifecycleOutcome.ACQUIRE_SUCCEEDED:
        return AcquireDisposition.SUCCEEDED
    if result.outcome is LifecycleOutcome.ACQUIRE_FAILED:
        return AcquireDisposition.FAILED
    if result.outcome is not LifecycleOutcome.NOT_EXECUTED:
        raise RuntimeError("acquire classifier received a non-acquire executed outcome")

    snapshot = result.snapshot
    if snapshot.generation != generation:
        return AcquireDisposition.GENERATION_MISMATCH
    if snapshot.phase in (
        LifecyclePhase.ACQUIRING,
        LifecyclePhase.RELEASING,
        LifecyclePhase.RECOVERING,
    ):
        return AcquireDisposition.BUSY
    if snapshot.phase is LifecyclePhase.ACTIVE:
        if snapshot.request == request:
            return AcquireDisposition.ALREADY_ACTIVE
        return AcquireDisposition.REQUEST_MISMATCH
    if snapshot.phase in (
        LifecyclePhase.CLEANUP_PENDING,
        LifecyclePhase.FINALIZATION_PENDING,
    ):
        if snapshot.request != request:
            return AcquireDisposition.REQUEST_MISMATCH
        return AcquireDisposition.RECOVERY_REQUIRED
    raise RuntimeError("non-executed acquire returned an unsupported lifecycle state")


AcquireSupervisionResult: TypeAlias = (
    LifecycleResult[GenerationT, RequestT, CapabilityT] | SupervisionStopped
)


class AcquireSupervisor(Generic[GenerationT, RequestT, CapabilityT]):
    """Retry only same-generation busy acquisition calls; never cross generations."""

    def __init__(
        self,
        lifecycle: CapabilityLifecycle[GenerationT, RequestT, CapabilityT],
        *,
        policy: AcquireSupervisionPolicy = AcquireSupervisionPolicy(),
        _sleeper: _Sleeper = sleep,
        _clock: Clock = monotonic,
    ) -> None:
        if not isinstance(lifecycle, CapabilityLifecycle):
            raise TypeError("lifecycle must satisfy CapabilityLifecycle")
        if not isinstance(policy, AcquireSupervisionPolicy):
            raise TypeError("policy must be AcquireSupervisionPolicy")
        if not callable(_sleeper) or not callable(_clock):
            raise TypeError("_sleeper and _clock must be callable")
        self._lifecycle = lifecycle
        self._policy = policy
        self._sleep = _sleeper
        self._clock = _clock

    @property
    def lifecycle(self) -> CapabilityLifecycle[GenerationT, RequestT, CapabilityT]:
        return self._lifecycle

    @property
    def policy(self) -> AcquireSupervisionPolicy:
        return self._policy

    def supervise(
        self,
        generation: GenerationT,
        request: RequestT,
        *,
        timeout_seconds: float | None = None,
        cancellation: CancellationSignal | None = None,
    ) -> AcquireSupervisionResult[GenerationT, RequestT, CapabilityT]:
        if generation is None:
            raise TypeError("generation cannot be None")
        if request is None:
            raise TypeError("request cannot be None")
        timeout = normalize_supervision_timeout(
            timeout_seconds,
            field_name="acquire supervision timeout",
        )
        validate_cancellation(cancellation)
        deadline = None if timeout is None else self._clock() + timeout
        attempts = 0

        while True:
            reason = stop_reason(deadline=deadline, cancellation=cancellation, clock=self._clock)
            if reason is not None:
                return SupervisionStopped(reason, attempts)
            attempts += 1
            result = self._lifecycle.acquire(generation, request)
            if classify_acquire_result(result, generation, request) is not AcquireDisposition.BUSY:
                return result
            reason = stop_reason(deadline=deadline, cancellation=cancellation, clock=self._clock)
            if reason is not None:
                return SupervisionStopped(reason, attempts)
            delay = retry_delay(
                self._policy.deferred_retry_seconds,
                deadline=deadline,
                clock=self._clock,
            )
            if delay <= 0.0:
                return SupervisionStopped(SupervisionStopReason.TIMED_OUT, attempts)
            self._sleep(delay)


__all__ = [
    "AcquireDisposition",
    "AcquireSupervisionResult",
    "AcquireSupervisor",
    "classify_acquire_result",
]
