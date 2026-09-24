from __future__ import annotations

from collections.abc import Callable
from enum import Enum, auto
from time import monotonic, sleep
from typing import Generic, TypeAlias, TypeVar

from lifecycle.capability.lifecycle import CapabilityLifecycle
from lifecycle.capability.result import LifecycleOutcome, LifecycleResult
from lifecycle.capability.snapshot import CleanupOrigin, LifecyclePhase
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
from lifecycle.capability.supervision.policy import (
    RecoverySupervisionPolicy,
    ReleaseSupervisionPolicy,
)
from lifecycle.capability.supervision.recovery import RecoverySupervisor


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")
_Sleeper = Callable[[float], None]


class ReleaseDisposition(Enum):
    SUCCEEDED = auto()
    ALREADY_IDLE = auto()
    INCOMPLETE = auto()
    RECOVERY_REQUIRED = auto()
    GENERATION_MISMATCH = auto()
    REQUEST_MISMATCH = auto()
    BUSY = auto()


def classify_release_result(
    result: LifecycleResult[GenerationT, RequestT, CapabilityT],
    generation: GenerationT,
    request: RequestT,
) -> ReleaseDisposition:
    if not isinstance(result, LifecycleResult):
        raise TypeError("result must be LifecycleResult")

    if result.outcome is LifecycleOutcome.RELEASE_COMPLETED:
        return ReleaseDisposition.SUCCEEDED
    if result.outcome is LifecycleOutcome.RELEASE_INCOMPLETE:
        return ReleaseDisposition.INCOMPLETE
    if result.outcome is LifecycleOutcome.RECOVERY_COMPLETED:
        if result.origin is CleanupOrigin.RELEASE:
            return ReleaseDisposition.SUCCEEDED
        raise RuntimeError("release classifier received acquire-origin recovery")
    if result.outcome is LifecycleOutcome.RECOVERY_INCOMPLETE:
        if result.origin is CleanupOrigin.RELEASE:
            return ReleaseDisposition.INCOMPLETE
        raise RuntimeError("release classifier received acquire-origin recovery")
    if result.outcome is not LifecycleOutcome.NOT_EXECUTED:
        raise RuntimeError("release classifier received a non-release executed outcome")

    snapshot = result.snapshot
    if snapshot.generation != generation:
        return ReleaseDisposition.GENERATION_MISMATCH
    if snapshot.phase in (
        LifecyclePhase.ACQUIRING,
        LifecyclePhase.RELEASING,
        LifecyclePhase.RECOVERING,
    ):
        return ReleaseDisposition.BUSY
    if snapshot.phase is LifecyclePhase.IDLE:
        return ReleaseDisposition.ALREADY_IDLE
    if snapshot.phase is LifecyclePhase.ACTIVE:
        if snapshot.request != request:
            return ReleaseDisposition.REQUEST_MISMATCH
        raise RuntimeError("matching active release was unexpectedly not executed")
    if snapshot.phase in (
        LifecyclePhase.CLEANUP_PENDING,
        LifecyclePhase.FINALIZATION_PENDING,
    ):
        if snapshot.request != request:
            return ReleaseDisposition.REQUEST_MISMATCH
        return ReleaseDisposition.RECOVERY_REQUIRED
    raise RuntimeError("non-executed release returned an unsupported lifecycle state")


ReleaseSupervisionResult: TypeAlias = (
    LifecycleResult[GenerationT, RequestT, CapabilityT] | SupervisionStopped
)


class ReleaseSupervisor(Generic[GenerationT, RequestT, CapabilityT]):
    """Start release once, then delegate unresolved work to recovery."""

    def __init__(
        self,
        lifecycle: CapabilityLifecycle[GenerationT, RequestT, CapabilityT],
        *,
        policy: ReleaseSupervisionPolicy = ReleaseSupervisionPolicy(),
        _sleeper: _Sleeper = sleep,
        _clock: Clock = monotonic,
    ) -> None:
        if not isinstance(lifecycle, CapabilityLifecycle):
            raise TypeError("lifecycle must satisfy CapabilityLifecycle")
        if not isinstance(policy, ReleaseSupervisionPolicy):
            raise TypeError("policy must be ReleaseSupervisionPolicy")
        if not callable(_sleeper) or not callable(_clock):
            raise TypeError("_sleeper and _clock must be callable")
        self._lifecycle = lifecycle
        self._policy = policy
        self._sleep = _sleeper
        self._clock = _clock
        self._recovery = RecoverySupervisor(
            lifecycle,
            policy=RecoverySupervisionPolicy(policy.retry_seconds),
            _sleeper=_sleeper,
            _clock=_clock,
        )

    @property
    def lifecycle(self) -> CapabilityLifecycle[GenerationT, RequestT, CapabilityT]:
        return self._lifecycle

    @property
    def policy(self) -> ReleaseSupervisionPolicy:
        return self._policy

    def supervise(
        self,
        generation: GenerationT,
        request: RequestT,
        *,
        timeout_seconds: float | None = None,
        cancellation: CancellationSignal | None = None,
    ) -> ReleaseSupervisionResult[GenerationT, RequestT, CapabilityT]:
        if generation is None:
            raise TypeError("generation cannot be None")
        if request is None:
            raise TypeError("request cannot be None")
        timeout = normalize_supervision_timeout(
            timeout_seconds,
            field_name="release supervision timeout",
        )
        validate_cancellation(cancellation)
        deadline = None if timeout is None else self._clock() + timeout
        attempts = 0

        while True:
            reason = stop_reason(deadline=deadline, cancellation=cancellation, clock=self._clock)
            if reason is not None:
                return SupervisionStopped(reason, attempts)
            attempts += 1
            result = self._lifecycle.release(generation, request)
            disposition = classify_release_result(result, generation, request)
            if disposition is ReleaseDisposition.BUSY:
                reason = stop_reason(
                    deadline=deadline,
                    cancellation=cancellation,
                    clock=self._clock,
                )
                if reason is not None:
                    return SupervisionStopped(reason, attempts)
                delay = retry_delay(
                    self._policy.retry_seconds,
                    deadline=deadline,
                    clock=self._clock,
                )
                if delay <= 0.0:
                    return SupervisionStopped(SupervisionStopReason.TIMED_OUT, attempts)
                self._sleep(delay)
                continue

            if disposition in (
                ReleaseDisposition.INCOMPLETE,
                ReleaseDisposition.RECOVERY_REQUIRED,
            ):
                snapshot = result.snapshot
                if snapshot.phase not in (
                    LifecyclePhase.CLEANUP_PENDING,
                    LifecyclePhase.FINALIZATION_PENDING,
                ):
                    raise RuntimeError("release recovery did not leave pending state")
                # Never let a release-side helper consume acquire rollback debt. Runtime
                # deactivation handles generic pending recovery explicitly.
                if snapshot.origin is not CleanupOrigin.RELEASE:
                    return result
                remaining = None if deadline is None else max(0.0, deadline - self._clock())
                if remaining is not None and remaining <= 0.0:
                    return SupervisionStopped(SupervisionStopReason.TIMED_OUT, attempts)
                return self._recovery.supervise(
                    generation,
                    request,
                    timeout_seconds=remaining,
                    cancellation=cancellation,
                )

            return result


__all__ = [
    "ReleaseDisposition",
    "ReleaseSupervisionResult",
    "ReleaseSupervisor",
    "classify_release_result",
]
