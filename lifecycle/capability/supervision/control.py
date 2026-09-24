from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
import math
from numbers import Real
from typing import Protocol


Clock = Callable[[], float]


class CancellationSignal(Protocol):
    """Minimal cancellation contract accepted by synchronous supervisors."""

    def is_set(self) -> bool: ...


class SupervisionStopReason(Enum):
    """Why bounded supervision stopped before reaching a terminal lifecycle result."""

    TIMED_OUT = auto()
    CANCELLED = auto()
    HOST_REQUIRED = auto()


@dataclass(frozen=True, slots=True)
class SupervisionStopped:
    """Report that a caller-supplied supervision bound stopped retrying."""

    reason: SupervisionStopReason
    attempts: int

    def __post_init__(self) -> None:
        if not isinstance(self.reason, SupervisionStopReason):
            raise TypeError("reason must be SupervisionStopReason")
        if isinstance(self.attempts, bool) or not isinstance(self.attempts, int):
            raise TypeError("attempts must be an integer")
        if self.attempts < 0:
            raise ValueError("attempts must be greater than or equal to zero")


def normalize_supervision_timeout(value: object, *, field_name: str) -> float | None:
    """Normalize an optional positive timeout used to bound one supervision call."""

    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"{field_name} must be a real number or None")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0.0:
        raise ValueError(f"{field_name} must be finite and greater than zero")
    return normalized


def validate_cancellation(cancellation: CancellationSignal | None) -> None:
    if cancellation is not None and not callable(getattr(cancellation, "is_set", None)):
        raise TypeError("cancellation must provide is_set() or be None")


def stop_reason(
    *,
    deadline: float | None,
    cancellation: CancellationSignal | None,
    clock: Clock,
) -> SupervisionStopReason | None:
    if cancellation is not None and cancellation.is_set():
        return SupervisionStopReason.CANCELLED
    if deadline is not None and clock() >= deadline:
        return SupervisionStopReason.TIMED_OUT
    return None


def retry_delay(
    requested_seconds: float,
    *,
    deadline: float | None,
    clock: Clock,
) -> float:
    """Clamp one retry sleep so it never intentionally extends past the deadline."""

    if deadline is None:
        return requested_seconds
    return min(requested_seconds, max(0.0, deadline - clock()))


__all__ = [
    "CancellationSignal",
    "Clock",
    "SupervisionStopped",
    "SupervisionStopReason",
    "normalize_supervision_timeout",
]
