from __future__ import annotations

import math
from dataclasses import dataclass
from numbers import Real


def _normalize_positive_seconds(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"{field_name} must be a real number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0.0:
        raise ValueError(f"{field_name} must be finite and greater than zero")
    return normalized


@dataclass(frozen=True, slots=True)
class AcquireSupervisionPolicy:
    deferred_retry_seconds: float = 0.1

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "deferred_retry_seconds",
            _normalize_positive_seconds(
                self.deferred_retry_seconds,
                field_name="acquire supervision deferred retry",
            ),
        )


@dataclass(frozen=True, slots=True)
class RecoverySupervisionPolicy:
    retry_seconds: float = 0.1

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "retry_seconds",
            _normalize_positive_seconds(
                self.retry_seconds,
                field_name="recovery supervision retry",
            ),
        )


@dataclass(frozen=True, slots=True)
class ReleaseSupervisionPolicy:
    retry_seconds: float = 0.1

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "retry_seconds",
            _normalize_positive_seconds(
                self.retry_seconds,
                field_name="release supervision retry",
            ),
        )


__all__ = [
    "AcquireSupervisionPolicy",
    "RecoverySupervisionPolicy",
    "ReleaseSupervisionPolicy",
]
