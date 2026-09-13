from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Generic, Protocol, TypeVar, runtime_checkable


@dataclass(frozen=True, slots=True, order=True)
class Epoch:
    """Strongly typed monotonic lifetime ordinal."""

    value: int

    def __post_init__(self) -> None:
        if isinstance(self.value, bool) or not isinstance(self.value, int):
            raise TypeError("epoch value must be an integer")
        if self.value <= 0:
            raise ValueError("epoch value must be greater than zero")

    def __str__(self) -> str:
        return str(self.value)


EpochT = TypeVar("EpochT", bound=Epoch)


@runtime_checkable
class EpochIssuer(Protocol[EpochT]):
    """Issue monotonically increasing epochs within one ownership scope."""

    def issue(self) -> EpochT:
        ...


class EpochSequence(Generic[EpochT]):
    """Thread-safe monotonically increasing issuer for one concrete epoch type."""

    def __init__(self, epoch_type: type[EpochT], *, initial_value: int = 0) -> None:
        if not isinstance(epoch_type, type) or not issubclass(epoch_type, Epoch):
            raise TypeError("epoch_type must be an Epoch subclass")
        if isinstance(initial_value, bool) or not isinstance(initial_value, int):
            raise TypeError("initial_value must be an integer")
        if initial_value < 0:
            raise ValueError("initial_value must be greater than or equal to zero")
        self._epoch_type = epoch_type
        self._lock = Lock()
        self._current = initial_value

    def issue(self) -> EpochT:
        with self._lock:
            self._current += 1
            return self._epoch_type(self._current)


__all__ = ["Epoch", "EpochIssuer", "EpochSequence"]
