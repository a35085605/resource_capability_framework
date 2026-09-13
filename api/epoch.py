from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Generic, Protocol, TypeVar, runtime_checkable


@dataclass(frozen=True, slots=True, order=True)
class Epoch:
    """Represent a positive integer ordinal for one lifetime.

    ``Epoch`` validates the ordinal value itself; monotonic issuance is a responsibility
    of an issuer such as ``EpochSequence``.
    """

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
    """Issue monotonically increasing epochs within one issuer scope."""

    def issue(self) -> EpochT:
        """Return an epoch newer than every epoch previously issued here."""
        ...


class EpochSequence(Generic[EpochT]):
    """Thread-safely issue consecutive epochs of one concrete epoch type.

    The first issued value is ``initial_value + 1`` and each later call increments it by
    one within this sequence instance.
    """

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
        """Atomically increment the sequence and return the resulting epoch."""

        with self._lock:
            self._current += 1
            return self._epoch_type(self._current)


__all__ = ["Epoch", "EpochIssuer", "EpochSequence"]
