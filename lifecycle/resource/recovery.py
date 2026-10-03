from __future__ import annotations

from dataclasses import dataclass
from threading import Lock

from lifecycle.effect import IO


@dataclass(frozen=True, slots=True)
class RecoveryEntry:
    """Detached finalization responsibility retained after a finalizer failed."""

    resource: object
    release: IO[None]
    error: BaseException

    def __post_init__(self) -> None:
        if not isinstance(self.release, IO):
            raise TypeError("release must be IO")
        if not isinstance(self.error, BaseException):
            raise TypeError("error must be a BaseException")


class ResourceRecoveryPool:
    """Thread-safe in-memory sink for detached failed finalization responsibilities.

    The pool deliberately exposes only immutable snapshots. It does not retry entries or
    schedule background work; adapters decide what a detached recovery action means.
    Each action must remain valid after its former ancestor scopes have closed.
    """

    __slots__ = ("_entries", "_lock")

    def __init__(self) -> None:
        self._entries: list[RecoveryEntry] = []
        self._lock = Lock()

    def snapshot(self) -> tuple[RecoveryEntry, ...]:
        with self._lock:
            return tuple(self._entries)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def _accept(
        self,
        resource: object,
        release: IO[None],
        error: BaseException,
    ) -> None:
        entry = RecoveryEntry(resource, release, error)
        with self._lock:
            self._entries.append(entry)


__all__ = ["RecoveryEntry", "ResourceRecoveryPool"]
