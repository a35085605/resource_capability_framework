from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock


@dataclass(frozen=True, slots=True)
class RecoveryEntry:
    """Detached finalization responsibility retained after a finalizer failed."""

    resource: object
    release: Callable[[], None]
    error: BaseException

    def __post_init__(self) -> None:
        if not callable(self.release):
            raise TypeError("release must be callable")
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
        release: Callable[[], None],
        error: BaseException,
    ) -> None:
        entry = RecoveryEntry(resource, release, error)
        with self._lock:
            self._entries.append(entry)


__all__ = ["RecoveryEntry", "ResourceRecoveryPool"]
