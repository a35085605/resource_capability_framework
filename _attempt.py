from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event, Lock


@dataclass(slots=True, eq=False)
class AttemptToken:
    """Process-local identity plus persistent cancellation for one acquire attempt.

    A token may begin at most one Resource acquisition lifecycle.  Cancellation is
    sticky so a release that races before lower-layer registration cannot be lost.
    """

    _cancellation: Event = field(default_factory=Event, init=False, repr=False)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _started: bool = field(default=False, init=False, repr=False)

    @property
    def cancellation(self) -> Event:
        return self._cancellation

    @property
    def cancelled(self) -> bool:
        return self._cancellation.is_set()

    def begin(self) -> bool:
        """Claim this token for its single acquisition lifecycle."""

        with self._lock:
            if self._started:
                return False
            self._started = True
            return True

    def cancel(self) -> None:
        self._cancellation.set()


__all__ = ["AttemptToken"]
