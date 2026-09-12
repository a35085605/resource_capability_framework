from __future__ import annotations

from collections.abc import Callable
from threading import Thread
from typing import Protocol


class CleanupScheduler(Protocol):
    """Non-blocking execution policy for detached resource cleanup work.

    ``schedule`` must return promptly without waiting for the job to finish, must not
    fall back to running the job on the caller thread, and must return ``False`` when
    it cannot accept the job. A ``True`` result transfers execution responsibility to
    the scheduler.
    """

    def schedule(self, job: Callable[[], None]) -> bool: ...


class DaemonThreadCleanupScheduler:
    """Schedule each cleanup job on its own detached daemon thread."""

    def schedule(self, job: Callable[[], None]) -> bool:
        if not callable(job):
            raise TypeError("job must be callable")

        try:
            worker = Thread(
                target=job,
                name=f"resource-cleanup-{id(job):x}",
                daemon=True,
            )
            worker.start()
        except Exception:
            return False
        return True


__all__ = ["CleanupScheduler", "DaemonThreadCleanupScheduler"]
