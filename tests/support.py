from __future__ import annotations

from collections.abc import Callable
from threading import Event, Thread
from typing import Any
from unittest import TestCase

from lifecycle.resource import ResourceScope


TIMEOUT = 5


def wait(event: Event) -> None:
    if not event.wait(TIMEOUT):
        raise AssertionError("timed out waiting for the test's synchronization event")


def wait_closing(scope: ResourceScope) -> None:
    # Observe the admission boundary itself instead of guessing thread scheduling.
    with scope._condition:
        if not scope._condition.wait_for(
            lambda: not scope._is_open_locked(), TIMEOUT
        ):
            raise AssertionError("scope did not begin closing")


class ThreadCall:
    """Capture worker failures so assertions cannot silently die on a test thread."""

    def __init__(self, callback: Callable[[], Any]) -> None:
        self.result: Any = None
        self.error: BaseException | None = None
        self.done = Event()

        def run() -> None:
            try:
                self.result = callback()
            except BaseException as exc:
                self.error = exc
            finally:
                self.done.set()

        self.thread = Thread(target=run, daemon=True)
        self.thread.start()

    def join(self, case: TestCase) -> "ThreadCall":
        self.thread.join(TIMEOUT)
        case.assertFalse(self.thread.is_alive(), "worker did not complete")
        return self

    def succeeded(self, case: TestCase) -> Any:
        self.join(case)
        if self.error is not None:
            raise self.error
        return self.result
