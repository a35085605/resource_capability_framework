from __future__ import annotations

import unittest
from collections.abc import Callable
from threading import Event, Thread

from _managed.coordinator import ManagedCoordinator
from _managed.result import (
    AcquireCommitted,
    AcquireFailed,
    AcquireReleaseRequired,
    Busy,
    ReleaseDetached,
    ReleaseFailed,
)
from _managed.snapshot import ManagedPhase
from _resource.result import ResourceFailed, ResourceReady


class SequenceIssuer:
    def __init__(self, *, fail_on: set[int] | None = None) -> None:
        self._value = 0
        self._calls = 0
        self._fail_on = set() if fail_on is None else set(fail_on)

    def __call__(self) -> int:
        self._calls += 1
        if self._calls in self._fail_on:
            raise RuntimeError("generation failure")
        self._value += 1
        return self._value


class StubResourceManager:
    def __init__(
        self,
        *,
        acquire: Callable[[str], object] | None = None,
        release: Callable[[tuple[str, ...]], None] | None = None,
    ) -> None:
        self._acquire = acquire or (lambda request: ResourceReady((f"resource:{request}",)))
        self._release = release or (lambda resources: None)
        self.released: list[tuple[str, ...]] = []

    def acquire(self, request: str) -> object:
        return self._acquire(request)

    def release(self, resources: tuple[str, ...]) -> None:
        self.released.append(resources)
        self._release(resources)


class Projection:
    def __init__(self, *, fail: bool = False) -> None:
        self._fail = fail

    def project(self, request: str, resources: tuple[str, ...]) -> str:
        if self._fail:
            raise RuntimeError("projection failure")
        return f"capability:{request}:{','.join(resources)}"


class ManagedCoordinatorTests(unittest.TestCase):
    def make_coordinator(
        self,
        *,
        issuer: SequenceIssuer | None = None,
        manager: StubResourceManager | None = None,
        projection: Projection | None = None,
    ) -> ManagedCoordinator[int, str, str, str]:
        return ManagedCoordinator(
            issuer or SequenceIssuer(),
            manager or StubResourceManager(),
            projection or Projection(),
        )

    def test_successful_lifecycle_advances_generation_only_after_release(self) -> None:
        coordinator = self.make_coordinator()
        initial = coordinator.read()

        acquired = coordinator.acquire(
            expected_generation=initial.generation,
            request="request-a",
        )
        self.assertIsInstance(acquired, AcquireCommitted)
        self.assertEqual(coordinator.read().generation, initial.generation)
        self.assertIs(coordinator.read().phase, ManagedPhase.CURRENT)

        released = coordinator.release(
            expected_generation=initial.generation,
            request="request-a",
        )
        self.assertIsInstance(released, ReleaseDetached)
        self.assertGreater(released.next_generation, initial.generation)
        self.assertIs(coordinator.read().phase, ManagedPhase.IDLE)

    def test_failed_acquire_requires_release_even_when_no_resources_exist(self) -> None:
        manager = StubResourceManager(
            acquire=lambda request: ResourceFailed(RuntimeError("acquire failure"), ())
        )
        coordinator = self.make_coordinator(manager=manager)
        generation = coordinator.read().generation

        failed = coordinator.acquire(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(failed, AcquireFailed)
        self.assertIs(failed.snapshot.phase, ManagedPhase.CLEANUP_PENDING)

        blocked = coordinator.acquire(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(blocked, AcquireReleaseRequired)

        released = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(released, ReleaseDetached)
        self.assertEqual(manager.released, [])

    def test_projection_failure_retains_resources_for_release(self) -> None:
        manager = StubResourceManager(acquire=lambda request: ResourceReady(("r1", "r2")))
        coordinator = self.make_coordinator(manager=manager, projection=Projection(fail=True))
        generation = coordinator.read().generation

        failed = coordinator.acquire(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(failed, AcquireFailed)
        self.assertIs(failed.snapshot.phase, ManagedPhase.CLEANUP_PENDING)

        released = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(released, ReleaseDetached)
        self.assertEqual(manager.released, [("r1", "r2")])

    def test_release_cleanup_failure_preserves_generation_and_can_retry(self) -> None:
        attempts = 0

        def release(resources: tuple[str, ...]) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("cleanup failure")

        manager = StubResourceManager(release=release)
        coordinator = self.make_coordinator(manager=manager)
        generation = coordinator.read().generation
        coordinator.acquire(expected_generation=generation, request="request-a")

        failed = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(failed, ReleaseFailed)
        self.assertEqual(failed.snapshot.generation, generation)
        self.assertIs(failed.snapshot.phase, ManagedPhase.CLEANUP_PENDING)

        retried = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(retried, ReleaseDetached)
        self.assertEqual(manager.released, [("resource:request-a",), ("resource:request-a",)])

    def test_generation_failure_after_cleanup_does_not_cleanup_twice(self) -> None:
        issuer = SequenceIssuer(fail_on={2})
        manager = StubResourceManager()
        coordinator = self.make_coordinator(issuer=issuer, manager=manager)
        generation = coordinator.read().generation
        coordinator.acquire(expected_generation=generation, request="request-a")

        failed = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(failed, ReleaseFailed)
        self.assertEqual(manager.released, [("resource:request-a",)])

        retried = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(retried, ReleaseDetached)
        self.assertEqual(manager.released, [("resource:request-a",)])

    def test_calls_observing_acquiring_return_busy_without_waiting(self) -> None:
        entered = Event()
        continue_acquire = Event()

        def acquire(request: str) -> ResourceReady[str]:
            entered.set()
            self.assertTrue(continue_acquire.wait(timeout=2))
            return ResourceReady(("r1",))

        coordinator = self.make_coordinator(manager=StubResourceManager(acquire=acquire))
        generation = coordinator.read().generation
        result_holder: list[object] = []
        worker = Thread(
            target=lambda: result_holder.append(
                coordinator.acquire(
                    expected_generation=generation,
                    request="request-a",
                )
            )
        )
        worker.start()
        self.assertTrue(entered.wait(timeout=2))

        acquire_busy = coordinator.acquire(
            expected_generation=generation,
            request="request-a",
        )
        release_busy = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertEqual(acquire_busy, Busy(ManagedPhase.ACQUIRING))
        self.assertEqual(release_busy, Busy(ManagedPhase.ACQUIRING))

        continue_acquire.set()
        worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertIsInstance(result_holder[0], AcquireCommitted)

    def test_calls_observing_releasing_return_busy_without_waiting(self) -> None:
        entered = Event()
        continue_release = Event()

        def release(resources: tuple[str, ...]) -> None:
            entered.set()
            self.assertTrue(continue_release.wait(timeout=2))

        coordinator = self.make_coordinator(manager=StubResourceManager(release=release))
        generation = coordinator.read().generation
        coordinator.acquire(expected_generation=generation, request="request-a")
        result_holder: list[object] = []
        worker = Thread(
            target=lambda: result_holder.append(
                coordinator.release(
                    expected_generation=generation,
                    request="request-a",
                )
            )
        )
        worker.start()
        self.assertTrue(entered.wait(timeout=2))

        acquire_busy = coordinator.acquire(
            expected_generation=generation,
            request="request-a",
        )
        release_busy = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertEqual(acquire_busy, Busy(ManagedPhase.RELEASING))
        self.assertEqual(release_busy, Busy(ManagedPhase.RELEASING))

        continue_release.set()
        worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertIsInstance(result_holder[0], ReleaseDetached)


if __name__ == "__main__":
    unittest.main()
