from __future__ import annotations

import unittest
from collections.abc import Callable
from threading import Event, Thread

from _managed.coordinator import ManagedCoordinator
from _managed.result import (
    AcquireFailed,
    AcquireReleaseRequired,
    AcquireSucceeded,
    Busy,
    ReleaseFailed,
    ReleaseSucceeded,
)
from _managed.snapshot import ManagedPhase
from _resource.result import ResourceAcquireFailed, ResourceAcquireSucceeded


class LifecycleInterrupt(BaseException):
    pass


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


class BlockingSequenceIssuer:
    def __init__(self, entered: Event, continue_issue: Event) -> None:
        self._value = 0
        self._calls = 0
        self._entered = entered
        self._continue_issue = continue_issue

    def __call__(self) -> int:
        self._calls += 1
        if self._calls == 2:
            self._entered.set()
            if not self._continue_issue.wait(timeout=2):
                raise RuntimeError("generation issuance timed out")
        self._value += 1
        return self._value


class StubResourceProvider:
    def __init__(
        self,
        *,
        acquire: Callable[[str], object] | None = None,
        release: Callable[[tuple[str, ...]], None] | None = None,
    ) -> None:
        self._acquire = acquire or (
            lambda request: ResourceAcquireSucceeded((f"resource:{request}",))
        )
        self._release = release or (lambda resources: None)
        self.released: list[tuple[str, ...]] = []

    def acquire(self, request: str) -> object:
        return self._acquire(request)

    def release(self, resources: tuple[str, ...]) -> None:
        self.released.append(resources)
        self._release(resources)


class Projector:
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
        issuer: Callable[[], int] | None = None,
        provider: StubResourceProvider | None = None,
        projector: object | None = None,
    ) -> ManagedCoordinator[int, str, str, str]:
        return ManagedCoordinator(
            issuer or SequenceIssuer(),
            provider or StubResourceProvider(),
            projector or Projector(),
        )

    def test_successful_lifecycle_advances_generation_only_after_release(self) -> None:
        coordinator = self.make_coordinator()
        initial = coordinator.read()

        acquired = coordinator.acquire(
            expected_generation=initial.generation,
            request="request-a",
        )
        self.assertIsInstance(acquired, AcquireSucceeded)
        self.assertEqual(coordinator.read().generation, initial.generation)
        self.assertIs(coordinator.read().phase, ManagedPhase.ACTIVE)

        released = coordinator.release(
            expected_generation=initial.generation,
            request="request-a",
        )
        self.assertIsInstance(released, ReleaseSucceeded)
        self.assertGreater(released.next_generation, initial.generation)
        self.assertIs(coordinator.read().phase, ManagedPhase.IDLE)

    def test_failed_acquire_requires_release_even_when_no_resources_exist(self) -> None:
        provider = StubResourceProvider(
            acquire=lambda request: ResourceAcquireFailed(
                RuntimeError("acquire failure"), ()
            )
        )
        coordinator = self.make_coordinator(provider=provider)
        generation = coordinator.read().generation

        failed = coordinator.acquire(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(failed, AcquireFailed)
        self.assertIs(failed.snapshot.phase, ManagedPhase.RELEASE_PENDING)

        blocked = coordinator.acquire(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(blocked, AcquireReleaseRequired)

        released = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(released, ReleaseSucceeded)
        self.assertEqual(provider.released, [])

    def test_projection_failure_retains_resources_for_release(self) -> None:
        provider = StubResourceProvider(
            acquire=lambda request: ResourceAcquireSucceeded(("r1", "r2"))
        )
        coordinator = self.make_coordinator(provider=provider, projector=Projector(fail=True))
        generation = coordinator.read().generation

        failed = coordinator.acquire(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(failed, AcquireFailed)
        self.assertIs(failed.snapshot.phase, ManagedPhase.RELEASE_PENDING)

        released = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(released, ReleaseSucceeded)
        self.assertEqual(provider.released, [("r1", "r2")])

    def test_invalid_provider_error_preserves_valid_reported_resources(self) -> None:
        provider = StubResourceProvider(
            acquire=lambda request: ResourceAcquireFailed("invalid error", ("r1",))
        )
        coordinator = self.make_coordinator(provider=provider)
        generation = coordinator.read().generation

        failed = coordinator.acquire(generation, "request-a")

        self.assertIsInstance(failed, AcquireFailed)
        self.assertIsInstance(failed.snapshot.last_error, TypeError)
        released = coordinator.release(generation, "request-a")
        self.assertIsInstance(released, ReleaseSucceeded)
        self.assertEqual(provider.released, [("r1",)])

    def test_invalid_provider_resources_do_not_enter_lifecycle_state(self) -> None:
        provider = StubResourceProvider(
            acquire=lambda request: ResourceAcquireFailed(
                "invalid error",
                ["not-a-physical-resources-tuple"],
            )
        )
        coordinator = self.make_coordinator(provider=provider)
        generation = coordinator.read().generation

        failed = coordinator.acquire(generation, "request-a")

        self.assertIsInstance(failed, AcquireFailed)
        self.assertIsInstance(failed.snapshot.last_error, TypeError)
        self.assertIn("resources", str(failed.snapshot.last_error))
        released = coordinator.release(generation, "request-a")
        self.assertIsInstance(released, ReleaseSucceeded)
        self.assertEqual(provider.released, [])

    def test_release_cleanup_failure_preserves_generation_and_can_retry(self) -> None:
        attempts = 0

        def release(resources: tuple[str, ...]) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("cleanup failure")

        provider = StubResourceProvider(release=release)
        coordinator = self.make_coordinator(provider=provider)
        generation = coordinator.read().generation
        coordinator.acquire(expected_generation=generation, request="request-a")

        failed = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(failed, ReleaseFailed)
        self.assertEqual(failed.snapshot.generation, generation)
        self.assertIs(failed.snapshot.phase, ManagedPhase.RELEASE_PENDING)

        retried = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(retried, ReleaseSucceeded)
        self.assertEqual(
            provider.released,
            [("resource:request-a",), ("resource:request-a",)],
        )

    def test_generation_failure_after_cleanup_does_not_cleanup_twice(self) -> None:
        issuer = SequenceIssuer(fail_on={2})
        provider = StubResourceProvider()
        coordinator = self.make_coordinator(issuer=issuer, provider=provider)
        generation = coordinator.read().generation
        coordinator.acquire(expected_generation=generation, request="request-a")

        failed = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(failed, ReleaseFailed)
        self.assertEqual(provider.released, [("resource:request-a",)])

        retried = coordinator.release(
            expected_generation=generation,
            request="request-a",
        )
        self.assertIsInstance(retried, ReleaseSucceeded)
        self.assertEqual(provider.released, [("resource:request-a",)])

    def test_calls_observing_resource_acquisition_return_busy_without_waiting(self) -> None:
        entered = Event()
        continue_acquire = Event()

        def acquire(request: str) -> ResourceAcquireSucceeded[str]:
            entered.set()
            self.assertTrue(continue_acquire.wait(timeout=2))
            return ResourceAcquireSucceeded(("r1",))

        coordinator = self.make_coordinator(
            provider=StubResourceProvider(acquire=acquire)
        )
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
        self.assertIsInstance(result_holder[0], AcquireSucceeded)

    def test_calls_observing_projection_return_busy_without_waiting(self) -> None:
        entered = Event()
        continue_projection = Event()

        class BlockingProjector:
            def project(self, request: str, resources: tuple[str, ...]) -> str:
                entered.set()
                if not continue_projection.wait(timeout=2):
                    raise RuntimeError("projection timed out")
                return "capability"

        coordinator = self.make_coordinator(projector=BlockingProjector())
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

        self.assertEqual(
            coordinator.acquire(generation, "request-a"),
            Busy(ManagedPhase.ACQUIRING),
        )
        self.assertEqual(
            coordinator.release(generation, "request-a"),
            Busy(ManagedPhase.ACQUIRING),
        )

        continue_projection.set()
        worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertIsInstance(result_holder[0], AcquireSucceeded)

    def test_calls_observing_resource_cleanup_return_busy_without_waiting(self) -> None:
        entered = Event()
        continue_release = Event()

        def release(resources: tuple[str, ...]) -> None:
            entered.set()
            self.assertTrue(continue_release.wait(timeout=2))

        coordinator = self.make_coordinator(
            provider=StubResourceProvider(release=release)
        )
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
        self.assertIsInstance(result_holder[0], ReleaseSucceeded)

    def test_calls_observing_generation_issuance_return_busy_without_waiting(self) -> None:
        entered = Event()
        continue_issue = Event()
        issuer = BlockingSequenceIssuer(entered, continue_issue)
        coordinator = self.make_coordinator(issuer=issuer)
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

        self.assertEqual(
            coordinator.acquire(generation, "request-a"),
            Busy(ManagedPhase.RELEASING),
        )
        self.assertEqual(
            coordinator.release(generation, "request-a"),
            Busy(ManagedPhase.RELEASING),
        )

        continue_issue.set()
        worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertIsInstance(result_holder[0], ReleaseSucceeded)

    def test_acquire_interruption_is_saved_before_reraising(self) -> None:
        interruption = LifecycleInterrupt("acquire interrupted")

        def acquire(request: str) -> object:
            raise interruption

        coordinator = self.make_coordinator(
            provider=StubResourceProvider(acquire=acquire)
        )
        generation = coordinator.read().generation

        with self.assertRaises(LifecycleInterrupt) as raised:
            coordinator.acquire(generation, "request-a")

        self.assertIs(raised.exception, interruption)
        snapshot = coordinator.read()
        self.assertIs(snapshot.phase, ManagedPhase.RELEASE_PENDING)
        self.assertEqual(snapshot.generation, generation)
        self.assertEqual(snapshot.request, "request-a")
        self.assertIs(snapshot.last_error, interruption)

    def test_release_interruption_is_saved_before_reraising_and_can_retry(self) -> None:
        interruption = LifecycleInterrupt("release interrupted")
        attempts = 0

        def release(resources: tuple[str, ...]) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise interruption

        provider = StubResourceProvider(release=release)
        coordinator = self.make_coordinator(provider=provider)
        generation = coordinator.read().generation
        coordinator.acquire(generation, "request-a")

        with self.assertRaises(LifecycleInterrupt) as raised:
            coordinator.release(generation, "request-a")

        self.assertIs(raised.exception, interruption)
        snapshot = coordinator.read()
        self.assertIs(snapshot.phase, ManagedPhase.RELEASE_PENDING)
        self.assertEqual(snapshot.generation, generation)
        self.assertEqual(snapshot.request, "request-a")
        self.assertIs(snapshot.last_error, interruption)

        retried = coordinator.release(generation, "request-a")
        self.assertIsInstance(retried, ReleaseSucceeded)


if __name__ == "__main__":
    unittest.main()
