from __future__ import annotations

from dataclasses import dataclass
from itertools import count
import unittest

from _managed import (
    AcquireBusy,
    ManagedCoordinator,
    ReleaseDetached,
    ResourceCleanupPendingError,
)
from _resource.driver import PhysicalAcquired, PhysicalFailed
from _resource.key import ResourceKey
from _resource.manager import ResourceManager
from _resource.policy import ResourcePolicy
from _resource.pool import ResourceReservationTable


@dataclass(frozen=True)
class Requirement:
    name: str
    policy: ResourcePolicy = ResourcePolicy.BLOCKING


class RequirementsModel:
    def requirements(self, request: str) -> tuple[Requirement, ...]:
        return (Requirement(request),)


class KeyModel:
    def key_for(self, requirement: Requirement) -> ResourceKey[str]:
        return ResourceKey("test", requirement.name)


class Projection:
    def project(self, request: str, resources: tuple[str, ...]) -> tuple[str, ...]:
        return resources


class RaisingProjection:
    def project(self, request: str, resources: tuple[str, ...]) -> object:
        raise ValueError("projection failed")


class FailAcquireAndCleanupOnceDriver:
    def __init__(self) -> None:
        self.cleanup_calls = 0

    def acquire(self, requirement: Requirement) -> PhysicalFailed[str]:
        return PhysicalFailed(RuntimeError("acquire failed"), ("resource",))

    def cleanup(self, resources: tuple[str, ...]) -> None:
        self.cleanup_calls += 1
        if self.cleanup_calls == 1:
            raise OSError("cleanup failed")


class AcquireAndCleanupOnceFailingDriver:
    def __init__(self) -> None:
        self.cleanup_calls = 0

    def acquire(self, requirement: Requirement) -> PhysicalAcquired[str]:
        return PhysicalAcquired(("resource",))

    def cleanup(self, resources: tuple[str, ...]) -> None:
        self.cleanup_calls += 1
        if self.cleanup_calls == 1:
            raise OSError("cleanup failed")


class RecordingDriver:
    def __init__(self) -> None:
        self.cleanup_calls: list[tuple[str, ...]] = []

    def acquire(self, requirement: Requirement) -> PhysicalAcquired[str]:
        return PhysicalAcquired(("resource",))

    def cleanup(self, resources: tuple[str, ...]) -> None:
        self.cleanup_calls.append(resources)


class ReleaseFailsOnceReservationTable(ResourceReservationTable[str, Requirement]):
    def __init__(self) -> None:
        super().__init__()
        self.release_calls = 0

    def release_reservation(self, attempt) -> None:  # type: ignore[no-untyped-def]
        self.release_calls += 1
        if self.release_calls == 1:
            raise RuntimeError("reservation release failed")
        super().release_reservation(attempt)


class BadAttempt:
    def __init__(self) -> None:
        self.release_calls = 0

    @property
    def cleanup_pending(self) -> bool:
        return False

    def acquire(self, request, use_resources):  # type: ignore[no-untyped-def]
        return object()

    def release(self) -> None:
        self.release_calls += 1


class BadManagement:
    def __init__(self) -> None:
        self.attempt = BadAttempt()

    def open_attempt(self) -> BadAttempt:
        return self.attempt


class SyncLifecycleRecoveryTests(unittest.TestCase):
    def _coordinator(self, driver, projection=None, table=None):  # type: ignore[no-untyped-def]
        generations = count(1)
        manager = ResourceManager(
            RequirementsModel(),
            KeyModel(),
            driver,
            resource_pool=table if table is not None else ResourceReservationTable(),
        )
        return ManagedCoordinator(
            generations.__next__,
            manager,
            projection or Projection(),
        )

    def test_acquire_rollback_cleanup_failure_is_machine_readable(self) -> None:
        driver = FailAcquireAndCleanupOnceDriver()
        coordinator = self._coordinator(driver)
        generation = coordinator.read().generation

        with self.assertRaises(ResourceCleanupPendingError) as raised:
            coordinator.acquire(generation, "camera")

        self.assertIsInstance(raised.exception.primary_error, RuntimeError)
        self.assertIsInstance(raised.exception.cleanup_error, OSError)
        self.assertIsInstance(coordinator.acquire(generation, "camera"), AcquireBusy)

        released = coordinator.release(generation, "camera")
        self.assertIsInstance(released, ReleaseDetached)
        self.assertEqual(driver.cleanup_calls, 2)

    def test_projection_failure_with_cleanup_failure_is_machine_readable(self) -> None:
        driver = AcquireAndCleanupOnceFailingDriver()
        coordinator = self._coordinator(driver, RaisingProjection())
        generation = coordinator.read().generation

        with self.assertRaises(ResourceCleanupPendingError) as raised:
            coordinator.acquire(generation, "camera")

        self.assertIsInstance(raised.exception.primary_error, ValueError)
        self.assertIsInstance(raised.exception.cleanup_error, OSError)
        self.assertIsInstance(coordinator.release(generation, "camera"), ReleaseDetached)
        self.assertEqual(driver.cleanup_calls, 2)

    def test_reservation_release_retry_does_not_reclean_resources(self) -> None:
        driver = RecordingDriver()
        table = ReleaseFailsOnceReservationTable()
        coordinator = self._coordinator(driver, table=table)
        generation = coordinator.read().generation
        coordinator.acquire(generation, "camera")

        with self.assertRaisesRegex(RuntimeError, "reservation release failed"):
            coordinator.release(generation, "camera")

        self.assertEqual(driver.cleanup_calls, [("resource",)])
        self.assertIsInstance(coordinator.release(generation, "camera"), ReleaseDetached)
        self.assertEqual(driver.cleanup_calls, [("resource",)])
        self.assertEqual(table.release_calls, 2)

    def test_invalid_resource_result_releases_uncommitted_attempt(self) -> None:
        generations = count(1)
        management = BadManagement()
        coordinator = ManagedCoordinator(
            generations.__next__,
            management,
            Projection(),
        )
        generation = coordinator.read().generation

        with self.assertRaisesRegex(RuntimeError, "unsupported ResourceManagement"):
            coordinator.acquire(generation, "camera")

        self.assertEqual(management.attempt.release_calls, 1)


if __name__ == "__main__":
    unittest.main()
