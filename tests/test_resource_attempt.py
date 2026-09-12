from __future__ import annotations

import gc
from dataclasses import dataclass
from threading import Event, Thread
from typing import Callable
from weakref import ref

import pytest

from _managed.coordinator import ManagedCoordinator
from _managed.result import (
    AcquireCommitted,
    AcquireSuperseded,
    ReleaseAcquisitionRevoked,
    ReleaseDetached,
)
from _resource.driver import PhysicalAcquired, PhysicalFailed, PhysicalInterrupted
from _resource.key import ResourceKey
from _resource.manager import ResourceAcquisitionCancelled, ResourceManager
from _resource.policy import ResourcePolicy
from _resource.pool import ResourceReservationTable
from _resource.result import ResourceFailed, ResourceReady


@dataclass(frozen=True)
class Requirement:
    name: str
    policy: ResourcePolicy = ResourcePolicy.BLOCKING


class RequirementsModel:
    def __init__(self, requirements: tuple[Requirement, ...]) -> None:
        self.value = requirements
        self.calls = 0

    def requirements(self, request: str) -> tuple[Requirement, ...]:
        self.calls += 1
        return self.value


class KeyModel:
    def key_for(self, requirement: Requirement) -> ResourceKey[str]:
        return ResourceKey("test", requirement.name)


class RejectingScheduler:
    def __init__(self) -> None:
        self.calls = 0

    def schedule(self, job: Callable[[], None]) -> bool:
        self.calls += 1
        return False


class QueuedScheduler:
    def __init__(self) -> None:
        self.jobs: list[Callable[[], None]] = []

    def schedule(self, job: Callable[[], None]) -> bool:
        self.jobs.append(job)
        return True


class ImmediatePhysical:
    def __init__(self, outcome: object) -> None:
        self.outcome = outcome
        self.interrupt_calls = 0

    def acquire(self):
        return self.outcome

    def interrupt(self) -> None:
        self.interrupt_calls += 1


class BlockingPhysical:
    def __init__(self, resources: tuple[str, ...] = ("partial",)) -> None:
        self.started = Event()
        self.interrupted = Event()
        self.resources = resources
        self.interrupt_calls = 0

    def acquire(self):
        self.started.set()
        assert self.interrupted.wait(2)
        return PhysicalInterrupted(self.resources)

    def interrupt(self) -> None:
        self.interrupt_calls += 1
        self.interrupted.set()


class Driver:
    def __init__(self, physicals: list[object]) -> None:
        self.physicals = physicals
        self.prepare_calls = 0
        self.cleanup_calls: list[tuple[str, ...]] = []
        self.cleanup_errors: list[Exception] = []

    def prepare(self, requirement: Requirement):
        physical = self.physicals[self.prepare_calls]
        self.prepare_calls += 1
        return physical

    def cleanup(self, resources: tuple[str, ...]) -> None:
        self.cleanup_calls.append(resources)
        if self.cleanup_errors:
            raise self.cleanup_errors.pop(0)


class Projection:
    def __init__(self, callback: Callable[[tuple[str, ...]], object]) -> None:
        self.callback = callback

    def project(self, request: str, resources: tuple[str, ...]):
        return self.callback(resources)


def manager_for(
    requirements: tuple[Requirement, ...],
    driver: Driver,
    scheduler: object,
):
    pool: ResourceReservationTable[str, Requirement] = ResourceReservationTable()
    manager = ResourceManager(
        RequirementsModel(requirements),
        KeyModel(),
        driver,
        resource_pool=pool,
        cleanup_scheduler=scheduler,
    )
    return manager, pool


def test_release_before_start_prevents_requirement_resolution_and_physical_io() -> None:
    requirements_model = RequirementsModel((Requirement("a"),))
    driver = Driver([ImmediatePhysical(PhysicalAcquired(("r",)))])
    pool: ResourceReservationTable[str, Requirement] = ResourceReservationTable()
    manager = ResourceManager(
        requirements_model,
        KeyModel(),
        driver,
        resource_pool=pool,
        cleanup_scheduler=RejectingScheduler(),
    )

    attempt = manager.open_attempt()
    attempt.release()
    result = attempt.acquire("request", lambda resources: resources)

    assert isinstance(result, ResourceFailed)
    assert isinstance(result.error, ResourceAcquisitionCancelled)
    assert requirements_model.calls == 0
    assert driver.prepare_calls == 0
    assert pool.reservations() == ()


def test_release_during_physical_acquire_interrupts_then_retires_for_cleanup() -> None:
    physical = BlockingPhysical()
    driver = Driver([physical])
    manager, pool = manager_for(
        (Requirement("a"),), driver, RejectingScheduler()
    )
    attempt = manager.open_attempt()
    box: list[object] = []

    worker = Thread(
        target=lambda: box.append(attempt.acquire("request", lambda resources: resources))
    )
    worker.start()
    assert physical.started.wait(2)

    attempt.release()
    worker.join(2)
    assert not worker.is_alive()

    assert physical.interrupt_calls == 1
    assert isinstance(box[0], ResourceFailed)
    assert len(pool.reservations()) == 1
    assert driver.cleanup_calls == []

    assert manager.cleanup_retired("request") is True
    assert driver.cleanup_calls == [("partial",)]
    assert pool.reservations() == ()


def test_release_during_projection_defers_cleanup_until_projection_returns() -> None:
    projection_started = Event()
    allow_projection_return = Event()
    scheduler = QueuedScheduler()
    driver = Driver([ImmediatePhysical(PhysicalAcquired(("resource",)))])
    manager, pool = manager_for((Requirement("a"),), driver, scheduler)
    attempt = manager.open_attempt()
    box: list[object] = []

    def project(resources: tuple[str, ...]):
        projection_started.set()
        assert allow_projection_return.wait(2)
        return resources

    worker = Thread(target=lambda: box.append(attempt.acquire("request", project)))
    worker.start()
    assert projection_started.wait(2)

    attempt.release()
    assert driver.cleanup_calls == []
    assert scheduler.jobs == []

    allow_projection_return.set()
    worker.join(2)
    assert not worker.is_alive()
    assert isinstance(box[0], ResourceFailed)
    assert len(scheduler.jobs) == 1
    assert driver.cleanup_calls == []
    assert len(pool.reservations()) == 1

    scheduler.jobs.pop()()
    assert driver.cleanup_calls == [("resource",)]
    assert pool.reservations() == ()


def test_partial_acquisition_failure_retains_every_created_resource_for_retry() -> None:
    failure = RuntimeError("second acquisition failed")
    driver = Driver(
        [
            ImmediatePhysical(PhysicalAcquired(("first",))),
            ImmediatePhysical(PhysicalFailed(failure, ("second-partial",))),
        ]
    )
    manager, pool = manager_for(
        (Requirement("a"), Requirement("b")),
        driver,
        RejectingScheduler(),
    )
    attempt = manager.open_attempt()

    result = attempt.acquire("request", lambda resources: resources)

    assert isinstance(result, ResourceFailed)
    assert result.error is failure
    assert len(pool.reservations()) == 1
    assert manager.cleanup_retired("request") is True
    assert driver.cleanup_calls == [("first", "second-partial")]
    assert pool.reservations() == ()


def test_cleanup_failure_keeps_attempt_and_reservation_for_later_retry() -> None:
    scheduler = RejectingScheduler()
    driver = Driver([ImmediatePhysical(PhysicalAcquired(("resource",)))])
    cleanup_error = RuntimeError("cleanup failed")
    driver.cleanup_errors.append(cleanup_error)
    manager, pool = manager_for((Requirement("a"),), driver, scheduler)
    attempt = manager.open_attempt()

    result = attempt.acquire("request", lambda resources: resources)
    assert isinstance(result, ResourceReady)
    attempt.release()

    with pytest.raises(RuntimeError, match="cleanup failed"):
        manager.cleanup_retired("request")

    assert len(pool.reservations()) == 1
    assert manager.cleanup_retired("request") is True
    assert driver.cleanup_calls == [("resource",), ("resource",)]
    assert pool.reservations() == ()


def test_cleanup_claim_prevents_manual_retry_from_overlapping_scheduled_cleanup() -> None:
    scheduler = QueuedScheduler()
    driver = Driver([ImmediatePhysical(PhysicalAcquired(("resource",)))])
    manager, pool = manager_for((Requirement("a"),), driver, scheduler)
    attempt = manager.open_attempt()

    assert isinstance(attempt.acquire("request", lambda resources: resources), ResourceReady)
    attempt.release()
    assert len(scheduler.jobs) == 1

    # The scheduled job owns the cleanup claim, so a manual recovery pass observes the
    # retired attempt but must not run cleanup concurrently.
    assert manager.cleanup_retired("request") is True
    assert driver.cleanup_calls == []
    assert len(pool.reservations()) == 1

    scheduler.jobs.pop()()
    assert driver.cleanup_calls == [("resource",)]
    assert pool.reservations() == ()


def test_manager_holds_retired_attempt_after_managed_reference_is_dropped() -> None:
    scheduler = RejectingScheduler()
    driver = Driver([ImmediatePhysical(PhysicalAcquired(("resource",)))])
    manager, pool = manager_for((Requirement("a"),), driver, scheduler)
    attempt = manager.open_attempt()
    attempt_ref = ref(attempt)

    assert isinstance(attempt.acquire("request", lambda resources: resources), ResourceReady)
    attempt.release()
    del attempt
    gc.collect()

    assert attempt_ref() is not None
    assert len(pool.reservations()) == 1
    assert manager.cleanup_retired("request") is True
    assert pool.reservations() == ()


def test_managed_release_during_projection_supersedes_commit() -> None:
    projection_started = Event()
    allow_projection_return = Event()
    scheduler = QueuedScheduler()
    driver = Driver([ImmediatePhysical(PhysicalAcquired(("resource",)))])
    manager, _ = manager_for((Requirement("a"),), driver, scheduler)

    generations = iter((1, 2, 3))
    coordinator = ManagedCoordinator(
        lambda: next(generations),
        manager,
        Projection(
            lambda resources: (
                projection_started.set(),
                allow_projection_return.wait(2),
                resources,
            )[-1]
        ),
    )
    initial = coordinator.read()
    box: list[object] = []

    worker = Thread(target=lambda: box.append(coordinator.acquire(initial.generation, "request")))
    worker.start()
    assert projection_started.wait(2)

    released = coordinator.release(initial.generation, "request")
    assert isinstance(released, ReleaseAcquisitionRevoked)

    allow_projection_return.set()
    worker.join(2)
    assert not worker.is_alive()
    assert isinstance(box[0], AcquireSuperseded)


def test_pool_rejects_duplicate_claim_keys() -> None:
    driver = Driver(
        [
            ImmediatePhysical(PhysicalAcquired(("one",))),
            ImmediatePhysical(PhysicalAcquired(("two",))),
        ]
    )
    manager, pool = manager_for(
        (Requirement("same"), Requirement("same")),
        driver,
        RejectingScheduler(),
    )
    attempt = manager.open_attempt()

    with pytest.raises(ValueError, match="duplicate resource keys"):
        attempt.acquire("request", lambda resources: resources)

    assert pool.reservations() == ()
    assert driver.prepare_calls == 0


def test_pool_conflict_policy_remains_directional() -> None:
    non_blocking = Requirement("same", ResourcePolicy.NON_BLOCKING)
    blocking = Requirement("same", ResourcePolicy.BLOCKING)
    key = ResourceKey("test", "same")

    pool: ResourceReservationTable[str, Requirement] = ResourceReservationTable()
    from _attempt import AttemptId

    first = AttemptId()
    second = AttemptId()
    assert pool.reserve(first, "first", (non_blocking,), (key,)) is True
    # Incoming BLOCKING does not retroactively make the existing NON_BLOCKING claim block.
    assert pool.reserve(second, "second", (blocking,), (key,)) is True

    other_pool: ResourceReservationTable[str, Requirement] = ResourceReservationTable()
    third = AttemptId()
    fourth = AttemptId()
    assert other_pool.reserve(third, "third", (blocking,), (key,)) is True
    assert other_pool.reserve(fourth, "fourth", (non_blocking,), (key,)) is False


def test_managed_successful_commit_and_release_uses_attempt_handle() -> None:
    scheduler = RejectingScheduler()
    driver = Driver([ImmediatePhysical(PhysicalAcquired(("resource",)))])
    manager, pool = manager_for((Requirement("a"),), driver, scheduler)

    generations = iter((1, 2, 3))
    coordinator = ManagedCoordinator(
        lambda: next(generations),
        manager,
        Projection(lambda resources: ("capability", resources)),
    )
    initial = coordinator.read()

    acquired = coordinator.acquire(initial.generation, "request")
    assert isinstance(acquired, AcquireCommitted)
    assert acquired.snapshot.capability == ("capability", ("resource",))

    released = coordinator.release(initial.generation, "request")
    assert isinstance(released, ReleaseDetached)
    assert len(pool.reservations()) == 1
    assert manager.cleanup_retired("request") is True
    assert pool.reservations() == ()
