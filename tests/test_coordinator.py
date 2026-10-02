from __future__ import annotations

import threading
import time
import unittest

from lifecycle.capability import (
    CapabilityLifecycleCoordinator,
    LifecycleOutcome,
    LifecyclePhase,
)
from lifecycle.resource import Resource, ResourceRecoveryPool


class ControlFlow(BaseException):
    pass


class CoordinatorTests(unittest.TestCase):
    def test_generation_and_not_executed_rules(self) -> None:
        pool = ResourceRecoveryPool()
        coordinator = CapabilityLifecycleCoordinator(
            lambda request: Resource.pure(request.upper()),
            pool,
        )

        stale = coordinator.acquire(1, "a")
        self.assertEqual(stale.outcome, LifecycleOutcome.NOT_EXECUTED)
        self.assertEqual(stale.snapshot.generation, 0)

        acquired = coordinator.acquire(0, "a")
        self.assertEqual(acquired.outcome, LifecycleOutcome.ACQUIRE_SUCCEEDED)
        self.assertEqual(acquired.snapshot.generation, 0)
        self.assertEqual(acquired.snapshot.capability, "A")

        wrong_request = coordinator.release(0, "b")
        self.assertEqual(wrong_request.outcome, LifecycleOutcome.NOT_EXECUTED)
        self.assertEqual(wrong_request.snapshot.generation, 0)

        released = coordinator.release(0, "a")
        self.assertEqual(released.outcome, LifecycleOutcome.RELEASE_COMPLETED)
        self.assertEqual(released.snapshot.generation, 1)
        self.assertEqual(released.snapshot.phase, LifecyclePhase.IDLE)

    def test_started_acquire_failure_rolls_back_and_advances_generation(self) -> None:
        pool = ResourceRecoveryPool()
        events: list[str] = []

        def factory(_request: str) -> Resource[str]:
            outer = Resource.make(
                lambda: events.append("acquire-server") or "server",
                lambda _value: events.append("release-server"),
            )

            def fail() -> str:
                raise ValueError("connection failed")

            return outer.flat_map(
                lambda _server: Resource.make(fail, lambda _value: None)
            )

        coordinator = CapabilityLifecycleCoordinator(factory, pool)
        result = coordinator.acquire(0, "request")

        self.assertEqual(result.outcome, LifecycleOutcome.ACQUIRE_FAILED)
        self.assertEqual(result.snapshot.generation, 1)
        self.assertIsInstance(result.diagnostics.acquire_error, ValueError)
        self.assertIsNotNone(result.diagnostics.release_report)
        self.assertEqual(events, ["acquire-server", "release-server"])

        stale = coordinator.acquire(0, "request")
        self.assertEqual(stale.outcome, LifecycleOutcome.NOT_EXECUTED)
        self.assertEqual(stale.snapshot.generation, 1)

    def test_busy_states_do_not_execute_other_operations(self) -> None:
        pool = ResourceRecoveryPool()
        acquire_started = threading.Event()
        allow_acquire = threading.Event()
        result_holder = []

        def acquire_value() -> str:
            acquire_started.set()
            allow_acquire.wait(2)
            return "capability"

        coordinator = CapabilityLifecycleCoordinator(
            lambda _request: Resource.make(acquire_value, lambda _value: None),
            pool,
        )

        thread = threading.Thread(
            target=lambda: result_holder.append(coordinator.acquire(0, "a"))
        )
        thread.start()
        self.assertTrue(acquire_started.wait(2))
        self.assertEqual(coordinator.read().phase, LifecyclePhase.ACQUIRING)

        busy_acquire = coordinator.acquire(0, "b")
        busy_release = coordinator.release(0, "a")
        self.assertEqual(busy_acquire.outcome, LifecycleOutcome.NOT_EXECUTED)
        self.assertEqual(busy_release.outcome, LifecycleOutcome.NOT_EXECUTED)

        allow_acquire.set()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result_holder[0].outcome, LifecycleOutcome.ACQUIRE_SUCCEEDED)

    def test_cleanup_failure_is_pooled_but_release_completes_and_next_generation_runs(self) -> None:
        pool = ResourceRecoveryPool()
        acquired = 0

        def factory(request: str) -> Resource[str]:
            def acquire() -> str:
                nonlocal acquired
                acquired += 1
                return f"{request}-{acquired}"

            def release(_value: str) -> None:
                raise OSError("cleanup failed")

            return Resource.make(acquire, release)

        coordinator = CapabilityLifecycleCoordinator(factory, pool)
        coordinator.acquire(0, "first")
        released = coordinator.release(0, "first")

        self.assertEqual(released.outcome, LifecycleOutcome.RELEASE_COMPLETED)
        self.assertEqual(released.snapshot.generation, 1)
        self.assertEqual(released.diagnostics.release_report.pooled_count, 1)
        self.assertEqual(len(pool), 1)

        next_result = coordinator.acquire(1, "second")
        self.assertEqual(next_result.outcome, LifecycleOutcome.ACQUIRE_SUCCEEDED)
        self.assertEqual(next_result.snapshot.capability, "second-2")

    def test_child_coordinator_is_bound_to_the_parent_handle_that_created_it(self) -> None:
        pool = ResourceRecoveryPool()
        events: list[str] = []
        physical_child_acquires = 0

        def parent_factory(request: str) -> Resource[str]:
            return Resource.make(
                lambda: f"server:{request}",
                lambda value: events.append(f"release-{value}"),
            )

        parent = CapabilityLifecycleCoordinator(parent_factory, pool)
        parent.acquire(0, "one")

        def child_factory(parent_capability: str, request: str) -> Resource[str]:
            def acquire() -> str:
                nonlocal physical_child_acquires
                physical_child_acquires += 1
                return f"{parent_capability}/{request}"

            return Resource.make(
                acquire,
                lambda value: events.append(f"release-{value}"),
            )

        child = parent.create_child(0, child_factory)
        child_result = child.acquire(0, "connection")
        self.assertEqual(
            child_result.snapshot.capability,
            "server:one/connection",
        )
        self.assertEqual(physical_child_acquires, 1)

        parent.release(0, "one")
        self.assertEqual(
            events,
            ["release-server:one/connection", "release-server:one"],
        )

        # Parent-driven release is reconciled lazily by the child coordinator.
        child_snapshot = child.read()
        self.assertEqual(child_snapshot.phase, LifecyclePhase.IDLE)
        self.assertEqual(child_snapshot.generation, 1)

        parent.acquire(1, "two")
        old_child = child.acquire(1, "another")
        self.assertEqual(old_child.outcome, LifecycleOutcome.ACQUIRE_FAILED)
        self.assertEqual(old_child.snapshot.generation, 2)
        self.assertEqual(physical_child_acquires, 1)

        replacement = parent.create_child(1, child_factory)
        replacement_result = replacement.acquire(0, "connection")
        self.assertEqual(
            replacement_result.snapshot.capability,
            "server:two/connection",
        )
        self.assertEqual(physical_child_acquires, 2)

    def test_create_child_requires_matching_active_generation(self) -> None:
        pool = ResourceRecoveryPool()
        coordinator = CapabilityLifecycleCoordinator(
            lambda request: Resource.pure(request),
            pool,
        )
        factory = lambda parent, child: Resource.pure((parent, child))

        with self.assertRaises(ValueError):
            coordinator.create_child(0, factory)

        coordinator.acquire(0, "parent")
        with self.assertRaises(ValueError):
            coordinator.create_child(1, factory)

    def test_release_control_flow_interruption_resets_generation_after_cleanup(self) -> None:
        pool = ResourceRecoveryPool()

        def release(_value: str) -> None:
            raise ControlFlow("stop")

        coordinator = CapabilityLifecycleCoordinator(
            lambda _request: Resource.make(lambda: "capability", release),
            pool,
        )
        coordinator.acquire(0, "request")

        with self.assertRaises(ControlFlow):
            coordinator.release(0, "request")

        snapshot = coordinator.read()
        self.assertEqual(snapshot.phase, LifecyclePhase.IDLE)
        self.assertEqual(snapshot.generation, 1)
        self.assertEqual(len(pool), 1)

    def test_acquire_control_flow_interruption_resets_generation_after_rollback(self) -> None:
        pool = ResourceRecoveryPool()
        events: list[str] = []

        outer = Resource.make(
            lambda: "outer",
            lambda _value: events.append("release-outer"),
        )

        def interrupt() -> str:
            raise ControlFlow("stop")

        coordinator = CapabilityLifecycleCoordinator(
            lambda _request: outer.flat_map(
                lambda _value: Resource.make(interrupt, lambda _value: None)
            ),
            pool,
        )

        with self.assertRaises(ControlFlow):
            coordinator.acquire(0, "request")

        snapshot = coordinator.read()
        self.assertEqual(snapshot.phase, LifecyclePhase.IDLE)
        self.assertEqual(snapshot.generation, 1)
        self.assertEqual(events, ["release-outer"])


if __name__ == "__main__":
    unittest.main()
