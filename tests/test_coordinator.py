from __future__ import annotations

import threading
import unittest

from lifecycle.capability import (
    CapabilityLifecycleCoordinator,
    LifecycleOutcome,
    LifecyclePhase,
)
from lifecycle.resource import Resource, ResourceRecoveryPool, ResourceScope, ScopeClosedError
from tests.support import ThreadCall, wait


class ControlFlow(BaseException):
    pass


class CoordinatorTests(unittest.TestCase):
    def test_generation_and_not_executed_rules(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        coordinator = CapabilityLifecycleCoordinator(
            lambda request: Resource.pure(request.upper()),
            scope=scope,
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
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
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

        coordinator = CapabilityLifecycleCoordinator(factory, scope=scope)
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
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        acquire_started = threading.Event()
        allow_acquire = threading.Event()

        def acquire_value() -> str:
            acquire_started.set()
            wait(allow_acquire)
            return "capability"

        coordinator = CapabilityLifecycleCoordinator(
            lambda _request: Resource.make(acquire_value, lambda _value: None),
            scope=scope,
        )

        self.addCleanup(allow_acquire.set)
        operation = ThreadCall(lambda: coordinator.acquire(0, "a"))
        wait(acquire_started)
        self.assertEqual(coordinator.read().phase, LifecyclePhase.ACQUIRING)

        busy_acquire = coordinator.acquire(0, "b")
        busy_release = coordinator.release(0, "a")
        self.assertEqual(busy_acquire.outcome, LifecycleOutcome.NOT_EXECUTED)
        self.assertEqual(busy_release.outcome, LifecycleOutcome.NOT_EXECUTED)

        allow_acquire.set()
        self.assertEqual(operation.succeeded(self).outcome, LifecycleOutcome.ACQUIRE_SUCCEEDED)

    def test_cleanup_failure_is_pooled_but_release_completes_and_next_generation_runs(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        acquired = 0

        def factory(request: str) -> Resource[str]:
            def acquire() -> str:
                nonlocal acquired
                acquired += 1
                return f"{request}-{acquired}"

            def release(_value: str) -> None:
                raise OSError("cleanup failed")

            return Resource.make(acquire, release)

        coordinator = CapabilityLifecycleCoordinator(factory, scope=scope)
        coordinator.acquire(0, "first")
        released = coordinator.release(0, "first")

        self.assertEqual(released.outcome, LifecycleOutcome.RELEASE_COMPLETED)
        self.assertEqual(released.snapshot.generation, 1)
        self.assertEqual(released.diagnostics.release_report.pooled_count, 1)
        self.assertEqual(len(pool), 1)

        next_result = coordinator.acquire(1, "second")
        self.assertEqual(next_result.outcome, LifecycleOutcome.ACQUIRE_SUCCEEDED)
        self.assertEqual(next_result.snapshot.capability, "second-2")

    def test_child_coordinator_is_bound_to_the_parent_activation_that_created_it(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        events: list[str] = []
        physical_child_acquires = 0

        def parent_factory(request: str) -> Resource[str]:
            return Resource.make(
                lambda: f"server:{request}",
                lambda value: events.append(f"release-{value}"),
            )

        parent = CapabilityLifecycleCoordinator(parent_factory, scope=scope)
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
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        coordinator = CapabilityLifecycleCoordinator(
            lambda request: Resource.pure(request),
            scope=scope,
        )
        factory = lambda parent, child: Resource.pure((parent, child))

        with self.assertRaises(ValueError):
            coordinator.create_child(0, factory)

        coordinator.acquire(0, "parent")
        with self.assertRaises(ValueError):
            coordinator.create_child(1, factory)

    def test_closed_owner_rejects_acquire_before_invoking_factory(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        scope.close()
        coordinator = CapabilityLifecycleCoordinator(
            lambda _: self.fail("closed owner must not run factory"), scope=scope
        )
        result = coordinator.acquire(0, "request")
        self.assertEqual(result.outcome, LifecycleOutcome.ACQUIRE_FAILED)
        self.assertIsInstance(result.diagnostics.acquire_error, ScopeClosedError)
        self.assertEqual(result.snapshot.generation, 1)
        self.assertEqual(coordinator.acquire(0, "request").outcome, LifecycleOutcome.NOT_EXECUTED)

    def test_invalid_factory_and_none_capability_fail_with_cleanup_diagnostics(self) -> None:
        factory_error = ValueError("factory")
        cleanup_error = OSError("cleanup")

        def fail(error: BaseException) -> None:
            raise error

        factories = (
            (lambda _: fail(factory_error), ValueError, ()),
            (lambda _: object(), TypeError, ()),
            (lambda _: Resource.make(lambda: None, lambda _: fail(cleanup_error)), TypeError, (cleanup_error,)),
        )
        for factory, error_type, cleanup_errors in factories:
            with self.subTest(factory=factory):
                pool = ResourceRecoveryPool()
                scope = ResourceScope(pool)
                coordinator = CapabilityLifecycleCoordinator(factory, scope=scope)
                result = coordinator.acquire(0, "request")
                self.assertEqual(result.outcome, LifecycleOutcome.ACQUIRE_FAILED)
                self.assertEqual(result.snapshot.generation, 1)
                self.assertEqual(result.snapshot.phase, LifecyclePhase.IDLE)
                self.assertIsInstance(result.diagnostics.acquire_error, error_type)
                self.assertEqual(result.diagnostics.release_report.errors, cleanup_errors)
                self.assertEqual(result.diagnostics.release_report.pooled_count, len(cleanup_errors))
                self.assertEqual(len(pool), len(cleanup_errors))
                self.assertEqual(scope.close().errors, ())

    def test_child_can_release_and_reacquire_without_stopping_parent_or_sibling(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        events: list[str] = []
        parent = CapabilityLifecycleCoordinator(
            lambda _: Resource.make(lambda: "server", events.append), scope=scope
        )
        parent.acquire(0, "request")

        def factory(server: str, name: str) -> Resource[str]:
            return Resource.make(lambda: f"{server}/{name}", events.append)

        first = parent.create_child(0, factory)
        second = parent.create_child(0, factory)
        first.acquire(0, "first")
        second.acquire(0, "second")
        first.release(0, "first")
        self.assertEqual(parent.read().capability, "server")
        self.assertEqual(second.read().capability, "server/second")
        self.assertEqual(first.acquire(1, "new-first").snapshot.capability, "server/new-first")
        scope.close()
        self.assertEqual(events, ["server/first", "server/new-first", "server/second", "server"])
        self.assertEqual(first.read().generation, 2)
        self.assertEqual(second.read().generation, 1)

    def test_release_control_flow_interruption_resets_generation_after_cleanup(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)

        def release(_value: str) -> None:
            raise ControlFlow("stop")

        coordinator = CapabilityLifecycleCoordinator(
            lambda _request: Resource.make(lambda: "capability", release),
            scope=scope,
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
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
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
            scope=scope,
        )

        with self.assertRaises(ControlFlow):
            coordinator.acquire(0, "request")

        snapshot = coordinator.read()
        self.assertEqual(snapshot.phase, LifecyclePhase.IDLE)
        self.assertEqual(snapshot.generation, 1)
        self.assertEqual(events, ["release-outer"])


if __name__ == "__main__":
    unittest.main()
