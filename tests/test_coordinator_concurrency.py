from __future__ import annotations

import threading
import unittest
from unittest.mock import patch

from lifecycle.capability import CapabilityLifecycleCoordinator, LifecycleOutcome, LifecyclePhase
from lifecycle.resource import Resource, ResourceRecoveryPool, ResourceScope, ScopeClosedError
from tests.support import ThreadCall, wait, wait_closing


class ControlFlow(BaseException):
    pass


def fail(error: BaseException) -> None:
    raise error


class CoordinatorConcurrencyTests(unittest.TestCase):
    def test_publication_before_shutdown_succeeds_then_reads_releasing_and_idle(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        releasing = threading.Event()
        proceed = threading.Event()
        self.addCleanup(proceed.set)

        def release(_: str) -> None:
            releasing.set()
            wait(proceed)

        coordinator = CapabilityLifecycleCoordinator(
            lambda _: Resource.make(lambda: "value", release), scope=scope
        )
        acquired = coordinator.acquire(0, "request")
        self.assertEqual(acquired.outcome, LifecycleOutcome.ACQUIRE_SUCCEEDED)
        self.assertEqual(acquired.snapshot.phase, LifecyclePhase.ACTIVE)
        closing = ThreadCall(scope.close)
        wait(releasing)
        snapshot = coordinator.read()
        self.assertEqual(snapshot.phase, LifecyclePhase.RELEASING)
        self.assertEqual(snapshot.generation, 0)
        self.assertIsNone(snapshot.capability)
        self.assertEqual(coordinator.release(0, "request").outcome, LifecycleOutcome.NOT_EXECUTED)
        proceed.set()
        closing.succeeded(self)
        for _ in range(2):
            self.assertEqual(coordinator.read().phase, LifecyclePhase.IDLE)
            self.assertEqual(coordinator.read().generation, 1)
        self.assertEqual(acquired.snapshot.capability, "value")

    def test_shutdown_between_batch_commit_and_publication_fails_without_double_generation(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        committed = threading.Event()
        publish = threading.Event()
        releasing = threading.Event()
        finish_release = threading.Event()
        self.addCleanup(publish.set)
        self.addCleanup(finish_release.set)
        cleanup_error = OSError("cleanup")

        def release(_: str) -> None:
            releasing.set()
            wait(finish_release)
            raise cleanup_error

        coordinator = CapabilityLifecycleCoordinator(
            lambda _: Resource.make(lambda: "value", release), scope=scope
        )
        original_acquire = ResourceScope.acquire

        def pause_after_commit(activation, resource):
            value = original_acquire(activation, resource)
            committed.set()
            wait(publish)
            return value

        with patch.object(ResourceScope, "acquire", pause_after_commit):
            acquisition = ThreadCall(lambda: coordinator.acquire(0, "request"))
            wait(committed)
            closing = ThreadCall(scope.close)
            wait(releasing)
            snapshot = coordinator.read()
            self.assertEqual(snapshot.phase, LifecyclePhase.RELEASING)
            self.assertIsNone(snapshot.capability)
            finish_release.set()
            close_report = closing.succeeded(self)
            self.assertEqual(coordinator.read().generation, 1)
            self.assertEqual(coordinator.read().phase, LifecyclePhase.IDLE)
            publish.set()
            result = acquisition.succeeded(self)
        self.assertEqual(result.outcome, LifecycleOutcome.ACQUIRE_FAILED)
        self.assertEqual(result.snapshot.generation, 1)
        self.assertEqual(result.snapshot.phase, LifecyclePhase.IDLE)
        self.assertIsInstance(result.diagnostics.acquire_error, ScopeClosedError)
        self.assertEqual(result.diagnostics.release_report.errors, (cleanup_error,))
        self.assertEqual(close_report.errors, (cleanup_error,))
        self.assertEqual(len(pool), 1)

    def test_shutdown_waits_for_child_factory_and_rollback_before_parent_release(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        events: list[str] = []
        factory_started = threading.Event()
        proceed = threading.Event()
        self.addCleanup(proceed.set)
        rollback_error = OSError("child rollback")
        parent_error = OSError("parent cleanup")

        def release_parent(value: str) -> None:
            events.append(value)
            raise parent_error

        parent = CapabilityLifecycleCoordinator(
            lambda _: Resource.make(lambda: "parent", release_parent), scope=scope
        )
        parent.acquire(0, "server")

        def child_factory(parent_value: str, request: str) -> Resource[str]:
            self.assertEqual(parent_value, "parent")
            factory_started.set()
            wait(proceed)

            def rollback(value: str) -> None:
                events.append(value)
                raise rollback_error

            return Resource.make(lambda: request, rollback)

        child = parent.create_child(0, child_factory)
        acquisition = ThreadCall(lambda: child.acquire(0, "child"))
        wait(factory_started)
        closing = ThreadCall(scope.close)
        wait_closing(scope)
        self.assertFalse(closing.done.is_set())
        self.assertEqual(events, [])
        self.assertEqual(parent.read().phase, LifecyclePhase.RELEASING)
        self.assertEqual(child.read().phase, LifecyclePhase.RELEASING)
        proceed.set()
        result = acquisition.succeeded(self)
        report = closing.succeeded(self)
        self.assertEqual(result.outcome, LifecycleOutcome.ACQUIRE_FAILED)
        self.assertIsInstance(result.diagnostics.acquire_error, ScopeClosedError)
        self.assertEqual(result.diagnostics.release_report.errors, (rollback_error,))
        self.assertEqual(report.errors, (rollback_error, parent_error))
        self.assertEqual(report.pooled_count, 2)
        self.assertEqual(len(pool), 2)
        self.assertEqual(events, ["child", "parent"])
        self.assertEqual(child.read().generation, 1)
        self.assertEqual(parent.read().generation, 1)

    def test_ancestor_close_collects_failed_activation_detached_before_parent_is_visited(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        parent = CapabilityLifecycleCoordinator(lambda _: Resource.pure("parent"), scope=scope)
        parent.acquire(0, "request")
        started = threading.Event()
        proceed = threading.Event()
        blocked = threading.Event()
        unblock = threading.Event()
        self.addCleanup(proceed.set)
        self.addCleanup(unblock.set)
        cleanup_error = OSError("rollback")
        acquire_error = ValueError("acquire")

        def project(_: str) -> str:
            started.set()
            wait(proceed)
            raise acquire_error

        child = parent.create_child(
            0, lambda _parent, _: Resource.make(
                lambda: "child", lambda _: fail(cleanup_error)
            ).map(project)
        )

        def block(_: str) -> None:
            blocked.set()
            wait(unblock)

        scope.child().acquire(Resource.make(lambda: "blocker", block))
        acquisition = ThreadCall(lambda: child.acquire(0, "request"))
        wait(started)
        closing = ThreadCall(scope.close)
        wait(blocked)
        proceed.set()
        result = acquisition.succeeded(self)
        self.assertIs(result.diagnostics.acquire_error, acquire_error)
        self.assertEqual(result.diagnostics.release_report.errors, (cleanup_error,))
        self.assertFalse(closing.done.is_set())
        unblock.set()
        report = closing.succeeded(self)
        self.assertEqual(report.errors, (cleanup_error,))
        self.assertEqual(report.pooled_count, 1)
        self.assertEqual(len(pool), 1)

    def test_old_release_completion_cannot_retire_the_next_activation(self) -> None:
        for interrupted in (False, True):
            with self.subTest(interrupted=interrupted):
                pool = ResourceRecoveryPool()
                scope = ResourceScope(pool)
                finished_cleanup = threading.Event()
                finish_operation = threading.Event()
                self.addCleanup(finish_operation.set)
                signal = ControlFlow("cleanup")
                events: list[str] = []

                def release(value: str) -> None:
                    events.append(value)
                    if interrupted and value == "old":
                        raise signal

                coordinator = CapabilityLifecycleCoordinator(
                    lambda request: Resource.make(lambda: request, release), scope=scope
                )
                coordinator.acquire(0, "old")
                original_complete = CapabilityLifecycleCoordinator._complete_operation

                def pause_completion(instance, generation, activation):
                    if instance is coordinator and generation == 0:
                        finished_cleanup.set()
                        wait(finish_operation)
                    return original_complete(instance, generation, activation)

                with patch.object(CapabilityLifecycleCoordinator, "_complete_operation", pause_completion):
                    old_release = ThreadCall(lambda: coordinator.release(0, "old"))
                    wait(finished_cleanup)
                    self.assertEqual(coordinator.read().generation, 1)
                    self.assertEqual(coordinator.read().phase, LifecyclePhase.IDLE)
                    new = coordinator.acquire(1, "new")
                    self.assertEqual(new.outcome, LifecycleOutcome.ACQUIRE_SUCCEEDED)
                    finish_operation.set()
                    old_release.join(self)
                if interrupted:
                    self.assertIs(old_release.error, signal)
                    self.assertEqual(len(pool), 1)
                else:
                    self.assertIsNone(old_release.error)
                    self.assertEqual(old_release.result.outcome, LifecycleOutcome.RELEASE_COMPLETED)
                self.assertEqual(coordinator.read().capability, "new")
                self.assertEqual(coordinator.read().generation, 1)
                scope.close()
                self.assertEqual(coordinator.read().generation, 2)
                self.assertEqual(events, ["old", "new"])

    def test_old_failed_acquire_completion_cannot_overwrite_a_new_activation(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        finished_cleanup = threading.Event()
        finish_operation = threading.Event()
        self.addCleanup(finish_operation.set)
        coordinator = CapabilityLifecycleCoordinator(
            lambda request: Resource.pure(request).map(
                lambda value: fail(ValueError("old")) if value == "old" else value
            ), scope=scope
        )
        original_complete = CapabilityLifecycleCoordinator._complete_operation

        def pause_completion(instance, generation, activation):
            if instance is coordinator and generation == 0:
                finished_cleanup.set()
                wait(finish_operation)
            return original_complete(instance, generation, activation)

        with patch.object(CapabilityLifecycleCoordinator, "_complete_operation", pause_completion):
            old_acquire = ThreadCall(lambda: coordinator.acquire(0, "old"))
            wait(finished_cleanup)
            self.assertEqual(coordinator.read().generation, 1)
            coordinator.acquire(1, "new")
            finish_operation.set()
            result = old_acquire.succeeded(self)
        self.assertEqual(result.outcome, LifecycleOutcome.ACQUIRE_FAILED)
        self.assertEqual(result.snapshot.generation, 1)
        self.assertEqual(coordinator.read().capability, "new")
        self.assertEqual(coordinator.read().generation, 1)
        scope.close()

    def test_old_unpublished_acquire_cannot_publish_over_a_new_activation(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        committed = threading.Event()
        publish = threading.Event()
        self.addCleanup(publish.set)
        activations: list[ResourceScope] = []
        released: list[str] = []
        coordinator = CapabilityLifecycleCoordinator(
            lambda request: Resource.make(lambda: request, released.append), scope=scope
        )
        original_acquire = ResourceScope.acquire

        def pause_first_commit(activation, resource):
            value = original_acquire(activation, resource)
            if not activations:
                activations.append(activation)
                committed.set()
                wait(publish)
            return value

        with patch.object(ResourceScope, "acquire", pause_first_commit):
            old_acquire = ThreadCall(lambda: coordinator.acquire(0, "old"))
            wait(committed)
            activations[0].close()
            self.assertEqual(coordinator.read().generation, 1)
            new = coordinator.acquire(1, "new")
            self.assertEqual(new.outcome, LifecycleOutcome.ACQUIRE_SUCCEEDED)
            publish.set()
            result = old_acquire.succeeded(self)
        self.assertEqual(result.outcome, LifecycleOutcome.ACQUIRE_FAILED)
        self.assertEqual(result.snapshot.generation, 1)
        self.assertEqual(coordinator.read().capability, "new")
        scope.close()
        self.assertEqual(released, ["old", "new"])

    def test_ancestor_closing_withdraws_capability_before_activation_is_visited(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        coordinator = CapabilityLifecycleCoordinator(lambda _: Resource.pure("value"), scope=scope)
        coordinator.acquire(0, "request")
        blocked = threading.Event()
        unblock = threading.Event()
        self.addCleanup(unblock.set)

        def block(_: str) -> None:
            blocked.set()
            wait(unblock)

        scope.child().acquire(Resource.make(lambda: "blocker", block))
        closing = ThreadCall(scope.close)
        wait(blocked)
        snapshot = coordinator.read()
        self.assertEqual(snapshot.phase, LifecyclePhase.RELEASING)
        self.assertEqual(snapshot.generation, 0)
        self.assertIsNone(snapshot.capability)
        self.assertEqual(coordinator.acquire(0, "request").outcome, LifecycleOutcome.NOT_EXECUTED)
        with self.assertRaises(ValueError):
            coordinator.create_child(0, lambda _parent, child: Resource.pure(child))
        unblock.set()
        closing.succeeded(self)
        self.assertEqual(coordinator.read().generation, 1)

    def test_busy_release_does_not_start_other_operations(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        entered = threading.Event()
        proceed = threading.Event()
        self.addCleanup(proceed.set)

        def release(_: str) -> None:
            entered.set()
            wait(proceed)

        coordinator = CapabilityLifecycleCoordinator(
            lambda _: Resource.make(lambda: "value", release), scope=scope
        )
        coordinator.acquire(0, "request")
        releasing = ThreadCall(lambda: coordinator.release(0, "request"))
        wait(entered)
        for result in (coordinator.acquire(0, "next"), coordinator.release(0, "request")):
            self.assertEqual(result.outcome, LifecycleOutcome.NOT_EXECUTED)
            self.assertEqual(result.snapshot.phase, LifecyclePhase.RELEASING)
            self.assertEqual(result.snapshot.generation, 0)
            self.assertIsNone(result.snapshot.capability)
        proceed.set()
        result = releasing.succeeded(self)
        self.assertEqual(result.snapshot.generation, 1)
        scope.close()

    def test_factory_runs_outside_control_lock(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        phases: list[LifecyclePhase] = []

        def factory(request: str) -> Resource[str]:
            phases.append(ThreadCall(coordinator.read).succeeded(self).phase)
            return Resource.pure(request)

        coordinator = CapabilityLifecycleCoordinator(factory, scope=scope)
        self.assertEqual(coordinator.acquire(0, "request").outcome, LifecycleOutcome.ACQUIRE_SUCCEEDED)
        self.assertEqual(phases, [LifecyclePhase.ACQUIRING])
        scope.close()


if __name__ == "__main__":
    unittest.main()
