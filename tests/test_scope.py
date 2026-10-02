from __future__ import annotations

import threading
import unittest
from unittest.mock import patch

from lifecycle.resource import (
    Resource,
    ResourceAllocationError,
    ResourceRecoveryPool,
    ResourceScope,
    ScopeClosedError,
)
from tests.support import ThreadCall, wait, wait_closing


class ControlFlow(BaseException):
    pass


def fail(error: BaseException) -> None:
    raise error


class ResourceScopeTests(unittest.TestCase):
    def test_server_releases_remaining_children_before_itself(self) -> None:
        pool = ResourceRecoveryPool()
        events: list[str] = []
        root = ResourceScope(pool)
        server = root.child()
        server.acquire(Resource.make(lambda: "server", events.append))

        def connection(name: str) -> ResourceScope:
            child = server.child()
            child.acquire(Resource.make(lambda: name, events.append))
            return child

        first = connection("c1")
        second = connection("c2")
        third = connection("c3")
        second.close()
        self.assertEqual(events, ["c2"])
        self.assertFalse(server.closed)
        self.assertFalse(first.closed)
        self.assertFalse(third.closed)

        root.close()
        self.assertEqual(events, ["c2", "c3", "c1", "server"])
        self.assertTrue(server.closed)
        self.assertTrue(first.closed)
        self.assertTrue(third.closed)

    def test_failed_child_finalizer_is_pooled_and_does_not_stop_unwind(self) -> None:
        pool = ResourceRecoveryPool()
        events: list[str] = []
        server = ResourceScope(pool)
        server.acquire(Resource.make(lambda: "server", events.append))
        server.child().acquire(Resource.make(lambda: "good", events.append))

        def release_bad(value: str) -> None:
            events.append(value)
            raise OSError("still open")

        server.child().acquire(Resource.make(lambda: "bad", release_bad))
        report = server.close()
        self.assertEqual(events, ["bad", "good", "server"])
        self.assertEqual(report.pooled_count, 1)
        self.assertEqual(len(report.errors), 1)
        self.assertEqual(len(pool), 1)
        entry = pool.snapshot()[0]
        self.assertEqual(entry.resource, "bad")
        self.assertIsInstance(entry.error, OSError)

    def test_finalizer_owns_its_finite_retry_policy_and_external_close_runs_once(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        attempts = 0

        def release(_value: str) -> None:
            nonlocal attempts
            for _ in range(3):
                attempts += 1
            raise OSError("retry budget exhausted")

        scope.acquire(Resource.make(lambda: "resource", release))
        first = scope.close()
        self.assertIs(first, scope.close())
        self.assertEqual(attempts, 3)
        self.assertEqual(first.pooled_count, 1)
        self.assertEqual(len(pool), 1)

    def test_concurrent_close_shares_one_finalization_round(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        entered = threading.Event()
        allow_finish = threading.Event()
        waiting = threading.Event()
        self.addCleanup(allow_finish.set)
        attempts = 0

        def release(_value: str) -> None:
            nonlocal attempts
            attempts += 1
            entered.set()
            wait(allow_finish)

        scope.acquire(Resource.make(lambda: "resource", release))
        first = ThreadCall(scope.close)
        wait(entered)
        original_wait = scope._condition.wait

        def observed_wait(timeout=None):
            waiting.set()
            return original_wait(timeout)

        with patch.object(scope._condition, "wait", observed_wait):
            second = ThreadCall(scope.close)
            wait(waiting)
            self.assertFalse(scope.closed)
            self.assertFalse(second.done.is_set())
            allow_finish.set()
            report = first.succeeded(self)
            self.assertIs(report, second.succeeded(self))
        self.assertIs(report, scope.close())
        self.assertEqual(attempts, 1)

    def test_parent_and_child_concurrent_close_finalize_child_once(self) -> None:
        parent = ResourceScope(ResourceRecoveryPool())
        events: list[str] = []
        child_entered = threading.Event()
        allow_child_finish = threading.Event()
        self.addCleanup(allow_child_finish.set)
        parent.acquire(Resource.make(lambda: "parent", events.append))
        child = parent.child()

        def release_child(value: str) -> None:
            child_entered.set()
            wait(allow_child_finish)
            events.append(value)

        child.acquire(Resource.make(lambda: "child", release_child))
        child_call = ThreadCall(child.close)
        wait(child_entered)
        parent_call = ThreadCall(parent.close)
        wait_closing(parent)
        self.assertFalse(parent_call.done.is_set())
        allow_child_finish.set()
        child_call.succeeded(self)
        parent_call.succeeded(self)
        self.assertEqual(events, ["child", "parent"])

    def test_parent_close_waits_for_inflight_child_rollback_before_parent_finalizer(self) -> None:
        parent = ResourceScope(ResourceRecoveryPool())
        events: list[str] = []
        started = threading.Event()
        allow_acquire = threading.Event()
        rollback_started = threading.Event()
        allow_rollback = threading.Event()
        self.addCleanup(allow_acquire.set)
        self.addCleanup(allow_rollback.set)
        parent.acquire(Resource.make(lambda: "server", events.append))
        child = parent.child()

        def acquire() -> str:
            started.set()
            wait(allow_acquire)
            return "child"

        def rollback(value: str) -> None:
            rollback_started.set()
            wait(allow_rollback)
            events.append(value)

        allocation = ThreadCall(lambda: child.acquire(Resource.make(acquire, rollback)))
        wait(started)
        closing = ThreadCall(parent.close)
        wait_closing(parent)
        self.assertFalse(closing.done.is_set())
        with self.assertRaises(ResourceAllocationError) as raised:
            child.acquire(Resource.make(lambda: self.fail("must not acquire"), lambda _: None))
        self.assertIsInstance(raised.exception.cause, ScopeClosedError)
        with self.assertRaises(ScopeClosedError):
            child.child()
        allow_acquire.set()
        wait(rollback_started)
        self.assertEqual(events, [])
        self.assertFalse(closing.done.is_set())
        allow_rollback.set()
        allocation.join(self)
        self.assertIsInstance(allocation.error, ResourceAllocationError)
        self.assertIsInstance(allocation.error.cause, ScopeClosedError)
        closing.succeeded(self)
        self.assertEqual(events, ["child", "server"])

    def test_closed_scope_rejects_new_operations_without_physical_acquire(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        scope.close()
        with self.assertRaises(ResourceAllocationError) as raised:
            scope.acquire(Resource.make(lambda: self.fail("must not acquire"), lambda _: None))
        self.assertIsInstance(raised.exception.cause, ScopeClosedError)
        self.assertEqual(raised.exception.release_report.pooled_count, 0)
        with self.assertRaises(ScopeClosedError):
            scope.child()
        with self.assertRaises(ScopeClosedError):
            scope.__enter__()

    def test_close_interruption_continues_unwind_and_only_initiator_sees_it(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        events: list[str] = []
        entered = threading.Event()
        proceed = threading.Event()
        waiting = threading.Event()
        self.addCleanup(proceed.set)
        first_signal = ControlFlow("child")
        later_signal = ControlFlow("parent")

        def release_child(value: str) -> None:
            entered.set()
            wait(proceed)
            events.append(value)
            raise first_signal

        scope.acquire(Resource.make(lambda: "outer", events.append))
        scope.acquire(Resource.make(lambda: "inner", lambda _: fail(later_signal)))
        scope.child().acquire(Resource.make(lambda: "child", release_child))
        initiator = ThreadCall(scope.close)
        wait(entered)
        original_wait = scope._condition.wait

        def observed_wait(timeout=None):
            waiting.set()
            return original_wait(timeout)

        with patch.object(scope._condition, "wait", observed_wait):
            follower = ThreadCall(scope.close)
            wait(waiting)
            proceed.set()
            initiator.join(self)
            report = follower.succeeded(self)
        self.assertIs(initiator.error, first_signal)
        self.assertTrue(scope.closed)
        self.assertIs(report, scope.close())
        self.assertEqual(report.errors, (first_signal, later_signal))
        self.assertEqual(report.pooled_count, 2)
        self.assertEqual(len(pool), 2)
        self.assertEqual(events, ["child", "outer"])

    def test_failed_acquire_only_rolls_back_its_batch(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        live: set[str] = set()

        def resource(name: str) -> Resource[str]:
            return Resource.make(lambda: live.add(name) or name, live.remove)

        self.assertEqual(scope.acquire(resource("first")), "first")
        broken = resource("second").map(lambda _: fail(ValueError("projection")))
        with self.assertRaises(ResourceAllocationError):
            scope.acquire(broken)
        self.assertEqual(live, {"first"})
        self.assertEqual(scope.acquire(resource("third")), "third")
        self.assertEqual(live, {"first", "third"})
        scope.close()
        self.assertEqual(live, set())

    def test_children_then_batches_then_batch_finalizers_are_lifo(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        events: list[str] = []

        def resource(name: str) -> Resource[str]:
            return Resource.make(lambda: name, events.append)

        scope.acquire(resource("a1").flat_map(lambda _: resource("a2")))
        scope.child().acquire(resource("child1"))
        scope.acquire(resource("b1").flat_map(lambda _: resource("b2")))
        scope.child().acquire(resource("child2"))
        scope.acquire(resource("c"))
        scope.close()
        self.assertEqual(events, ["child2", "child1", "c", "b2", "b1", "a2", "a1"])

    def test_waited_rollback_and_child_and_own_release_reports_merge_once(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        child = scope.child()
        errors = [OSError(name) for name in ("own rollback", "child rollback", "child", "own")]
        scope.acquire(Resource.make(lambda: "own", lambda _: fail(errors[3])))
        child.acquire(Resource.make(lambda: "child", lambda _: fail(errors[2])))
        own_started = threading.Event()
        child_started = threading.Event()
        proceed = threading.Event()
        self.addCleanup(proceed.set)

        def broken(started: threading.Event, cleanup_error: Exception) -> Resource[str]:
            def project(value: str) -> str:
                started.set()
                wait(proceed)
                raise ValueError("acquire failed")

            return Resource.make(lambda: "rollback", lambda _: fail(cleanup_error)).map(project)

        own_call = ThreadCall(lambda: scope.acquire(broken(own_started, errors[0])))
        child_call = ThreadCall(lambda: child.acquire(broken(child_started, errors[1])))
        wait(own_started)
        wait(child_started)
        closing = ThreadCall(scope.close)
        wait_closing(scope)
        proceed.set()
        own_call.join(self)
        child_call.join(self)
        for call, error in ((own_call, errors[0]), (child_call, errors[1])):
            self.assertIsInstance(call.error, ResourceAllocationError)
            self.assertIsInstance(call.error.cause, ValueError)
            self.assertEqual(call.error.release_report.errors, (error,))
        report = closing.succeeded(self)
        self.assertEqual(report.errors, tuple(errors))
        self.assertEqual(report.pooled_count, 4)
        self.assertEqual(len(pool), 4)
        self.assertIs(report, scope.close())

    def test_completed_failures_and_early_child_close_are_not_merged_later(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        error = OSError("cleanup")
        broken = Resource.make(lambda: "first", lambda _: fail(error)).map(
            lambda _: fail(ValueError("acquire"))
        )
        with self.assertRaises(ResourceAllocationError) as raised:
            scope.acquire(broken)
        child = scope.child()
        child.acquire(Resource.make(lambda: "child", lambda _: fail(error)))
        self.assertEqual(child.close().pooled_count, 1)
        self.assertEqual(raised.exception.release_report.pooled_count, 1)
        self.assertEqual(scope.close().pooled_count, 0)
        self.assertEqual(len(pool), 2)

    def test_child_finishing_while_parent_waits_still_belongs_to_close_report(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        child = scope.child()
        error = OSError("child")
        child.acquire(Resource.make(lambda: "child", lambda _: fail(error)))
        started = threading.Event()
        proceed = threading.Event()
        self.addCleanup(proceed.set)

        def acquire() -> str:
            started.set()
            wait(proceed)
            return "own"

        allocation = ThreadCall(lambda: scope.acquire(Resource.make(acquire, lambda _: None)))
        wait(started)
        closing = ThreadCall(scope.close)
        wait_closing(scope)
        self.assertEqual(child.close().errors, (error,))
        proceed.set()
        allocation.join(self)
        self.assertIsInstance(allocation.error, ResourceAllocationError)
        self.assertEqual(closing.succeeded(self).errors, (error,))
        self.assertEqual(len(pool), 1)

    def test_ancestor_shutdown_retains_rollback_before_child_close_is_visited(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        child = scope.child()
        blocker = scope.child()
        blocked = threading.Event()
        unblock = threading.Event()
        started = threading.Event()
        proceed = threading.Event()
        self.addCleanup(unblock.set)
        self.addCleanup(proceed.set)
        error = OSError("rollback")

        def release_blocker(_: str) -> None:
            blocked.set()
            wait(unblock)

        def acquire() -> str:
            started.set()
            wait(proceed)
            return "child"

        blocker.acquire(Resource.make(lambda: "blocker", release_blocker))
        allocation = ThreadCall(
            lambda: child.acquire(Resource.make(acquire, lambda _: fail(error)))
        )
        wait(started)
        closing = ThreadCall(scope.close)
        wait(blocked)
        with self.assertRaises(ScopeClosedError):
            child.__enter__()
        proceed.set()
        allocation.join(self)
        self.assertIsInstance(allocation.error, ResourceAllocationError)
        self.assertEqual(allocation.error.release_report.errors, (error,))
        self.assertFalse(child.closed)
        unblock.set()
        self.assertEqual(closing.succeeded(self).errors, (error,))
        self.assertEqual(len(pool), 1)

    def test_context_manager_returns_self_closes_and_preserves_body_exception(self) -> None:
        for body_error in (None, ValueError("body"), ControlFlow("body")):
            with self.subTest(body_error=body_error):
                scope = ResourceScope(ResourceRecoveryPool())
                cleanup_error = OSError("cleanup")

                def run() -> None:
                    with scope as entered:
                        self.assertIs(entered, scope)
                        self.assertIsNone(scope.acquire(Resource.pure(None)))
                        scope.acquire(Resource.make(lambda: "value", lambda _: fail(cleanup_error)))
                        if body_error is not None:
                            raise body_error

                if body_error is None:
                    run()
                else:
                    with self.assertRaises(type(body_error)) as raised:
                        run()
                    self.assertIs(raised.exception, body_error)
                self.assertTrue(scope.closed)
                self.assertEqual(scope.close().errors, (cleanup_error,))
                with self.assertRaises(ScopeClosedError):
                    scope.__enter__()

    def test_context_manager_cleanup_interruption_preserves_control_flow_priority(self) -> None:
        for body_error in (None, ValueError("body"), ControlFlow("body")):
            with self.subTest(body_error=body_error):
                scope = ResourceScope(ResourceRecoveryPool())
                interruption = ControlFlow("cleanup")
                with self.assertRaises(ControlFlow) as raised:
                    with scope:
                        scope.acquire(Resource.make(lambda: "value", lambda _: fail(interruption)))
                        if body_error is not None:
                            raise body_error
                expected = body_error if isinstance(body_error, ControlFlow) else interruption
                self.assertIs(raised.exception, expected)
                if isinstance(body_error, Exception):
                    self.assertIs(raised.exception.__cause__, body_error)
                self.assertTrue(scope.closed)
                self.assertEqual(scope.close().errors, (interruption,))

    def test_context_manager_cannot_be_reentered(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        with scope:
            with self.assertRaisesRegex(RuntimeError, "re-entered"):
                scope.__enter__()
            self.assertFalse(scope.closed)
        self.assertTrue(scope.closed)

    def test_callbacks_run_outside_tree_control_lock(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        child = scope.child()
        observations: list[bool] = []

        def callback() -> str:
            # A different thread must be able to take the shared lock while this
            # callback is still on the acquisition/finalization call stack.
            observations.append(ThreadCall(lambda: child.closed).succeeded(self))
            return "value"

        scope.acquire(Resource.make(callback, lambda _: callback()))
        report = scope.close()
        self.assertEqual(report.errors, ())
        self.assertEqual(observations, [False, True])

    def test_interruption_while_close_waits_does_not_strand_closing(self) -> None:
        scope = ResourceScope(ResourceRecoveryPool())
        started = threading.Event()
        proceed = threading.Event()
        interrupted = threading.Event()
        self.addCleanup(proceed.set)
        signal = ControlFlow("wait interrupted")

        def acquire() -> str:
            started.set()
            wait(proceed)
            return "value"

        allocation = ThreadCall(lambda: scope.acquire(Resource.make(acquire, lambda _: None)))
        wait(started)
        original_wait = scope._condition.wait

        def interrupt_once(timeout=None):
            if not interrupted.is_set():
                interrupted.set()
                raise signal
            return original_wait(timeout)

        with patch.object(scope._condition, "wait", interrupt_once):
            closing = ThreadCall(scope.close)
            wait(interrupted)
            proceed.set()
            closing.join(self)
            allocation.join(self)
        self.assertIs(closing.error, signal)
        self.assertIsInstance(allocation.error, ResourceAllocationError)
        self.assertTrue(scope.closed)
        self.assertEqual(scope.close().errors, ())


if __name__ == "__main__":
    unittest.main()
