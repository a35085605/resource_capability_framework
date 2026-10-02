from __future__ import annotations

import threading
import time
import unittest

from lifecycle.resource import (
    Resource,
    ResourceAllocationError,
    ResourceRecoveryPool,
)


class ControlFlow(BaseException):
    pass


class ResourceHandleTests(unittest.TestCase):
    def test_server_releases_remaining_children_before_itself(self) -> None:
        pool = ResourceRecoveryPool()
        events: list[str] = []
        server = Resource.make(
            lambda: "server",
            lambda _value: events.append("release-server"),
        ).allocate(pool)

        def connection(name: str) -> Resource[str]:
            return Resource.make(
                lambda: name,
                lambda _value: events.append(f"release-{name}"),
            )

        first = server.allocate_child(connection("c1"))
        second = server.allocate_child(connection("c2"))
        third = server.allocate_child(connection("c3"))

        second.release()
        self.assertEqual(events, ["release-c2"])
        self.assertFalse(server.closed)
        self.assertFalse(first.closed)
        self.assertFalse(third.closed)

        server.release()
        self.assertEqual(
            events,
            ["release-c2", "release-c3", "release-c1", "release-server"],
        )
        self.assertTrue(first.closed)
        self.assertTrue(third.closed)

    def test_failed_child_finalizer_is_pooled_and_does_not_stop_unwind(self) -> None:
        pool = ResourceRecoveryPool()
        events: list[str] = []
        server = Resource.make(
            lambda: "server",
            lambda _value: events.append("release-server"),
        ).allocate(pool)

        server.allocate_child(
            Resource.make(
                lambda: "good",
                lambda _value: events.append("release-good"),
            )
        )

        def release_bad(_value: str) -> None:
            events.append("release-bad")
            raise OSError("still open")

        server.allocate_child(Resource.make(lambda: "bad", release_bad))

        report = server.release()
        self.assertEqual(events, ["release-bad", "release-good", "release-server"])
        self.assertEqual(report.pooled_count, 1)
        self.assertEqual(len(report.errors), 1)
        self.assertEqual(len(pool), 1)
        entry = pool.snapshot()[0]
        self.assertEqual(entry.resource, "bad")
        self.assertIsInstance(entry.error, OSError)

    def test_finalizer_owns_its_finite_retry_policy_and_external_release_runs_once(self) -> None:
        pool = ResourceRecoveryPool()
        attempts = 0

        def release(_value: str) -> None:
            nonlocal attempts
            for _ in range(3):
                attempts += 1
            raise OSError("retry budget exhausted")

        handle = Resource.make(lambda: "resource", release).allocate(pool)
        first = handle.release()
        second = handle.release()

        self.assertIs(first, second)
        self.assertEqual(attempts, 3)
        self.assertEqual(first.pooled_count, 1)
        self.assertEqual(len(pool), 1)

    def test_concurrent_release_shares_one_finalization_round(self) -> None:
        pool = ResourceRecoveryPool()
        entered = threading.Event()
        allow_finish = threading.Event()
        attempts = 0
        results = []

        def release(_value: str) -> None:
            nonlocal attempts
            attempts += 1
            entered.set()
            allow_finish.wait(2)

        handle = Resource.make(lambda: "resource", release).allocate(pool)

        def worker() -> None:
            results.append(handle.release())

        first = threading.Thread(target=worker)
        second = threading.Thread(target=worker)
        first.start()
        self.assertTrue(entered.wait(2))
        second.start()
        time.sleep(0.05)
        allow_finish.set()
        first.join(2)
        second.join(2)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(attempts, 1)
        self.assertEqual(len(results), 2)
        self.assertIs(results[0], results[1])

    def test_parent_and_child_concurrent_release_finalize_child_once(self) -> None:
        pool = ResourceRecoveryPool()
        child_entered = threading.Event()
        allow_child_finish = threading.Event()
        child_attempts = 0
        events: list[str] = []

        parent = Resource.make(
            lambda: "parent",
            lambda _value: events.append("release-parent"),
        ).allocate(pool)

        def release_child(_value: str) -> None:
            nonlocal child_attempts
            child_attempts += 1
            child_entered.set()
            allow_child_finish.wait(2)
            events.append("release-child")

        child = parent.allocate_child(
            Resource.make(lambda: "child", release_child)
        )

        child_thread = threading.Thread(target=child.release)
        parent_thread = threading.Thread(target=parent.release)
        child_thread.start()
        self.assertTrue(child_entered.wait(2))
        parent_thread.start()
        time.sleep(0.05)
        allow_child_finish.set()
        child_thread.join(2)
        parent_thread.join(2)

        self.assertFalse(child_thread.is_alive())
        self.assertFalse(parent_thread.is_alive())
        self.assertEqual(child_attempts, 1)
        self.assertEqual(events, ["release-child", "release-parent"])

    def test_parent_release_waits_for_inflight_child_cleanup_before_parent_finalizer(self) -> None:
        pool = ResourceRecoveryPool()
        events: list[str] = []
        child_acquire_started = threading.Event()
        allow_child_acquire = threading.Event()
        child_finished = threading.Event()
        parent_finished = threading.Event()
        allocation_error: list[BaseException] = []

        parent = Resource.make(
            lambda: "server",
            lambda _value: events.append("release-server"),
        ).allocate(pool)

        def acquire_child() -> str:
            events.append("acquire-child")
            child_acquire_started.set()
            allow_child_acquire.wait(2)
            return "connection"

        child_resource = Resource.make(
            acquire_child,
            lambda _value: events.append("release-child"),
        )

        def allocate_worker() -> None:
            try:
                parent.allocate_child(child_resource)
            except BaseException as exc:
                allocation_error.append(exc)
            finally:
                child_finished.set()

        def release_worker() -> None:
            parent.release()
            parent_finished.set()

        allocate_thread = threading.Thread(target=allocate_worker)
        allocate_thread.start()
        self.assertTrue(child_acquire_started.wait(2))

        release_thread = threading.Thread(target=release_worker)
        release_thread.start()
        time.sleep(0.05)
        self.assertFalse(parent_finished.is_set())

        allow_child_acquire.set()
        self.assertTrue(child_finished.wait(2))
        self.assertTrue(parent_finished.wait(2))
        allocate_thread.join(2)
        release_thread.join(2)

        self.assertEqual(len(allocation_error), 1)
        self.assertIsInstance(allocation_error[0], ResourceAllocationError)
        self.assertEqual(events, ["acquire-child", "release-child", "release-server"])

    def test_parent_closing_rejects_new_child_without_physical_acquire(self) -> None:
        pool = ResourceRecoveryPool()
        parent = Resource.pure("server").allocate(pool)
        parent.release()
        acquire_calls = 0

        def acquire() -> str:
            nonlocal acquire_calls
            acquire_calls += 1
            return "child"

        with self.assertRaises(ResourceAllocationError):
            parent.allocate_child(Resource.make(acquire, lambda _value: None))
        self.assertEqual(acquire_calls, 0)

    def test_release_interruption_closes_scope_continues_unwind_and_only_initiator_sees_it(self) -> None:
        pool = ResourceRecoveryPool()
        events: list[str] = []

        outer = Resource.make(
            lambda: "outer",
            lambda _value: events.append("release-outer"),
        )

        def release_inner(_value: str) -> None:
            events.append("release-inner")
            raise ControlFlow("cancel")

        handle = outer.flat_map(
            lambda _outer: Resource.make(lambda: "inner", release_inner)
        ).allocate(pool)

        with self.assertRaises(ControlFlow):
            handle.release()

        self.assertTrue(handle.closed)
        self.assertEqual(events, ["release-inner", "release-outer"])
        report = handle.release()
        self.assertEqual(report.pooled_count, 1)
        self.assertEqual(len(report.errors), 1)
        self.assertEqual(len(pool), 1)


if __name__ == "__main__":
    unittest.main()
