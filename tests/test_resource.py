from __future__ import annotations

import unittest

from lifecycle.resource import (
    Resource,
    ResourceAllocationError,
    ResourceRecoveryPool,
    ResourceScope,
)


class ControlFlow(BaseException):
    pass


class ResourceCompositionTests(unittest.TestCase):
    def test_original_interruption_wins_over_cleanup_interruption(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        original = ControlFlow("acquire")
        cleanup = ControlFlow("cleanup")

        def interrupt(error: BaseException) -> None:
            raise error

        resource = Resource.make(lambda: "value", lambda _: interrupt(cleanup)).map(
            lambda _: interrupt(original)
        )
        with self.assertRaises(ControlFlow) as raised:
            scope.acquire(resource)
        self.assertIs(raised.exception, original)
        self.assertIs(pool.snapshot()[0].error, cleanup)
        self.assertEqual(scope.close().pooled_count, 0)

    def test_cleanup_interruption_wins_over_ordinary_acquire_error(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        original = ValueError("acquire")
        cleanup = ControlFlow("cleanup")
        events: list[str] = []

        def interrupt(error: BaseException) -> None:
            raise error

        resource = Resource.make(lambda: "outer", events.append).flat_map(
            lambda _: Resource.make(lambda: "inner", lambda _: interrupt(cleanup))
        ).map(lambda _: interrupt(original))
        with self.assertRaises(ControlFlow) as raised:
            scope.acquire(resource)
        self.assertIs(raised.exception, cleanup)
        self.assertIs(raised.exception.__cause__, original)
        self.assertEqual(events, ["outer"])
        self.assertEqual(len(pool), 1)
        scope.close()

    def test_description_is_lazy_and_each_acquire_creates_a_fresh_instance(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        events: list[tuple[str, int]] = []
        next_id = 0

        def acquire() -> dict[str, int]:
            nonlocal next_id
            next_id += 1
            events.append(("acquire", next_id))
            return {"id": next_id}

        def release(value: dict[str, int]) -> None:
            events.append(("release", value["id"]))

        resource = Resource.make(acquire, release)
        self.assertEqual(events, [])

        first_scope = scope.child()
        second_scope = scope.child()
        first = first_scope.acquire(resource)
        second = second_scope.acquire(resource)
        self.assertIsNot(first, second)
        self.assertEqual(events, [("acquire", 1), ("acquire", 2)])

        first_scope.close()
        second_scope.close()
        self.assertEqual(
            events,
            [("acquire", 1), ("acquire", 2), ("release", 1), ("release", 2)],
        )

    def test_map_changes_only_the_exposed_value(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        released: list[object] = []
        raw = object()

        value = scope.acquire(Resource.make(lambda: raw, released.append).map(
            lambda value: {"capability": value}
        ))

        self.assertIs(value["capability"], raw)
        scope.close()
        self.assertEqual(released, [raw])

    def test_flat_map_acquires_in_order_and_releases_lifo(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        events: list[str] = []

        outer = Resource.make(
            lambda: events.append("acquire-server") or "server",
            lambda _value: events.append("release-server"),
        )
        resource = outer.flat_map(
            lambda server: Resource.make(
                lambda: events.append("acquire-connection") or f"{server}/connection",
                lambda _value: events.append("release-connection"),
            )
        )

        value = scope.acquire(resource)
        self.assertEqual(value, "server/connection")
        scope.close()
        self.assertEqual(
            events,
            [
                "acquire-server",
                "acquire-connection",
                "release-connection",
                "release-server",
            ],
        )

    def test_later_acquire_failure_rolls_back_registered_resources(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        events: list[str] = []

        outer = Resource.make(
            lambda: events.append("acquire-server") or "server",
            lambda _value: events.append("release-server"),
        )

        def fail() -> str:
            events.append("acquire-connection")
            raise ValueError("connection failed")

        resource = outer.flat_map(
            lambda _server: Resource.make(fail, lambda _value: events.append("never"))
        )

        with self.assertRaises(ResourceAllocationError) as raised:
            scope.acquire(resource)

        self.assertIsInstance(raised.exception.cause, ValueError)
        self.assertEqual(raised.exception.release_report.pooled_count, 0)
        self.assertEqual(
            events,
            ["acquire-server", "acquire-connection", "release-server"],
        )

    def test_cleanup_failure_during_rollback_does_not_replace_acquire_error(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)

        def release_outer(_value: str) -> None:
            raise OSError("rollback cleanup failed")

        outer = Resource.make(lambda: "outer", release_outer)

        def fail() -> str:
            raise ValueError("primary acquire failure")

        resource = outer.flat_map(
            lambda _value: Resource.make(fail, lambda _value: None)
        )

        with self.assertRaises(ResourceAllocationError) as raised:
            scope.acquire(resource)

        self.assertIsInstance(raised.exception.cause, ValueError)
        self.assertEqual(str(raised.exception.cause), "primary acquire failure")
        self.assertEqual(raised.exception.release_report.pooled_count, 1)
        self.assertEqual(len(raised.exception.release_report.errors), 1)
        self.assertIsInstance(raised.exception.release_report.errors[0], OSError)
        self.assertEqual(len(pool), 1)

    def test_projection_failure_rolls_back_original_resource(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        events: list[str] = []

        def project(_value: str) -> str:
            raise LookupError("projection failed")

        resource = Resource.make(
            lambda: events.append("acquire") or "raw",
            lambda _value: events.append("release"),
        ).map(project)

        with self.assertRaises(ResourceAllocationError) as raised:
            scope.acquire(resource)

        self.assertIsInstance(raised.exception.cause, LookupError)
        self.assertEqual(events, ["acquire", "release"])

    def test_single_make_failure_cannot_finalize_unreturned_internal_state(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        release_calls = 0

        def acquire() -> str:
            raise RuntimeError("failed before ownership transfer")

        def release(_value: str) -> None:
            nonlocal release_calls
            release_calls += 1

        with self.assertRaises(ResourceAllocationError):
            scope.acquire(Resource.make(acquire, release))

        self.assertEqual(release_calls, 0)

    def test_control_flow_interruption_unwinds_registered_resources_then_propagates(self) -> None:
        pool = ResourceRecoveryPool()
        scope = ResourceScope(pool)
        self.addCleanup(scope.close)
        events: list[str] = []
        outer = Resource.make(
            lambda: events.append("acquire-outer") or "outer",
            lambda _value: events.append("release-outer"),
        )

        def interrupt() -> str:
            events.append("acquire-inner")
            raise ControlFlow("stop")

        resource = outer.flat_map(
            lambda _value: Resource.make(interrupt, lambda _value: None)
        )

        with self.assertRaises(ControlFlow):
            scope.acquire(resource)
        self.assertEqual(events, ["acquire-outer", "acquire-inner", "release-outer"])


if __name__ == "__main__":
    unittest.main()
