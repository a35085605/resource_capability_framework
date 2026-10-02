from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from threading import Condition, Lock
from typing import Generic, TypeVar, cast

from lifecycle.resource.recovery import ResourceRecoveryPool
from lifecycle.resource.result import ReleaseReport, ResourceAllocationError


T = TypeVar("T")
U = TypeVar("U")


@dataclass(slots=True)
class _Finalizer:
    resource: object
    release: Callable[[], None]


class _FinalizerScope:
    """One static Resource program's LIFO finalizer stack."""

    __slots__ = ("_finalizers", "_pool")

    def __init__(self, pool: ResourceRecoveryPool) -> None:
        self._pool = pool
        self._finalizers: list[_Finalizer] = []

    def register(self, resource: object, release: Callable[[], None]) -> None:
        self._finalizers.append(_Finalizer(resource, release))

    def release_all(self) -> tuple[ReleaseReport, BaseException | None]:
        # Detach first so a repeated close cannot execute finalizers twice.
        finalizers = self._finalizers
        self._finalizers = []

        errors: list[BaseException] = []
        pooled_count = 0
        interruption: BaseException | None = None

        for finalizer in reversed(finalizers):
            try:
                finalizer.release()
            except BaseException as exc:
                errors.append(exc)
                self._pool._accept(finalizer.resource, finalizer.release, exc)
                pooled_count += 1
                if not isinstance(exc, Exception) and interruption is None:
                    interruption = exc

        return ReleaseReport(tuple(errors), pooled_count), interruption


class Resource(Generic[T]):
    """A reusable, lazy description of how to allocate and finalize a value."""

    __slots__ = ("_allocate_into",)

    def __init__(self, allocate_into: Callable[[_FinalizerScope], T]) -> None:
        if not callable(allocate_into):
            raise TypeError("allocate_into must be callable")
        self._allocate_into = allocate_into

    @classmethod
    def make(
        cls,
        acquire: Callable[[], T],
        release: Callable[[T], None],
    ) -> "Resource[T]":
        if not callable(acquire):
            raise TypeError("acquire must be callable")
        if not callable(release):
            raise TypeError("release must be callable")

        def allocate(scope: _FinalizerScope) -> T:
            value = acquire()

            def finalize(value: T = value) -> None:
                release(value)

            # Ownership becomes framework-visible only after acquire returns. Register
            # immediately so subsequent map/flat_map failures are safely unwound.
            scope.register(value, finalize)
            return value

        return cls(allocate)

    @classmethod
    def pure(cls, value: T) -> "Resource[T]":
        return cls(lambda _scope: value)

    def map(self, project: Callable[[T], U]) -> "Resource[U]":
        if not callable(project):
            raise TypeError("project must be callable")

        def allocate(scope: _FinalizerScope) -> U:
            return project(self._allocate_into(scope))

        return Resource(allocate)

    def flat_map(self, bind: Callable[[T], "Resource[U]"]) -> "Resource[U]":
        if not callable(bind):
            raise TypeError("bind must be callable")

        def allocate(scope: _FinalizerScope) -> U:
            next_resource = bind(self._allocate_into(scope))
            if not isinstance(next_resource, Resource):
                raise TypeError("flat_map must return Resource")
            return next_resource._allocate_into(scope)

        return Resource(allocate)

    def allocate(self, pool: ResourceRecoveryPool) -> "ResourceHandle[T]":
        if not isinstance(pool, ResourceRecoveryPool):
            raise TypeError("pool must be a ResourceRecoveryPool")

        scope = _FinalizerScope(pool)
        try:
            value = self._allocate_into(scope)
        except BaseException as exc:
            report, cleanup_interruption = scope.release_all()

            # Control-flow interruptions are never normalized into ordinary allocation
            # failures. The original interruption wins over one raised during cleanup.
            if not isinstance(exc, Exception):
                raise
            if cleanup_interruption is not None:
                raise cleanup_interruption from exc
            raise ResourceAllocationError(exc, report) from exc

        return ResourceHandle(value, scope, pool)


class _HandleState(Enum):
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


class ResourceHandle(Generic[T]):
    """One allocated Resource instance and the sole runtime ownership carrier."""

    __slots__ = (
        "_children",
        "_condition",
        "_inflight_children",
        "_parent",
        "_pool",
        "_release_report",
        "_scope",
        "_state",
        "_value",
    )

    def __init__(
        self,
        value: T,
        scope: _FinalizerScope,
        pool: ResourceRecoveryPool,
    ) -> None:
        self._value = value
        self._scope = scope
        self._pool = pool
        self._condition = Condition(Lock())
        self._state = _HandleState.OPEN
        self._children: list[ResourceHandle[object]] = []
        self._inflight_children = 0
        self._release_report: ReleaseReport | None = None
        self._parent: ResourceHandle[object] | None = None

    @property
    def value(self) -> T:
        return self._value

    @property
    def closed(self) -> bool:
        with self._condition:
            return self._state is _HandleState.CLOSED

    def allocate_child(self, resource: Resource[U]) -> "ResourceHandle[U]":
        if not isinstance(resource, Resource):
            raise TypeError("resource must be a Resource")

        with self._condition:
            if self._state is not _HandleState.OPEN:
                raise ResourceAllocationError(
                    RuntimeError("parent resource handle is closing or closed"),
                    ReleaseReport(),
                )
            self._inflight_children += 1

        try:
            child = resource.allocate(self._pool)
        except BaseException:
            with self._condition:
                self._inflight_children -= 1
                self._condition.notify_all()
            raise

        with self._condition:
            if self._state is _HandleState.OPEN:
                child._parent = cast(ResourceHandle[object], self)
                self._children.append(cast(ResourceHandle[object], child))
                self._inflight_children -= 1
                self._condition.notify_all()
                return child

        # The parent began closing while physical child allocation was in flight. Keep
        # the allocation counted as in-flight until its cleanup/handoff is complete so
        # the parent cannot finalize dependencies too early.
        interruption: BaseException | None = None
        try:
            report = child.release()
        except BaseException as exc:
            interruption = exc
            report = child.release()  # already CLOSED; returns the cached report
        finally:
            with self._condition:
                self._inflight_children -= 1
                self._condition.notify_all()

        if interruption is not None and not isinstance(interruption, Exception):
            raise interruption
        raise ResourceAllocationError(
            RuntimeError("parent resource handle closed during child allocation"),
            report,
        )

    def release(self) -> ReleaseReport:
        with self._condition:
            if self._state is _HandleState.CLOSED:
                assert self._release_report is not None
                return self._release_report

            if self._state is _HandleState.CLOSING:
                while self._state is not _HandleState.CLOSED:
                    self._condition.wait()
                assert self._release_report is not None
                return self._release_report

            self._state = _HandleState.CLOSING
            while self._inflight_children:
                self._condition.wait()

            # The closing caller takes responsibility for this exact child snapshot.
            # Children may concurrently finish their own release; release() is idempotent.
            children = list(reversed(self._children))
            self._children.clear()

        combined = ReleaseReport()
        interruption: BaseException | None = None

        for child in children:
            try:
                child_report = child.release()
            except BaseException as exc:
                if not isinstance(exc, Exception) and interruption is None:
                    interruption = exc
                # The first caller completed the child's close before propagating the
                # interruption. A second call obtains the shared cached report.
                child_report = child.release()
            combined = combined.merged(child_report)

        own_report, own_interruption = self._scope.release_all()
        combined = combined.merged(own_report)
        if interruption is None:
            interruption = own_interruption

        with self._condition:
            self._release_report = combined
            self._state = _HandleState.CLOSED
            self._condition.notify_all()

        self._detach_from_parent()

        if interruption is not None:
            raise interruption
        return combined

    def _detach_from_parent(self) -> None:
        parent = self._parent
        if parent is None:
            return
        self._parent = None
        parent._detach_child(cast(ResourceHandle[object], self))

    def _detach_child(self, child: "ResourceHandle[object]") -> None:
        with self._condition:
            try:
                self._children.remove(child)
            except ValueError:
                pass


__all__ = ["Resource", "ResourceHandle"]
