from __future__ import annotations

from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass
from enum import Enum
from threading import Condition, RLock
from types import TracebackType
from typing import Generic, TypeVar

from lifecycle.resource.recovery import ResourceRecoveryPool
from lifecycle.resource.result import (
    ReleaseReport,
    ResourceAllocationError,
    ScopeClosedError,
)


T = TypeVar("T")
U = TypeVar("U")


@dataclass(frozen=True, slots=True)
class _CloseResult:
    report: ReleaseReport
    interruption: BaseException | None = None


class _CleanupBatch:
    """One acquisition's ExitStack; ownership transfers only on commit."""

    __slots__ = ("_stack", "_pool", "_errors", "_interruption")

    def __init__(self, pool: ResourceRecoveryPool) -> None:
        self._stack = ExitStack()
        self._pool = pool
        self._errors: list[BaseException] = []
        self._interruption: BaseException | None = None

    def register(self, resource: object, release: Callable[[], None]) -> None:
        def finalize() -> None:
            try:
                release()
            except BaseException as exc:
                self._errors.append(exc)
                self._pool._accept(resource, release, exc)
                if not isinstance(exc, Exception) and self._interruption is None:
                    self._interruption = exc

        # Catch inside each callback so ExitStack always unwinds the whole batch,
        # without replacing the first control-flow interruption with a later one.
        self._stack.callback(finalize)

    def close(self) -> _CloseResult:
        self._stack.close()
        return _CloseResult(
            ReleaseReport(tuple(self._errors), len(self._errors)),
            self._interruption,
        )


class Resource(Generic[T]):
    """A reusable, lazy description of acquisition and finalization."""

    __slots__ = ("_allocate_into",)

    def __init__(self, allocate_into: Callable[[_CleanupBatch], T]) -> None:
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

        def allocate(batch: _CleanupBatch) -> T:
            value = acquire()

            def finalize() -> None:
                release(value)

            # Register each successful step before running a map or flat_map callback.
            batch.register(value, finalize)
            return value

        return cls(allocate)

    @classmethod
    def pure(cls, value: T) -> "Resource[T]":
        return cls(lambda _batch: value)

    def map(self, project: Callable[[T], U]) -> "Resource[U]":
        if not callable(project):
            raise TypeError("project must be callable")

        def allocate(batch: _CleanupBatch) -> U:
            return project(self._allocate_into(batch))

        return Resource(allocate)

    def flat_map(self, bind: Callable[[T], "Resource[U]"]) -> "Resource[U]":
        if not callable(bind):
            raise TypeError("bind must be callable")

        def allocate(batch: _CleanupBatch) -> U:
            next_resource = bind(self._allocate_into(batch))
            if not isinstance(next_resource, Resource):
                raise TypeError("flat_map must return Resource")
            return next_resource._allocate_into(batch)

        return Resource(allocate)


class _ScopeState(Enum):
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


class ResourceScope:
    """Own committed acquisition batches and dynamically registered child scopes.

    A tree shares one control condition. User callbacks always run outside it.
    Closing finishes cleanup or transfers responsibility; it never reopens a scope.
    """

    __slots__ = (
        "_batches",
        "_children",
        "_condition",
        "_entered",
        "_inflight",
        "_parent",
        "_pool",
        "_release_report",
        "_shutdown_report",
        "_state",
    )

    def __init__(self, pool: ResourceRecoveryPool) -> None:
        if not isinstance(pool, ResourceRecoveryPool):
            raise TypeError("pool must be a ResourceRecoveryPool")
        self._pool = pool
        self._condition = Condition(RLock())
        self._state = _ScopeState.OPEN
        self._batches: list[_CleanupBatch] = []
        self._children: list[ResourceScope] = []
        self._inflight = 0
        self._parent: ResourceScope | None = None
        self._release_report: ReleaseReport | None = None
        self._shutdown_report = ReleaseReport()
        self._entered = False

    @property
    def closed(self) -> bool:
        with self._condition:
            return self._state is _ScopeState.CLOSED

    def _is_open_locked(self) -> bool:
        scope: ResourceScope | None = self
        while scope is not None:
            if scope._state is not _ScopeState.OPEN:
                return False
            scope = scope._parent
        return True

    def _ensure_open_locked(self) -> None:
        if not self._is_open_locked():
            raise ScopeClosedError("scope or an ancestor is closing or closed")

    def child(self) -> "ResourceScope":
        with self._condition:
            self._ensure_open_locked()
            child = ResourceScope(self._pool)
            child._condition = self._condition
            child._parent = self
            self._children.append(child)
            return child

    def acquire(self, resource: Resource[T]) -> T:
        if not isinstance(resource, Resource):
            raise TypeError("resource must be a Resource")

        with self._condition:
            try:
                self._ensure_open_locked()
            except ScopeClosedError as exc:
                raise ResourceAllocationError(exc, ReleaseReport()) from exc
            self._inflight += 1

        batch = _CleanupBatch(self._pool)
        try:
            value = resource._allocate_into(batch)
            with self._condition:
                self._ensure_open_locked()
                self._batches.append(batch)
                self._inflight -= 1
                self._condition.notify_all()
                return value
        except BaseException as exc:
            result = batch.close()
            with self._condition:
                # Only rollback still in flight when shutdown began belongs to that
                # closing round. An ancestor may be waiting on an earlier sibling.
                if not self._is_open_locked():
                    self._shutdown_report = self._shutdown_report.merged(result.report)
                self._inflight -= 1
                self._condition.notify_all()

            if not isinstance(exc, Exception):
                raise
            if result.interruption is not None:
                raise result.interruption from exc
            raise ResourceAllocationError(exc, result.report) from exc

    def close(self) -> ReleaseReport:
        result = self._close()
        if result.interruption is not None:
            raise result.interruption
        return result.report

    def _close(self) -> _CloseResult:
        """Complete and cache before returning an interruption to the initiator."""
        with self._condition:
            if self._state is _ScopeState.CLOSED:
                assert self._release_report is not None
                return _CloseResult(self._release_report)

            if self._state is _ScopeState.CLOSING:
                interruption = self._wait_locked(
                    lambda: self._state is _ScopeState.CLOSED
                )
                assert self._release_report is not None
                return _CloseResult(self._release_report, interruption)

            self._state = _ScopeState.CLOSING
            # Capture at admission, before waiting: a child that finishes while we
            # wait still belongs to this round even though it detaches from us.
            children = list(reversed(self._children))
            self._condition.notify_all()
            interruption = self._wait_locked(lambda: self._inflight == 0)
            combined = self._shutdown_report
            batches = list(reversed(self._batches))
            self._batches.clear()

        for child in children:
            result = child._close()
            combined = combined.merged(result.report)
            if interruption is None:
                interruption = result.interruption

        for batch in batches:
            result = batch.close()
            combined = combined.merged(result.report)
            if interruption is None:
                interruption = result.interruption

        with self._condition:
            self._release_report = combined
            self._state = _ScopeState.CLOSED
            if self._parent is not None:
                # An ancestor can be closing while our immediate parent is still
                # waiting its turn. Preserve this round's report before detaching;
                # a CLOSING parent already captured us and will merge it itself.
                if (
                    self._parent._state is _ScopeState.OPEN
                    and not self._parent._is_open_locked()
                ):
                    self._parent._shutdown_report = self._parent._shutdown_report.merged(
                        combined
                    )
                self._parent._children.remove(self)
                self._parent = None
            self._condition.notify_all()

        return _CloseResult(combined, interruption)

    def _wait_locked(self, completed: Callable[[], bool]) -> BaseException | None:
        interruption: BaseException | None = None
        while not completed():
            try:
                self._condition.wait()
            except BaseException as exc:
                if isinstance(exc, Exception):
                    raise
                # A control-flow signal while waiting must not strand CLOSING or
                # skip dependencies. Deliver it once this caller's wait completes.
                if interruption is None:
                    interruption = exc
        return interruption

    def __enter__(self) -> "ResourceScope":
        with self._condition:
            self._ensure_open_locked()
            if self._entered:
                raise RuntimeError("ResourceScope context manager cannot be re-entered")
            self._entered = True
            return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        result = self._close()
        # The body's control-flow interruption has priority over cleanup's. An
        # ordinary body exception remains the cause if cleanup interrupts instead.
        if result.interruption is not None and (exc is None or isinstance(exc, Exception)):
            raise result.interruption from exc
        return False


__all__ = ["Resource", "ResourceScope"]
