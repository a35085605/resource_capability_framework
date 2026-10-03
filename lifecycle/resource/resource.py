from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from threading import Condition, RLock
from typing import Any, Generic, TypeVar, cast

from lifecycle.effect import IO
from lifecycle.effect.io import _Exit
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

    def merged(self, other: _CloseResult) -> _CloseResult:
        return _CloseResult(
            self.report.merged(other.report),
            self.interruption if self.interruption is not None else other.interruption,
        )


class _CleanupBatch:
    """One acquisition's finalizers; ownership transfers only on commit."""

    __slots__ = ("_finalizers", "_pool")

    def __init__(self, pool: ResourceRecoveryPool) -> None:
        self._finalizers: list[tuple[object, IO[None]]] = []
        self._pool = pool

    def register(self, resource: object, release: IO[None]) -> None:
        self._finalizers.append((resource, release))

    def close(self) -> IO[_CloseResult]:
        def begin() -> IO[_CloseResult]:
            pending = list(reversed(self._finalizers))
            self._finalizers.clear()
            errors: list[BaseException] = []
            interruption: BaseException | None = None

            def record(resource: object, release: IO[None], result: _Exit[None]) -> None:
                nonlocal interruption
                if result.error is not None:
                    errors.append(result.error)
                    self._pool._accept(resource, release, result.error)
                    if not isinstance(result.error, Exception) and interruption is None:
                        interruption = result.error

            cleanup = IO.pure(None)
            for resource, release in pending:
                cleanup = cleanup.flat_map(
                    lambda _, resource=resource, release=release: release._capture().map(
                        lambda result: record(resource, release, result)
                    )
                )
            return cleanup.map(lambda _: _CloseResult(
                ReleaseReport(tuple(errors), len(errors)), interruption
            ))

        return IO.defer(begin)


class Resource(Generic[T]):
    """An immutable, reusable description of acquiring and finalizing a value."""

    __slots__ = ()

    @classmethod
    def make(
        cls,
        acquire: IO[T],
        release: Callable[[T], IO[None]],
    ) -> Resource[T]:
        if not isinstance(acquire, IO):
            raise TypeError("acquire must be IO")
        if not callable(release):
            raise TypeError("release must be callable")
        return _Allocate(acquire, release)

    @classmethod
    def pure(cls, value: T) -> Resource[T]:
        return _PureResource(value)

    @classmethod
    def eval(cls, effect: IO[T]) -> Resource[T]:
        if not isinstance(effect, IO):
            raise TypeError("effect must be IO")
        return _EvalResource(effect)

    def map(self, project: Callable[[T], U]) -> Resource[U]:
        if not callable(project):
            raise TypeError("project must be callable")
        return self.flat_map(lambda value: Resource.pure(project(value)))

    def flat_map(self, bind: Callable[[T], Resource[U]]) -> Resource[U]:
        if not callable(bind):
            raise TypeError("bind must be callable")
        return _BindResource(self, bind)

    def eval_map(self, project: Callable[[T], IO[U]]) -> Resource[U]:
        if not callable(project):
            raise TypeError("project must be callable")
        return self.flat_map(lambda value: Resource.eval(project(value)))

    def use(
        self,
        body: Callable[[T], IO[U]],
        *,
        pool: ResourceRecoveryPool,
    ) -> IO[U]:
        if not callable(body):
            raise TypeError("body must be callable")
        if not isinstance(pool, ResourceRecoveryPool):
            raise TypeError("pool must be a ResourceRecoveryPool")
        return IO.defer(lambda: ResourceScope(pool).use(
            lambda scope: scope.acquire(self).flat_map(body)
        ))

    def _allocate_into(self, batch: _CleanupBatch) -> IO[T]:
        def begin() -> IO[T]:
            binds: list[Callable[[Any], Resource[Any]]] = []

            def advance(value: Any) -> IO[Any]:
                if not binds:
                    return IO.pure(value)
                bind = binds.pop()
                return IO.defer(lambda: descend(bind(value)))

            def descend(resource: Resource[Any]) -> IO[Any]:
                if not isinstance(resource, Resource):
                    raise TypeError("flat_map must return Resource")
                while isinstance(resource, _BindResource):
                    binds.append(resource.bind)
                    resource = resource.source
                if isinstance(resource, _PureResource):
                    effect = IO.pure(resource.value)
                elif isinstance(resource, _EvalResource):
                    effect = resource.effect
                elif isinstance(resource, _Allocate):
                    allocation = resource

                    def register(value: Any) -> Any:
                        # Defer the release factory too: construction failures are
                        # finalizer failures, and recovery can rebuild the effect.
                        batch.register(value, IO.defer(lambda: allocation.release(value)))
                        return value

                    effect = allocation.acquire.map(register)
                else:
                    raise TypeError("unsupported Resource description")
                return effect.flat_map(advance)

            return descend(self)

        return IO.defer(begin)


@dataclass(frozen=True, slots=True, eq=False)
class _Allocate(Resource[T]):
    acquire: IO[T]
    release: Callable[[T], IO[None]]


@dataclass(frozen=True, slots=True, eq=False)
class _PureResource(Resource[T]):
    value: T


@dataclass(frozen=True, slots=True, eq=False)
class _EvalResource(Resource[T]):
    effect: IO[T]


@dataclass(frozen=True, slots=True, eq=False)
class _BindResource(Resource[T]):
    source: Resource[Any]
    bind: Callable[[Any], Resource[T]]


class _ScopeState(Enum):
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


class ResourceScope:
    """Own acquisition batches and dynamically registered child scopes.

    A tree shares one control condition. All public operations describe IO; user
    functions execute outside the lock. Closure finalizes or transfers responsibility.
    """

    __slots__ = (
        "_batches", "_children", "_condition", "_entered", "_inflight", "_parent",
        "_pool", "_release_report", "_shutdown_report", "_state",
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

    def is_closed(self) -> IO[bool]:
        def observe() -> bool:
            with self._condition:
                return self._is_closed_locked()

        return IO.delay(observe)

    def _is_closed_locked(self) -> bool:
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

    def _child_locked(self) -> ResourceScope:
        self._ensure_open_locked()
        child = ResourceScope(self._pool)
        child._condition = self._condition
        child._parent = self
        self._children.append(child)
        return child

    def child(self) -> IO[ResourceScope]:
        def register() -> ResourceScope:
            with self._condition:
                return self._child_locked()

        return IO.delay(register)

    def acquire(self, resource: Resource[T]) -> IO[T]:
        if not isinstance(resource, Resource):
            raise TypeError("resource must be a Resource")

        def begin() -> IO[T]:
            with self._condition:
                try:
                    self._ensure_open_locked()
                except ScopeClosedError as exc:
                    raise ResourceAllocationError(exc, ReleaseReport()) from exc
                self._inflight += 1
            batch = _CleanupBatch(self._pool)

            def commit(value: T) -> T:
                with self._condition:
                    self._ensure_open_locked()
                    self._batches.append(batch)
                    self._inflight -= 1
                    self._condition.notify_all()
                    return value

            def finish(outcome: _Exit[T]) -> IO[T]:
                if outcome.error is None:
                    return IO.pure(cast(T, outcome.value))
                error = outcome.error

                def rollback(result: _CloseResult) -> T:
                    with self._condition:
                        # Only rollback in flight when shutdown began belongs to
                        # that round, including when an ancestor closed first.
                        if not self._is_open_locked():
                            self._shutdown_report = self._shutdown_report.merged(result.report)
                        self._inflight -= 1
                        self._condition.notify_all()
                    if not isinstance(error, Exception):
                        raise error
                    if result.interruption is not None:
                        raise result.interruption from error
                    raise ResourceAllocationError(error, result.report) from error

                return batch.close().map(rollback)

            return resource._allocate_into(batch).map(commit)._capture().flat_map(finish)

        return IO.defer(begin)

    def close(self) -> IO[ReleaseReport]:
        def report(result: _CloseResult) -> ReleaseReport:
            if result.interruption is not None:
                raise result.interruption
            return result.report

        return self._close().map(report)

    def _close(self) -> IO[_CloseResult]:
        """Cache closure before delivering an interruption to its initiating run."""
        def begin() -> IO[_CloseResult]:
            with self._condition:
                if self._state is _ScopeState.CLOSED:
                    assert self._release_report is not None
                    return IO.pure(_CloseResult(self._release_report))
                if self._state is _ScopeState.CLOSING:
                    interruption = self._wait_locked(
                        lambda: self._state is _ScopeState.CLOSED
                    )
                    assert self._release_report is not None
                    return IO.pure(_CloseResult(self._release_report, interruption))

                self._state = _ScopeState.CLOSING
                # Capture children before waiting: one may detach during this round.
                children = list(reversed(self._children))
                self._condition.notify_all()
                interruption = self._wait_locked(lambda: self._inflight == 0)
                initial = _CloseResult(self._shutdown_report, interruption)
                batches = list(reversed(self._batches))
                self._batches.clear()

            cleanup = IO.pure(initial)
            for child in children:
                cleanup = cleanup.flat_map(
                    lambda combined, child=child: child._close().map(combined.merged)
                )
            for batch in batches:
                cleanup = cleanup.flat_map(
                    lambda combined, batch=batch: batch.close().map(combined.merged)
                )
            return cleanup.map(self._complete_close)

        return IO.defer(begin)

    def _complete_close(self, result: _CloseResult) -> _CloseResult:
        with self._condition:
            self._release_report = result.report
            self._state = _ScopeState.CLOSED
            if self._parent is not None:
                # An OPEN immediate parent may still await its turn under a closing
                # ancestor. Preserve the report before detaching in that case.
                if (
                    self._parent._state is _ScopeState.OPEN
                    and not self._parent._is_open_locked()
                ):
                    self._parent._shutdown_report = self._parent._shutdown_report.merged(
                        result.report
                    )
                self._parent._children.remove(self)
                self._parent = None
            self._condition.notify_all()
        return result

    def _wait_locked(self, completed: Callable[[], bool]) -> BaseException | None:
        interruption: BaseException | None = None
        while not completed():
            try:
                self._condition.wait()
            except BaseException as exc:
                if isinstance(exc, Exception):
                    raise
                # Do not strand CLOSING or skip dependencies after a signal.
                if interruption is None:
                    interruption = exc
        return interruption

    def use(self, body: Callable[[ResourceScope], IO[T]]) -> IO[T]:
        if not callable(body):
            raise TypeError("body must be callable")

        def enter() -> None:
            with self._condition:
                self._ensure_open_locked()
                if self._entered:
                    raise RuntimeError("ResourceScope use cannot be re-entered")
                self._entered = True

        # Install cleanup only after entry succeeds; a failed re-entry cannot close
        # the scope owned by an already running use.
        return IO.delay(enter).flat_map(lambda _: IO.defer(lambda: body(self)).guarantee(
            self.close().map(lambda _: None)
        ))


__all__ = ["Resource", "ResourceScope"]
