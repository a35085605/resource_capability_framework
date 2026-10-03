from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast


T = TypeVar("T")
U = TypeVar("U")


class IO(Generic[T]):
    """An immutable description of a synchronous computation.

    Effects run on the caller's thread and are never memoized. Error handlers catch
    Exception only; guarantee runs even for control-flow BaseException signals.
    This is not a scheduler or a cooperative cancellation runtime.
    """

    __slots__ = ()

    @staticmethod
    def pure(value: T) -> IO[T]:
        """Lift an already evaluated value; use delay to suspend side effects."""
        return _Pure(value)

    @staticmethod
    def delay(thunk: Callable[[], T]) -> IO[T]:
        if not callable(thunk):
            raise TypeError("thunk must be callable")
        return _Delay(thunk)

    @staticmethod
    def defer(factory: Callable[[], IO[T]]) -> IO[T]:
        if not callable(factory):
            raise TypeError("factory must be callable")
        return _Defer(factory)

    @staticmethod
    def raise_error(error: BaseException) -> IO[Any]:
        if not isinstance(error, BaseException):
            raise TypeError("error must be a BaseException")
        return _Raise(error)

    def map(self, project: Callable[[T], U]) -> IO[U]:
        if not callable(project):
            raise TypeError("project must be callable")
        return self.flat_map(lambda value: IO.pure(project(value)))

    def flat_map(self, bind: Callable[[T], IO[U]]) -> IO[U]:
        if not callable(bind):
            raise TypeError("bind must be callable")
        return _Bind(self, bind)

    def handle_error_with(self, recover: Callable[[Exception], IO[T]]) -> IO[T]:
        if not callable(recover):
            raise TypeError("recover must be callable")
        return _Handle(self, recover)

    def guarantee(self, finalizer: IO[None]) -> IO[T]:
        _require_io(finalizer)
        return _Guarantee(self, finalizer)

    def _capture(self) -> IO[_Exit[T]]:
        """Internal lifecycle observation, including control-flow interruptions."""
        return _Capture(self)


@dataclass(frozen=True, slots=True, eq=False)
class _Pure(IO[T]):
    value: T


@dataclass(frozen=True, slots=True, eq=False)
class _Delay(IO[T]):
    thunk: Callable[[], T]


@dataclass(frozen=True, slots=True, eq=False)
class _Defer(IO[T]):
    factory: Callable[[], IO[T]]


@dataclass(frozen=True, slots=True, eq=False)
class _Raise(IO[Any]):
    error: BaseException


@dataclass(frozen=True, slots=True, eq=False)
class _Bind(IO[T]):
    source: IO[Any]
    bind: Callable[[Any], IO[T]]


@dataclass(frozen=True, slots=True, eq=False)
class _Handle(IO[T]):
    source: IO[T]
    recover: Callable[[Exception], IO[T]]


@dataclass(frozen=True, slots=True, eq=False)
class _Guarantee(IO[T]):
    source: IO[T]
    finalizer: IO[None]


@dataclass(frozen=True, slots=True)
class _Exit(Generic[T]):
    value: T | None = None
    error: BaseException | None = None


@dataclass(frozen=True, slots=True, eq=False)
class _Capture(IO[_Exit[T]]):
    source: IO[T]


@dataclass(frozen=True, slots=True)
class _Restore:
    value: Any
    error: BaseException | None


def _require_io(value: IO[T]) -> IO[T]:
    if not isinstance(value, IO):
        raise TypeError("effect callback must return IO")
    return value


def _combine_errors(
    original: BaseException | None, cleanup: BaseException | None
) -> BaseException | None:
    if original is None:
        return cleanup
    if cleanup is None or cleanup is original:
        return original
    if isinstance(original, Exception) and not isinstance(cleanup, Exception):
        primary, secondary = cleanup, original
    else:
        primary, secondary = original, cleanup

    # Keep an existing cause chain, and avoid cycles when exception values are reused.
    seen: set[int] = set()
    tail = primary
    while True:
        if id(tail) in seen or tail is secondary:
            return primary
        seen.add(id(tail))
        if tail.__cause__ is None:
            break
        tail = tail.__cause__
    other: BaseException | None = secondary
    while other is not None:
        if id(other) in seen:
            return primary
        seen.add(id(other))
        other = other.__cause__
    tail.__cause__ = secondary
    tail.__suppress_context__ = True
    return primary


def run_sync(effect: IO[T]) -> T:
    """Evaluate an IO on this thread, with an execution-local continuation stack."""
    current: IO[Any] | None = _require_io(effect)
    frames: list[_Bind[Any] | _Handle[Any] | _Guarantee[Any] | _Capture[Any] | _Restore] = []
    value: Any = None
    error: BaseException | None = None

    while True:
        if current is not None:
            try:
                if isinstance(current, _Pure):
                    value, error = current.value, None
                elif isinstance(current, _Delay):
                    value, error = current.thunk(), None
                elif isinstance(current, _Defer):
                    current = _require_io(current.factory())
                    continue
                elif isinstance(current, _Raise):
                    value, error = None, current.error
                elif isinstance(current, (_Bind, _Handle, _Guarantee, _Capture)):
                    frames.append(current)
                    current = current.source
                    continue
                else:
                    raise TypeError("unsupported IO description")
            except BaseException as exc:
                value, error = None, exc
            current = None

        if not frames:
            if error is not None:
                raise error
            return cast(T, value)

        frame = frames.pop()
        try:
            if isinstance(frame, _Bind):
                if error is None:
                    current = _require_io(frame.bind(value))
            elif isinstance(frame, _Handle):
                if isinstance(error, Exception):
                    current = _require_io(frame.recover(error))
                    value, error = None, None
            elif isinstance(frame, _Guarantee):
                frames.append(_Restore(value, error))
                current = frame.finalizer
                value, error = None, None
            elif isinstance(frame, _Restore):
                value, error = frame.value, _combine_errors(frame.error, error)
            elif isinstance(frame, _Capture):
                value, error = _Exit(value, error), None
        except BaseException as exc:
            value, error = None, exc


__all__ = ["IO", "run_sync"]
