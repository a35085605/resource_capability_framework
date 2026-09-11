from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from threading import Lock
from typing import Generic, Protocol, TypeVar

from _attempt import AttemptId
from _resource.driver import PhysicalAcquisition, ResourceDriver, ResourceSet
from _resource.pool import GLOBAL_RESOURCE_POOL, ResourcePool, RetiredResource
from _resource.requirement import ResourceRequirements
from _resource.result import (
    ResourceAcquireResult,
    ResourceAcquired,
    ResourceBlocked,
    ResourceFailed,
)


AccessT = TypeVar("AccessT")
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")


class ResourceRequirementsModel(Protocol[AccessT, SpecT]):
    """Resolve only the physical Resource requirements for one Access."""

    def requirements(self, access: AccessT) -> ResourceRequirements[SpecT]: ...


class ResourceManagement(Protocol[AccessT, ResourceT]):
    """Managed-facing boundary for the complete Resource acquisition lifecycle.

    ``open_attempt`` registers an opaque identity before Managed publishes it, so a
    concurrent ``release`` cannot be lost.  Acquired resources remain physically
    pinned until ``finish_acquire`` even if ``release`` is requested concurrently.
    """

    def open_attempt(self) -> AttemptId: ...

    def acquire(
        self,
        attempt_id: AttemptId,
        access: AccessT,
    ) -> ResourceAcquireResult[ResourceT]: ...

    def release(self, attempt_id: AttemptId) -> None: ...

    def finish_acquire(self, attempt_id: AttemptId) -> None: ...

    def cleanup_retired(self, access: AccessT) -> bool: ...


class ResourceAcquisitionCancelled(RuntimeError):
    """Internal operational failure used when an acquisition is cancelled."""


@dataclass(slots=True)
class _AcquisitionPart(Generic[SpecT, ResourceT]):
    spec: SpecT
    physical: PhysicalAcquisition[ResourceT]
    resources: ResourceSet[ResourceT] | None = None
    finished: bool = False


@dataclass(slots=True)
class _AttemptContext(Generic[AccessT, SpecT, ResourceT]):
    attempt_id: AttemptId
    access: AccessT
    requirements: ResourceRequirements[SpecT] | None = None
    parts: list[_AcquisitionPart[SpecT, ResourceT]] = field(default_factory=list)
    resources: ResourceSet[ResourceT] | None = None


@dataclass(frozen=True, slots=True)
class _Pending:
    attempt_id: AttemptId


@dataclass(frozen=True, slots=True)
class _Cancelled:
    attempt_id: AttemptId


@dataclass(slots=True)
class _Acquiring(Generic[AccessT, SpecT, ResourceT]):
    context: _AttemptContext[AccessT, SpecT, ResourceT]


@dataclass(slots=True)
class _Cancelling(Generic[AccessT, SpecT, ResourceT]):
    context: _AttemptContext[AccessT, SpecT, ResourceT]


@dataclass(slots=True)
class _Pinned(Generic[AccessT, SpecT, ResourceT]):
    context: _AttemptContext[AccessT, SpecT, ResourceT]


@dataclass(slots=True)
class _Retained(Generic[AccessT, SpecT, ResourceT]):
    context: _AttemptContext[AccessT, SpecT, ResourceT]


@dataclass(slots=True)
class _RetiredPinned(Generic[AccessT, SpecT, ResourceT]):
    context: _AttemptContext[AccessT, SpecT, ResourceT]
    retired: RetiredResource[AccessT, SpecT, ResourceT]


@dataclass(slots=True)
class _Retired(Generic[AccessT, SpecT, ResourceT]):
    context: _AttemptContext[AccessT, SpecT, ResourceT] | None
    retired: RetiredResource[AccessT, SpecT, ResourceT]
    cleanup_in_progress: bool = False


type _AttemptState[A, S, R] = (
    _Pending
    | _Cancelled
    | _Acquiring[A, S, R]
    | _Cancelling[A, S, R]
    | _Pinned[A, S, R]
    | _Retained[A, S, R]
    | _RetiredPinned[A, S, R]
    | _Retired[A, S, R]
)


class ResourceManager(Generic[AccessT, SpecT, ResourceT]):
    """Own the Resource attempt state machine, physical I/O, and cleanup."""

    def __init__(
        self,
        requirements_model: ResourceRequirementsModel[AccessT, SpecT],
        driver: ResourceDriver[SpecT, ResourceT],
        *,
        resource_pool: ResourcePool[AccessT, SpecT, ResourceT] = GLOBAL_RESOURCE_POOL,
    ) -> None:
        self._requirements_model = requirements_model
        self._driver = driver
        if not isinstance(resource_pool, ResourcePool):
            raise TypeError("resource_pool must be ResourcePool")
        self._resource_pool = resource_pool
        self._owner = object()
        self._lock = Lock()
        self._attempts: dict[
            AttemptId, _AttemptState[AccessT, SpecT, ResourceT]
        ] = {}

    @property
    def requirements_model(self) -> ResourceRequirementsModel[AccessT, SpecT]:
        return self._requirements_model

    @property
    def driver(self) -> ResourceDriver[SpecT, ResourceT]:
        return self._driver

    @property
    def resource_pool(self) -> ResourcePool[AccessT, SpecT, ResourceT]:
        return self._resource_pool

    def open_attempt(self) -> AttemptId:
        """Create and register one Pending acquisition identity."""

        attempt_id = AttemptId()
        with self._lock:
            self._attempts[attempt_id] = _Pending(attempt_id)
        return attempt_id

    def acquire(
        self,
        attempt_id: AttemptId,
        access: AccessT,
    ) -> ResourceAcquireResult[ResourceT]:
        self._validate_attempt_id(attempt_id)
        if access is None:
            raise TypeError("access cannot be None")

        with self._lock:
            state = self._attempts.get(attempt_id)
            if isinstance(state, _Cancelled):
                return ResourceFailed(
                    ResourceAcquisitionCancelled("attempt was cancelled")
                )
            if state is None:
                raise RuntimeError("attempt is not open in this manager")
            if not isinstance(state, _Pending):
                raise RuntimeError("attempt can begin only from Pending")

            context = _AttemptContext[AccessT, SpecT, ResourceT](attempt_id, access)
            acquiring = _Acquiring(context)
            self._attempts[attempt_id] = acquiring

        requirements = self._requirements_model.requirements(access)

        with self._lock:
            current = self._attempts.get(attempt_id)
            if isinstance(current, _Cancelling) and current.context is context:
                return ResourceFailed(
                    ResourceAcquisitionCancelled("attempt was cancelled")
                )
            if current is not acquiring:
                raise RuntimeError("resource attempt registry changed unexpectedly")

            context.requirements = requirements
            reserved = self._resource_pool.reserve(
                self._owner,
                attempt_id,
                access,
                requirements,
            )
            if not reserved:
                self._attempts.pop(attempt_id, None)
                return ResourceBlocked()

        try:
            prepare_failure = self._prepare_parts(context)
            if prepare_failure is not None:
                self._fail_attempt(context, prepare_failure)
                return ResourceFailed(prepare_failure)

            if self._is_cancelling(context):
                self._cancel_before_resources(context)
                return ResourceFailed(
                    ResourceAcquisitionCancelled("attempt was cancelled")
                )

            acquire_failure = self._acquire_parts(context)
            if acquire_failure is not None:
                cleanup_failure = self._fail_attempt(context, acquire_failure)
                if cleanup_failure is not None:
                    acquire_failure.add_note(
                        f"resource cleanup also failed: {cleanup_failure!r}"
                    )
                return ResourceFailed(acquire_failure)

            resources = self._current_resources(context)
            context.resources = resources

            retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None
            with self._lock:
                current = self._attempts.get(attempt_id)
                if isinstance(current, _Cancelling) and current.context is context:
                    self._resource_pool.release(self._owner, attempt_id)
                    retired = self._resource_pool.finish(
                        self._owner,
                        attempt_id,
                        resources,
                    )
                    if retired is not None:
                        self._attempts[attempt_id] = _Retired(context, retired)
                    else:
                        self._attempts.pop(attempt_id, None)
                elif current is acquiring:
                    retired = self._resource_pool.finish(
                        self._owner,
                        attempt_id,
                        resources,
                    )
                    if retired is not None:
                        self._attempts[attempt_id] = _Retired(context, retired)
                    else:
                        self._resource_pool.install(self._owner, attempt_id)
                        self._attempts[attempt_id] = _Pinned(context)
                        return ResourceAcquired(resources)
                else:
                    raise RuntimeError("resource attempt registry changed unexpectedly")

            if retired is not None:
                self._cleanup_retired_attempt(retired)
            return ResourceFailed(
                ResourceAcquisitionCancelled("attempt was cancelled")
            )

        except BaseException:
            # Contract/invariant errors are not normalized into ResourceFailed, but
            # any partial physical resources still remain ResourceManager-owned.
            self._fail_attempt(context, None)
            raise

    def release(self, attempt_id: AttemptId) -> None:
        self._validate_attempt_id(attempt_id)

        retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None
        physicals: list[PhysicalAcquisition[ResourceT]] = []
        cleanup_now = False

        with self._lock:
            state = self._attempts.get(attempt_id)
            if state is None:
                return
            if isinstance(state, _Pending):
                self._attempts[attempt_id] = _Cancelled(attempt_id)
                return
            if isinstance(state, _Cancelled):
                return

            if isinstance(state, _Acquiring):
                context = state.context
                self._attempts[attempt_id] = _Cancelling(context)
                release = self._resource_pool.release(self._owner, attempt_id)
                retired = release.retired
                physicals = [
                    part.physical for part in context.parts if not part.finished
                ]
                if retired is not None:
                    self._attempts[attempt_id] = _Retired(context, retired)
                    cleanup_now = True
            elif isinstance(state, _Cancelling):
                context = state.context
                release = self._resource_pool.release(self._owner, attempt_id)
                retired = release.retired
                physicals = [
                    part.physical for part in context.parts if not part.finished
                ]
                if retired is not None:
                    self._attempts[attempt_id] = _Retired(context, retired)
                    cleanup_now = True
            elif isinstance(state, _Pinned):
                release = self._resource_pool.release(self._owner, attempt_id)
                retired = release.retired
                if retired is None:
                    raise RuntimeError("pinned resource attempt was not retained")
                self._attempts[attempt_id] = _RetiredPinned(state.context, retired)
            elif isinstance(state, _Retained):
                release = self._resource_pool.release(self._owner, attempt_id)
                retired = release.retired
                if retired is None:
                    raise RuntimeError("retained resource attempt was not retained")
                self._attempts[attempt_id] = _Retired(state.context, retired)
                cleanup_now = True
            elif isinstance(state, _RetiredPinned):
                return
            elif isinstance(state, _Retired):
                retired = state.retired
                cleanup_now = not state.cleanup_in_progress
            else:
                raise RuntimeError("unsupported Resource attempt state")

        first_interrupt_error: Exception | None = None
        for physical in physicals:
            try:
                physical.interrupt()
            except Exception as exc:
                if first_interrupt_error is None:
                    first_interrupt_error = exc

        cleanup_error: Exception | None = None
        if cleanup_now and retired is not None:
            cleanup_error = self._cleanup_retired_attempt(retired)

        if first_interrupt_error is not None:
            if cleanup_error is not None:
                first_interrupt_error.add_note(
                    f"resource cleanup also failed: {cleanup_error!r}"
                )
            raise first_interrupt_error
        if cleanup_error is not None:
            raise cleanup_error

    def finish_acquire(self, attempt_id: AttemptId) -> None:
        """End the temporary ResourceSet borrow used for capability projection."""

        self._validate_attempt_id(attempt_id)
        retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None
        with self._lock:
            state = self._attempts.get(attempt_id)
            if state is None:
                return
            if isinstance(state, _Cancelled):
                self._attempts.pop(attempt_id, None)
                return
            if isinstance(state, _Pinned):
                self._attempts[attempt_id] = _Retained(state.context)
                return
            if isinstance(state, _RetiredPinned):
                retired = state.retired
                self._attempts[attempt_id] = _Retired(state.context, retired)
            elif isinstance(state, _Cancelling) and not self._pool_has_attempt(attempt_id):
                self._attempts.pop(attempt_id, None)
                return

        if retired is not None:
            # A finally-path must not replace the Managed acquire result.  Failed
            # cleanup stays retained in the Pool for cleanup_retired().
            self._cleanup_retired_attempt(retired)

    def cleanup_retired(self, access: AccessT) -> bool:
        if access is None:
            raise TypeError("access cannot be None")
        requirements = self._requirements_model.requirements(access)
        retired_records = self._resource_pool.retired(
            self._owner,
            access,
            requirements,
        )
        if not retired_records:
            return False

        for retired in retired_records:
            error = self._cleanup_retired_attempt(retired)
            if error is not None:
                raise error
        return True

    def _prepare_parts(
        self,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
    ) -> Exception | None:
        requirements = context.requirements
        if requirements is None:
            raise RuntimeError("resource requirements are not resolved")

        for requirement in requirements:
            if self._is_cancelling(context):
                return ResourceAcquisitionCancelled("attempt was cancelled")
            try:
                physical = self._driver.prepare(requirement.spec)
            except Exception as exc:
                return exc
            if physical is None:
                raise TypeError("ResourceDriver.prepare() cannot return None")
            with self._lock:
                context.parts.append(_AcquisitionPart(requirement.spec, physical))
            if self._is_cancelling(context):
                try:
                    physical.interrupt()
                except Exception:
                    pass
                return ResourceAcquisitionCancelled("attempt was cancelled")
        return None

    def _acquire_parts(
        self,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
    ) -> Exception | None:
        if not context.parts:
            resources: ResourceSet[ResourceT] = ()
            self._resource_pool.publish(self._owner, context.attempt_id, resources)
            return None

        for part in context.parts:
            if self._is_cancelling(context):
                return ResourceAcquisitionCancelled("attempt was cancelled")
            try:
                snapshots = part.physical.acquire()
            except Exception as exc:
                part.finished = True
                return exc
            if not isinstance(snapshots, Iterator):
                part.finished = True
                raise TypeError("PhysicalAcquisition.acquire() must return an Iterator")

            yielded = False
            try:
                while True:
                    try:
                        snapshot = next(snapshots)
                    except StopIteration:
                        break
                    except Exception as exc:
                        return exc

                    yielded = True
                    if snapshot is None:
                        raise TypeError("PhysicalAcquisition.acquire() cannot yield None")
                    if not isinstance(snapshot, tuple):
                        raise TypeError(
                            "PhysicalAcquisition.acquire() must yield ResourceSet tuples"
                        )
                    part.resources = snapshot
                    self._resource_pool.publish(
                        self._owner,
                        context.attempt_id,
                        self._current_resources(context),
                    )
                    if self._is_cancelling(context):
                        return ResourceAcquisitionCancelled("attempt was cancelled")
            finally:
                part.finished = True

            if not yielded:
                if self._is_cancelling(context):
                    return ResourceAcquisitionCancelled("attempt was cancelled")
                raise RuntimeError(
                    "PhysicalAcquisition.acquire() must yield at least one ResourceSet"
                )
        return None

    def _fail_attempt(
        self,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
        primary_error: Exception | None,
    ) -> Exception | None:
        """Retire/clean partial resources while preserving failed cleanup in Pool."""

        retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None
        physicals: list[PhysicalAcquisition[ResourceT]] = []
        with self._lock:
            current = self._attempts.get(context.attempt_id)
            if not self._state_uses_context(current, context):
                return None

            self._attempts[context.attempt_id] = _Cancelling(context)
            release = self._resource_pool.release(self._owner, context.attempt_id)
            physicals = [
                part.physical for part in context.parts if not part.finished
            ]
            if release.processing:
                current_resources = self._current_resources_or_none(context)
                retired = self._resource_pool.finish(
                    self._owner,
                    context.attempt_id,
                    current_resources,
                )
            else:
                retired = release.retired

            if retired is not None:
                self._attempts[context.attempt_id] = _Retired(context, retired)
            else:
                self._attempts.pop(context.attempt_id, None)

        for physical in physicals:
            try:
                physical.interrupt()
            except Exception as exc:
                if primary_error is not None:
                    primary_error.add_note(f"resource interrupt also failed: {exc!r}")

        if retired is not None:
            return self._cleanup_retired_attempt(retired)
        return None

    def _cancel_before_resources(
        self,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
    ) -> None:
        physicals: list[PhysicalAcquisition[ResourceT]] = []
        with self._lock:
            current = self._attempts.get(context.attempt_id)
            if not self._state_uses_context(current, context):
                return
            physicals = [
                part.physical for part in context.parts if not part.finished
            ]
            try:
                self._resource_pool.cancel(self._owner, context.attempt_id)
            except RuntimeError:
                release = self._resource_pool.release(self._owner, context.attempt_id)
                if release.processing:
                    self._resource_pool.finish(self._owner, context.attempt_id, None)
            self._attempts.pop(context.attempt_id, None)

        for physical in physicals:
            try:
                physical.interrupt()
            except Exception:
                pass

    def _cleanup_retired_attempt(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> Exception | None:
        state: _AttemptState[AccessT, SpecT, ResourceT] | None
        with self._lock:
            state = self._attempts.get(retired.attempt)
            if isinstance(state, _RetiredPinned):
                return None
            if isinstance(state, _Retired):
                if state.cleanup_in_progress:
                    return None
                state.cleanup_in_progress = True

        try:
            self._driver.cleanup(retired.resources)
            self._resource_pool.discard(retired)
        except Exception as exc:
            with self._lock:
                current = self._attempts.get(retired.attempt)
                if isinstance(current, _Retired):
                    current.cleanup_in_progress = False
            return exc

        with self._lock:
            current = self._attempts.get(retired.attempt)
            if isinstance(current, _Retired):
                self._attempts.pop(retired.attempt, None)
        return None

    def _is_cancelling(
        self,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
    ) -> bool:
        with self._lock:
            current = self._attempts.get(context.attempt_id)
            return not (
                isinstance(current, _Acquiring)
                and current.context is context
            )

    def _pool_has_attempt(self, attempt_id: AttemptId) -> bool:
        if self._resource_pool.request_snapshot(attempt_id) is not None:
            return True
        return any(
            record.attempt is attempt_id for record in self._resource_pool.snapshot()
        )

    @staticmethod
    def _state_uses_context(
        state: _AttemptState[AccessT, SpecT, ResourceT] | None,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
    ) -> bool:
        return isinstance(
            state,
            (_Acquiring, _Cancelling, _Pinned, _Retained, _RetiredPinned, _Retired),
        ) and state.context is context

    @staticmethod
    def _validate_attempt_id(attempt_id: AttemptId) -> None:
        if not isinstance(attempt_id, AttemptId):
            raise TypeError("attempt_id must be AttemptId")

    @staticmethod
    def _current_resources(
        context: _AttemptContext[AccessT, SpecT, ResourceT],
    ) -> ResourceSet[ResourceT]:
        return tuple(
            resource
            for part in context.parts
            if part.resources is not None
            for resource in part.resources
        )

    @classmethod
    def _current_resources_or_none(
        cls,
        context: _AttemptContext[AccessT, SpecT, ResourceT],
    ) -> ResourceSet[ResourceT] | None:
        if not context.parts:
            return ()
        if not any(part.resources is not None for part in context.parts):
            return None
        return cls._current_resources(context)


__all__ = [
    "ResourceAcquisitionCancelled",
    "ResourceManagement",
    "ResourceManager",
    "ResourceRequirementsModel",
]
