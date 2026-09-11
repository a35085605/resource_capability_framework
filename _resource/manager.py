from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from threading import Lock, Thread
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
    concurrent ``release`` cannot be lost.  ``release`` only revokes retention
    authority; interruption and cleanup remain Resource-layer concerns and cleanup
    never has to complete before ``release`` returns.  Acquired resources remain
    physically pinned until ``finish_acquire`` even if ``release`` is requested
    concurrently.
    """

    def open_attempt(self) -> AttemptId: ...

    def acquire(
        self,
        attempt_id: AttemptId,
        access: AccessT,
    ) -> ResourceAcquireResult[ResourceT]: ...

    def release(self, attempt_id: AttemptId) -> None: ...

    def finish_acquire(self, attempt_id: AttemptId) -> None: ...


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
                self._fail_attempt(context, acquire_failure)
                return ResourceFailed(acquire_failure)

            resources = self._current_resources(context)
            context.resources = resources

            retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None
            with self._lock:
                current = self._attempts.get(attempt_id)
                if isinstance(current, _Cancelling) and current.context is context:
                    self._resource_pool.release(attempt_id)
                    retired = self._resource_pool.finish(
                        attempt_id,
                        resources,
                    )
                    if retired is not None:
                        self._attempts[attempt_id] = _Retired(context, retired)
                    else:
                        self._attempts.pop(attempt_id, None)
                elif current is acquiring:
                    retired = self._resource_pool.finish(
                        attempt_id,
                        resources,
                    )
                    if retired is not None:
                        self._attempts[attempt_id] = _Retired(context, retired)
                    else:
                        self._attempts[attempt_id] = _Pinned(context)
                        return ResourceAcquired(resources)
                else:
                    raise RuntimeError("resource attempt registry changed unexpectedly")

            if retired is not None:
                self._schedule_cleanup_retired_attempt(retired)
            return ResourceFailed(
                ResourceAcquisitionCancelled("attempt was cancelled")
            )

        except BaseException:
            # Contract/invariant errors are not normalized into ResourceFailed, but
            # any partial physical resources still remain ResourceManager-owned.
            self._fail_attempt(context, None)
            raise

    def release(self, attempt_id: AttemptId) -> None:
        """Revoke retention authority without waiting for physical cleanup.

        A processing attempt is marked retired and best-effort interrupted.  Its
        cleanup is scheduled automatically once processing has finished.  A retained
        attempt is cleanup-eligible immediately, but cleanup still runs detached from
        this call so Managed never waits for driver cleanup.
        """

        self._validate_attempt_id(attempt_id)

        retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None
        physicals: list[PhysicalAcquisition[ResourceT]] = []

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
                release = self._resource_pool.release(attempt_id)
                retired = release.retired
                physicals = [
                    part.physical for part in context.parts if not part.finished
                ]
                if retired is not None:
                    self._attempts[attempt_id] = _Retired(context, retired)
            elif isinstance(state, _Cancelling):
                context = state.context
                release = self._resource_pool.release(attempt_id)
                retired = release.retired
                physicals = [
                    part.physical for part in context.parts if not part.finished
                ]
                if retired is not None:
                    self._attempts[attempt_id] = _Retired(context, retired)
            elif isinstance(state, _Pinned):
                release = self._resource_pool.release(attempt_id)
                retired = release.retired
                if retired is None:
                    raise RuntimeError("pinned resource attempt was not retained")
                self._attempts[attempt_id] = _RetiredPinned(state.context, retired)
            elif isinstance(state, _Retained):
                release = self._resource_pool.release(attempt_id)
                retired = release.retired
                if retired is None:
                    raise RuntimeError("retained resource attempt was not retained")
                self._attempts[attempt_id] = _Retired(state.context, retired)
            elif isinstance(state, _RetiredPinned):
                return
            elif isinstance(state, _Retired):
                retired = state.retired
            else:
                raise RuntimeError("unsupported Resource attempt state")

        for physical in physicals:
            try:
                physical.interrupt()
            except Exception:
                # Retiring Managed authority must not depend on whether a backend can
                # synchronously interrupt an in-flight physical operation.  The
                # producer may still finish naturally and become cleanup-eligible.
                pass

        if retired is not None:
            self._schedule_cleanup_retired_attempt(retired)

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
            self._schedule_cleanup_retired_attempt(retired)

    def cleanup_retired(self, access: AccessT) -> bool:
        """Synchronously retry cleanup of retired entries for operational recovery.

        Normal Managed flows never need to call this; retirement schedules cleanup
        automatically.  This method remains available for retrying a cleanup that a
        driver previously failed.
        """

        if access is None:
            raise TypeError("access cannot be None")

        with self._lock:
            attempt_ids = tuple(
                state.retired.attempt
                for state in self._attempts.values()
                if isinstance(state, (_RetiredPinned, _Retired))
                and state.retired.access == access
            )

        retired_records = tuple(
            retired
            for attempt_id in attempt_ids
            if (retired := self._resource_pool.retired(attempt_id)) is not None
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
            self._resource_pool.publish(context.attempt_id, resources)
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
    ) -> None:
        """Retire partial resources and schedule detached cleanup."""

        retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None
        physicals: list[PhysicalAcquisition[ResourceT]] = []
        with self._lock:
            current = self._attempts.get(context.attempt_id)
            if not self._state_uses_context(current, context):
                return

            self._attempts[context.attempt_id] = _Cancelling(context)
            release = self._resource_pool.release(context.attempt_id)
            physicals = [
                part.physical for part in context.parts if not part.finished
            ]
            if release.processing:
                current_resources = self._current_resources_or_none(context)
                retired = self._resource_pool.finish(
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
            self._schedule_cleanup_retired_attempt(retired)

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
                self._resource_pool.cancel(context.attempt_id)
            except RuntimeError:
                release = self._resource_pool.release(context.attempt_id)
                if release.processing:
                    self._resource_pool.finish(context.attempt_id, None)
            self._attempts.pop(context.attempt_id, None)

        for physical in physicals:
            try:
                physical.interrupt()
            except Exception:
                pass

    def _schedule_cleanup_retired_attempt(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> None:
        """Run cleanup in a detached daemon thread if the attempt is eligible."""

        if not self._claim_cleanup(retired):
            return

        worker = Thread(
            target=self._cleanup_claimed_retired_attempt,
            args=(retired,),
            name=f"resource-cleanup-{id(retired.attempt):x}",
            daemon=True,
        )
        try:
            worker.start()
        except Exception:
            # Thread creation failure must not make Managed release fail.  Keep the
            # retired entry available for a later release/cleanup_retired retry.
            self._reset_cleanup_claim(retired)

    def _cleanup_retired_attempt(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> Exception | None:
        """Synchronously cleanup one eligible retired entry."""

        if not self._claim_cleanup(retired):
            return None
        return self._cleanup_claimed_retired_attempt(retired)

    def _claim_cleanup(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> bool:
        with self._lock:
            state = self._attempts.get(retired.attempt)
            if isinstance(state, _RetiredPinned):
                return False
            if not isinstance(state, _Retired):
                return False
            if state.cleanup_in_progress:
                return False
            state.cleanup_in_progress = True
            return True

    def _reset_cleanup_claim(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> None:
        with self._lock:
            current = self._attempts.get(retired.attempt)
            if isinstance(current, _Retired):
                current.cleanup_in_progress = False

    def _cleanup_claimed_retired_attempt(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> Exception | None:
        """Cleanup an entry whose ``cleanup_in_progress`` claim is already held."""

        try:
            self._driver.cleanup(retired.resources)
            self._resource_pool.discard(retired)
        except Exception as exc:
            self._reset_cleanup_claim(retired)
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
