from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from threading import Lock
from typing import Generic, Hashable, Protocol, TypeVar

from _access import AccessIdentity
from _attempt import AttemptToken
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
AccessKeyT = TypeVar("AccessKeyT", bound=Hashable)
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")


class ResourceRequirementsModel(Protocol[AccessT, SpecT]):
    """Resolve only the physical Resource requirements for one Access."""

    def requirements(self, access: AccessT) -> ResourceRequirements[SpecT]: ...


class ResourceManagement(Protocol[AccessT, ResourceT]):
    """Managed-facing boundary for the complete Resource acquisition lifecycle.

    ``Acquired`` resources remain physically pinned until ``finish_acquire`` even
    if ``release`` is requested concurrently.  ``release`` is idempotent and may
    be called before this manager has registered the attempt; persistent
    cancellation is carried by ``AttemptToken`` itself.
    """

    def acquire(
        self,
        attempt: AttemptToken,
        access: AccessT,
    ) -> ResourceAcquireResult[ResourceT]: ...

    def release(self, attempt: AttemptToken) -> None: ...

    def finish_acquire(self, attempt: AttemptToken) -> None: ...

    def cleanup_retired(self, access: AccessT) -> bool: ...


class ResourceAcquisitionCancelled(RuntimeError):
    """Internal operational failure used when an AttemptToken is cancelled."""


@dataclass(slots=True)
class _AcquisitionPart(Generic[SpecT, ResourceT]):
    spec: SpecT
    physical: PhysicalAcquisition[ResourceT]
    resources: ResourceSet[ResourceT] | None = None
    finished: bool = False


@dataclass(slots=True)
class _AttemptState(Generic[AccessKeyT, SpecT, ResourceT]):
    attempt: AttemptToken
    access_key: AccessKeyT
    requirements: ResourceRequirements[SpecT]
    parts: list[_AcquisitionPart[SpecT, ResourceT]] = field(default_factory=list)
    resources: ResourceSet[ResourceT] | None = None
    release_requested: bool = False
    projection_active: bool = False
    installed: bool = False
    cleanup_in_progress: bool = False


class ResourceManager(Generic[AccessT, AccessKeyT, SpecT, ResourceT]):
    """Own Access-to-Resource semantics, physical I/O, retention, and cleanup."""

    def __init__(
        self,
        access_identity: AccessIdentity[AccessT, AccessKeyT],
        requirements_model: ResourceRequirementsModel[AccessT, SpecT],
        driver: ResourceDriver[SpecT, ResourceT],
        *,
        resource_pool: ResourcePool[AccessKeyT, SpecT, ResourceT] = GLOBAL_RESOURCE_POOL,
    ) -> None:
        self._access_identity = access_identity
        self._requirements_model = requirements_model
        self._driver = driver
        if not isinstance(resource_pool, ResourcePool):
            raise TypeError("resource_pool must be ResourcePool")
        self._resource_pool = resource_pool
        self._owner = object()
        self._lock = Lock()
        self._attempts: dict[
            AttemptToken, _AttemptState[AccessKeyT, SpecT, ResourceT]
        ] = {}

    @property
    def access_identity(self) -> AccessIdentity[AccessT, AccessKeyT]:
        return self._access_identity

    @property
    def requirements_model(self) -> ResourceRequirementsModel[AccessT, SpecT]:
        return self._requirements_model

    @property
    def driver(self) -> ResourceDriver[SpecT, ResourceT]:
        return self._driver

    @property
    def resource_pool(self) -> ResourcePool[AccessKeyT, SpecT, ResourceT]:
        return self._resource_pool

    def acquire(
        self,
        attempt: AttemptToken,
        access: AccessT,
    ) -> ResourceAcquireResult[ResourceT]:
        self._validate_attempt(attempt)
        if access is None:
            raise TypeError("access cannot be None")
        if not attempt.begin():
            raise RuntimeError("attempt can begin only one resource lifecycle")
        if attempt.cancelled:
            return ResourceFailed(ResourceAcquisitionCancelled("attempt was cancelled"))

        access_key = self._access_key(access)
        requirements = self._requirements_model.requirements(access)

        if attempt.cancelled:
            return ResourceFailed(ResourceAcquisitionCancelled("attempt was cancelled"))

        reserved = self._resource_pool.reserve(
            self._owner,
            attempt,
            access_key,
            requirements,
        )
        if not reserved:
            return ResourceBlocked()

        state = _AttemptState(attempt, access_key, requirements)
        with self._lock:
            if attempt in self._attempts:
                self._resource_pool.cancel(self._owner, attempt)
                raise RuntimeError("attempt is already registered in this manager")
            self._attempts[attempt] = state

        if attempt.cancelled:
            self._cancel_before_resources(state)
            return ResourceFailed(ResourceAcquisitionCancelled("attempt was cancelled"))

        try:
            prepare_failure = self._prepare_parts(state)
            if prepare_failure is not None:
                self._fail_attempt(state, prepare_failure)
                return ResourceFailed(prepare_failure)

            if self._is_release_requested(state):
                self._cancel_before_resources(state)
                return ResourceFailed(ResourceAcquisitionCancelled("attempt was cancelled"))

            acquire_failure = self._acquire_parts(state)
            if acquire_failure is not None:
                cleanup_failure = self._fail_attempt(state, acquire_failure)
                if cleanup_failure is not None:
                    acquire_failure.add_note(
                        f"resource cleanup also failed: {cleanup_failure!r}"
                    )
                return ResourceFailed(acquire_failure)

            resources = self._current_resources(state)
            state.resources = resources

            with self._lock:
                current = self._attempts.get(attempt)
                if current is not state:
                    raise RuntimeError("resource attempt registry changed unexpectedly")

                if attempt.cancelled or state.release_requested:
                    state.release_requested = True
                    self._resource_pool.release(self._owner, attempt)
                    retired = self._resource_pool.finish(
                        self._owner,
                        attempt,
                        resources,
                    )
                else:
                    # Pin before publishing the retained record.  release() uses the
                    # same manager lock, so cleanup cannot race ahead of projection.
                    state.projection_active = True
                    retired = self._resource_pool.finish(
                        self._owner,
                        attempt,
                        resources,
                    )
                    if retired is not None:
                        state.projection_active = False
                        state.release_requested = True
                    else:
                        self._resource_pool.install(self._owner, attempt)
                        state.installed = True
                        return ResourceAcquired(resources)

            if retired is not None:
                self._cleanup_retired_attempt(retired)
            else:
                self._forget_if_unretained(state)
            return ResourceFailed(ResourceAcquisitionCancelled("attempt was cancelled"))

        except BaseException:
            # Contract/invariant errors are not normalized into ResourceFailed, but
            # any partial physical resources still remain ResourceManager-owned.
            self._fail_attempt(state, None)
            raise

    def release(self, attempt: AttemptToken) -> None:
        self._validate_attempt(attempt)
        attempt.cancel()

        retired: RetiredResource[AccessKeyT, SpecT, ResourceT] | None = None
        physicals: list[PhysicalAcquisition[ResourceT]] = []
        with self._lock:
            state = self._attempts.get(attempt)
            if state is None:
                return
            state.release_requested = True
            release = self._resource_pool.release(self._owner, attempt)
            retired = release.retired
            physicals = [
                part.physical for part in state.parts if not part.finished
            ]
            cleanup_now = retired is not None and not state.projection_active

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

    def finish_acquire(self, attempt: AttemptToken) -> None:
        """End the temporary ResourceSet borrow used for capability projection."""

        self._validate_attempt(attempt)
        retired: RetiredResource[AccessKeyT, SpecT, ResourceT] | None = None
        with self._lock:
            state = self._attempts.get(attempt)
            if state is None:
                return
            state.projection_active = False
            if state.release_requested:
                release = self._resource_pool.release(self._owner, attempt)
                retired = release.retired

        if retired is not None:
            # A finally-path must not replace the Managed acquire result.  Failed
            # cleanup stays retained in the Pool for cleanup_retired().
            self._cleanup_retired_attempt(retired)

    def cleanup_retired(self, access: AccessT) -> bool:
        if access is None:
            raise TypeError("access cannot be None")
        access_key = self._access_key(access)
        requirements = self._requirements_model.requirements(access)
        retired_records = self._resource_pool.retired(
            self._owner,
            access_key,
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
        state: _AttemptState[AccessKeyT, SpecT, ResourceT],
    ) -> Exception | None:
        for requirement in state.requirements:
            if self._is_release_requested(state):
                return ResourceAcquisitionCancelled("attempt was cancelled")
            try:
                physical = self._driver.prepare(requirement.spec)
            except Exception as exc:
                return exc
            if physical is None:
                raise TypeError("ResourceDriver.prepare() cannot return None")
            with self._lock:
                state.parts.append(_AcquisitionPart(requirement.spec, physical))
            if self._is_release_requested(state):
                try:
                    physical.interrupt()
                except Exception:
                    pass
                return ResourceAcquisitionCancelled("attempt was cancelled")
        return None

    def _acquire_parts(
        self,
        state: _AttemptState[AccessKeyT, SpecT, ResourceT],
    ) -> Exception | None:
        if not state.parts:
            resources: ResourceSet[ResourceT] = ()
            self._resource_pool.publish(self._owner, state.attempt, resources)
            return None

        for part in state.parts:
            if self._is_release_requested(state):
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
                        state.attempt,
                        self._current_resources(state),
                    )
                    if self._is_release_requested(state):
                        return ResourceAcquisitionCancelled("attempt was cancelled")
            finally:
                part.finished = True

            if not yielded:
                if self._is_release_requested(state):
                    return ResourceAcquisitionCancelled("attempt was cancelled")
                raise RuntimeError(
                    "PhysicalAcquisition.acquire() must yield at least one ResourceSet"
                )
        return None

    def _fail_attempt(
        self,
        state: _AttemptState[AccessKeyT, SpecT, ResourceT],
        primary_error: Exception | None,
    ) -> Exception | None:
        """Retire/clean partial resources while preserving failed cleanup in Pool."""

        retired: RetiredResource[AccessKeyT, SpecT, ResourceT] | None = None
        physicals: list[PhysicalAcquisition[ResourceT]] = []
        with self._lock:
            current = self._attempts.get(state.attempt)
            if current is not state:
                return None
            state.release_requested = True
            release = self._resource_pool.release(self._owner, state.attempt)
            physicals = [
                part.physical for part in state.parts if not part.finished
            ]
            if release.processing:
                current_resources = self._current_resources_or_none(state)
                retired = self._resource_pool.finish(
                    self._owner,
                    state.attempt,
                    current_resources,
                )
            else:
                retired = release.retired

        for physical in physicals:
            try:
                physical.interrupt()
            except Exception as exc:
                if primary_error is not None:
                    primary_error.add_note(f"resource interrupt also failed: {exc!r}")

        if retired is not None:
            return self._cleanup_retired_attempt(retired)

        self._forget_if_unretained(state)
        return None

    def _cancel_before_resources(
        self,
        state: _AttemptState[AccessKeyT, SpecT, ResourceT],
    ) -> None:
        physicals: list[PhysicalAcquisition[ResourceT]] = []
        with self._lock:
            current = self._attempts.get(state.attempt)
            if current is not state:
                return
            state.release_requested = True
            state.attempt.cancel()
            physicals = [
                part.physical for part in state.parts if not part.finished
            ]
            try:
                self._resource_pool.cancel(self._owner, state.attempt)
            except RuntimeError:
                release = self._resource_pool.release(self._owner, state.attempt)
                if release.processing:
                    self._resource_pool.finish(self._owner, state.attempt, None)
            self._attempts.pop(state.attempt, None)

        for physical in physicals:
            try:
                physical.interrupt()
            except Exception:
                pass

    def _cleanup_retired_attempt(
        self,
        retired: RetiredResource[AccessKeyT, SpecT, ResourceT],
    ) -> Exception | None:
        state: _AttemptState[AccessKeyT, SpecT, ResourceT] | None
        with self._lock:
            state = self._attempts.get(retired.attempt)
            if state is not None:
                if state.projection_active:
                    return None
                if state.cleanup_in_progress:
                    return None
                state.cleanup_in_progress = True

        try:
            self._driver.cleanup(retired.resources)
            self._resource_pool.discard(retired)
        except Exception as exc:
            with self._lock:
                current = self._attempts.get(retired.attempt)
                if current is not None:
                    current.cleanup_in_progress = False
            return exc

        with self._lock:
            self._attempts.pop(retired.attempt, None)
        return None

    def _forget_if_unretained(
        self,
        state: _AttemptState[AccessKeyT, SpecT, ResourceT],
    ) -> None:
        if self._resource_pool.request_snapshot(state.attempt) is not None:
            return
        if any(record.attempt is state.attempt for record in self._resource_pool.snapshot()):
            return
        with self._lock:
            if self._attempts.get(state.attempt) is state:
                self._attempts.pop(state.attempt, None)

    def _is_release_requested(
        self,
        state: _AttemptState[AccessKeyT, SpecT, ResourceT],
    ) -> bool:
        if state.attempt.cancelled:
            return True
        with self._lock:
            current = self._attempts.get(state.attempt)
            return current is not state or state.release_requested

    def _access_key(self, access: AccessT) -> AccessKeyT:
        access_key = self._access_identity.key(access)
        if access_key is None:
            raise TypeError("AccessIdentity.key() cannot return None")
        try:
            hash(access_key)
        except TypeError as exc:
            raise TypeError("AccessIdentity.key() must return a hashable value") from exc
        return access_key

    @staticmethod
    def _validate_attempt(attempt: AttemptToken) -> None:
        if not isinstance(attempt, AttemptToken):
            raise TypeError("attempt must be AttemptToken")

    @staticmethod
    def _current_resources(
        state: _AttemptState[AccessKeyT, SpecT, ResourceT],
    ) -> ResourceSet[ResourceT]:
        return tuple(
            resource
            for part in state.parts
            if part.resources is not None
            for resource in part.resources
        )

    @classmethod
    def _current_resources_or_none(
        cls,
        state: _AttemptState[AccessKeyT, SpecT, ResourceT],
    ) -> ResourceSet[ResourceT] | None:
        if not state.parts:
            return ()
        if not any(part.resources is not None for part in state.parts):
            return None
        return cls._current_resources(state)


__all__ = [
    "ResourceAcquisitionCancelled",
    "ResourceManagement",
    "ResourceManager",
    "ResourceRequirementsModel",
]
