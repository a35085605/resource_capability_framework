from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any, Generic, TypeVar

from _attempt import AttemptId
from _resource.driver import ResourceSet
from _resource.policy import ResourcePolicy
from _resource.requirement import ResourceRequirement, ResourceRequirements


AccessT = TypeVar("AccessT")
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")


@dataclass(frozen=True, slots=True)
class ResourceRequestRecord(Generic[AccessT, SpecT, ResourceT]):
    """Immutable point-in-time view of one attempt still being acquired."""

    attempt: AttemptId
    access: AccessT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT] | None
    retired: bool
    processing: bool


@dataclass(frozen=True, slots=True)
class ResourceRecord(Generic[AccessT, SpecT, ResourceT]):
    """Immutable point-in-time view of one retained or retired attempt."""

    attempt: AttemptId
    access: AccessT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT]
    retired: bool = False


@dataclass(frozen=True, slots=True, eq=False)
class RetiredResource(Generic[AccessT, SpecT, ResourceT]):
    """Exact retired attempt retained until physical cleanup succeeds."""

    attempt: AttemptId
    access: AccessT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT]


@dataclass(frozen=True, slots=True)
class AttemptRelease(Generic[AccessT, SpecT, ResourceT]):
    """Result of asking one attempt to stop or release its retained resources."""

    processing: bool
    retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None


@dataclass(slots=True)
class _Entry(Generic[AccessT, SpecT, ResourceT]):
    attempt: AttemptId
    access: AccessT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT] | None = None
    processing: bool = True
    retired: bool = False


class ResourcePool(Generic[AccessT, SpecT, ResourceT]):
    """Process-wide registry keyed by the caller-supplied acquisition attempt.

    Conflict checking and reservation are one atomic operation.  One entry tracks
    each attempt across processing, retained, and retired phases; ``processing``
    describes whether physical production may still change its ResourceSet, while
    ``retired`` records that retention authority has been revoked.  Access is kept
    only to associate the entry with its creator; conflicts are determined
    exclusively by Resource requirements.  ``AttemptId`` is the sole identity for
    the complete lifecycle.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._entries: dict[AttemptId, _Entry[AccessT, SpecT, ResourceT]] = {}

    @staticmethod
    def _validate_attempt(attempt: AttemptId) -> None:
        if not isinstance(attempt, AttemptId):
            raise TypeError("attempt must be AttemptId")

    @staticmethod
    def _validate_access(access: AccessT) -> None:
        if access is None:
            raise TypeError("access cannot be None")

    @staticmethod
    def _validate_requirements(
        requirements: ResourceRequirements[SpecT],
    ) -> ResourceRequirements[SpecT]:
        if not isinstance(requirements, tuple):
            raise TypeError("requirements must be a tuple")

        seen_specs: list[SpecT] = []
        for requirement in requirements:
            if not isinstance(requirement, ResourceRequirement):
                raise TypeError("requirements must contain ResourceRequirement values")
            if any(existing == requirement.spec for existing in seen_specs):
                raise ValueError("requirements cannot contain duplicate resource specs")
            seen_specs.append(requirement.spec)
        return requirements

    def snapshot(self) -> tuple[ResourceRecord[AccessT, SpecT, ResourceT], ...]:
        with self._lock:
            return tuple(
                ResourceRecord(
                    state.attempt,
                    state.access,
                    state.requirements,
                    state.resources,
                    state.retired,
                )
                for state in self._entries.values()
                if not state.processing and state.resources is not None
            )

    def request_snapshot(
        self,
        attempt: AttemptId,
    ) -> ResourceRequestRecord[AccessT, SpecT, ResourceT] | None:
        self._validate_attempt(attempt)
        with self._lock:
            state = self._entries.get(attempt)
            if state is None or not state.processing:
                return None
            return self._request_record_locked(state)

    def requests(
        self,
    ) -> tuple[ResourceRequestRecord[AccessT, SpecT, ResourceT], ...]:
        with self._lock:
            return tuple(
                self._request_record_locked(state)
                for state in self._entries.values()
                if state.processing
            )

    def lookup(
        self,
        access: AccessT,
        spec: SpecT,
    ) -> tuple[ResourceSet[ResourceT], ...]:
        self._validate_access(access)
        if spec is None:
            raise TypeError("spec cannot be None")
        with self._lock:
            return tuple(
                state.resources
                for state in self._entries.values()
                if not state.processing
                and state.resources is not None
                and state.access == access
                and not state.retired
                and self._contains_spec(state.requirements, spec)
            )

    def retired(
        self,
        attempt: AttemptId,
    ) -> RetiredResource[AccessT, SpecT, ResourceT] | None:
        """Return the exact retired resource identified by ``attempt``."""

        self._validate_attempt(attempt)
        with self._lock:
            state = self._entries.get(attempt)
            if (
                state is None
                or state.processing
                or not state.retired
                or state.resources is None
            ):
                return None
            return self._retired_view(state)

    def reserve(
        self,
        attempt: AttemptId,
        access: AccessT,
        requirements: ResourceRequirements[SpecT],
    ) -> bool:
        """Atomically conflict-check requirements and reserve them for ``attempt``."""

        self._validate_attempt(attempt)
        self._validate_access(access)
        normalized = self._validate_requirements(requirements)

        with self._lock:
            if attempt in self._entries:
                raise RuntimeError("attempt is already registered in this pool")

            for state in self._entries.values():
                if self._conflicts(state.requirements, normalized):
                    return False

            self._entries[attempt] = _Entry(
                attempt=attempt,
                access=access,
                requirements=normalized,
            )
            return True

    def publish(
        self,
        attempt: AttemptId,
        resources: ResourceSet[ResourceT],
    ) -> None:
        self._validate_resources(resources)
        with self._lock:
            state = self._require_entry_locked(attempt)
            if not state.processing:
                raise RuntimeError("resource attempt is no longer processing")
            state.resources = resources

    def release(
        self,
        attempt: AttemptId,
    ) -> AttemptRelease[AccessT, SpecT, ResourceT]:
        """Idempotently request stop/release for any current phase of ``attempt``."""

        self._validate_attempt(attempt)
        with self._lock:
            state = self._entries.get(attempt)
            if state is None:
                return AttemptRelease(processing=False)

            state.retired = True
            if state.processing:
                return AttemptRelease(processing=True)
            if state.resources is None:
                del self._entries[attempt]
                return AttemptRelease(processing=False)
            return AttemptRelease(processing=False, retired=self._retired_view(state))

    def finish(
        self,
        attempt: AttemptId,
        resources: ResourceSet[ResourceT] | None = None,
    ) -> RetiredResource[AccessT, SpecT, ResourceT] | None:
        """Publish the final snapshot and end physical processing for ``attempt``."""

        if resources is not None:
            self._validate_resources(resources)
        with self._lock:
            state = self._require_entry_locked(attempt)
            if not state.processing:
                raise RuntimeError("resource attempt is already finished")
            if resources is not None:
                state.resources = resources
            if state.resources is None:
                if state.retired:
                    del self._entries[attempt]
                    return None
                raise RuntimeError("resource attempt has no final resources")
            state.processing = False
            if not state.retired:
                return None
            return self._retired_view(state)

    def cancel(self, attempt: AttemptId) -> bool:
        """Cancel a reservation that has not published any Resource yet."""

        self._validate_attempt(attempt)
        with self._lock:
            state = self._entries.get(attempt)
            if state is None or not state.processing:
                return False
            if state.resources is not None:
                raise RuntimeError("cannot cancel an attempt that already has resources")
            del self._entries[attempt]
            return True

    def discard(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> None:
        if not isinstance(retired, RetiredResource):
            raise TypeError("retired must be RetiredResource")

        with self._lock:
            state = self._entries.get(retired.attempt)
            if state is None:
                raise RuntimeError("retired resource is not retained")
            if state.resources is not retired.resources:
                raise RuntimeError("retired resource does not match retained resources")
            if state.processing:
                raise RuntimeError("processing resource attempt cannot be discarded")
            if not state.retired:
                raise RuntimeError("resource set must be retired before discard")
            del self._entries[retired.attempt]

    @staticmethod
    def _validate_resources(resources: ResourceSet[ResourceT]) -> None:
        if resources is None:
            raise TypeError("resources cannot be None")
        if not isinstance(resources, tuple):
            raise TypeError("resources must be a ResourceSet tuple")

    def _require_entry_locked(
        self,
        attempt: AttemptId,
    ) -> _Entry[AccessT, SpecT, ResourceT]:
        self._validate_attempt(attempt)
        state = self._entries.get(attempt)
        if state is None:
            raise RuntimeError("resource attempt is not current")
        return state

    @staticmethod
    def _request_record_locked(
        state: _Entry[AccessT, SpecT, ResourceT],
    ) -> ResourceRequestRecord[AccessT, SpecT, ResourceT]:
        return ResourceRequestRecord(
            state.attempt,
            state.access,
            state.requirements,
            state.resources,
            state.retired,
            state.processing,
        )

    @staticmethod
    def _retired_view(
        state: _Entry[AccessT, SpecT, ResourceT],
    ) -> RetiredResource[AccessT, SpecT, ResourceT]:
        if state.resources is None:
            raise RuntimeError("retired resource has no ResourceSet")
        return RetiredResource(
            state.attempt,
            state.access,
            state.requirements,
            state.resources,
        )

    @staticmethod
    def _contains_spec(
        requirements: ResourceRequirements[SpecT],
        spec: SpecT,
    ) -> bool:
        return any(requirement.spec == spec for requirement in requirements)

    @staticmethod
    def _conflicts(
        existing: ResourceRequirements[SpecT],
        incoming: ResourceRequirements[SpecT],
    ) -> bool:
        for incoming_requirement in incoming:
            for existing_requirement in existing:
                if existing_requirement.spec != incoming_requirement.spec:
                    continue
                if existing_requirement.policy is ResourcePolicy.BLOCKING:
                    return True
        return False


GLOBAL_RESOURCE_POOL: ResourcePool[Any, Any, Any] = ResourcePool()


__all__ = [
    "AttemptRelease",
    "GLOBAL_RESOURCE_POOL",
    "ResourcePool",
    "ResourceRecord",
    "ResourceRequestRecord",
    "RetiredResource",
]
