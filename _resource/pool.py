from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any, Generic, TypeVar

from _attempt import AttemptToken
from _resource.driver import ResourceSet
from _resource.policy import ResourcePolicy
from _resource.requirement import ResourceRequirement, ResourceRequirements


AccessT = TypeVar("AccessT")
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")


@dataclass(frozen=True, slots=True)
class ResourceRequestRecord(Generic[AccessT, SpecT, ResourceT]):
    """Immutable point-in-time view of one attempt still being acquired."""

    attempt: AttemptToken
    access: AccessT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT] | None
    interrupted: bool
    processing: bool


@dataclass(frozen=True, slots=True)
class ResourceRecord(Generic[AccessT, SpecT, ResourceT]):
    """Immutable point-in-time view of one retained or retired attempt."""

    attempt: AttemptToken
    access: AccessT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT]
    retired: bool = False


@dataclass(frozen=True, slots=True, eq=False)
class RetiredResource(Generic[AccessT, SpecT, ResourceT]):
    """Exact retired attempt retained until physical cleanup succeeds."""

    attempt: AttemptToken
    access: AccessT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT]
    _owner: object


@dataclass(frozen=True, slots=True)
class AttemptRelease(Generic[AccessT, SpecT, ResourceT]):
    """Result of asking one attempt to stop or release its retained resources."""

    processing: bool
    retired: RetiredResource[AccessT, SpecT, ResourceT] | None = None


@dataclass(slots=True)
class _RequestState(Generic[AccessT, SpecT, ResourceT]):
    owner: object
    attempt: AttemptToken
    access: AccessT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT] | None = None
    interrupted: bool = False
    processing: bool = True


@dataclass(slots=True)
class _RecordState(Generic[AccessT, SpecT, ResourceT]):
    owner: object
    attempt: AttemptToken
    access: AccessT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT]
    retired: bool = False


class ResourcePool(Generic[AccessT, SpecT, ResourceT]):
    """Process-wide registry keyed by the caller-supplied acquisition attempt.

    Conflict checking and reservation are one atomic operation.  Access is retained
    only to associate records with the request that created them; conflicts are
    determined exclusively by Resource requirements.  ``AttemptToken`` identifies
    the same acquisition from reservation through retained/retired resource state;
    manager ownership remains a separate invariant.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._records: dict[
            AttemptToken, _RecordState[AccessT, SpecT, ResourceT]
        ] = {}
        self._requests: dict[
            AttemptToken, _RequestState[AccessT, SpecT, ResourceT]
        ] = {}

    @staticmethod
    def _validate_owner(owner: object) -> None:
        if owner is None:
            raise TypeError("owner cannot be None")

    @staticmethod
    def _validate_attempt(attempt: AttemptToken) -> None:
        if not isinstance(attempt, AttemptToken):
            raise TypeError("attempt must be AttemptToken")

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
                for state in self._records.values()
            )

    def request_snapshot(
        self,
        attempt: AttemptToken,
    ) -> ResourceRequestRecord[AccessT, SpecT, ResourceT] | None:
        self._validate_attempt(attempt)
        with self._lock:
            state = self._requests.get(attempt)
            if state is None:
                return None
            return self._request_record_locked(state)

    def requests(
        self,
    ) -> tuple[ResourceRequestRecord[AccessT, SpecT, ResourceT], ...]:
        with self._lock:
            return tuple(
                self._request_record_locked(state) for state in self._requests.values()
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
                for state in self._records.values()
                if state.access == access
                and not state.retired
                and self._contains_spec(state.requirements, spec)
            )

    def retired(
        self,
        owner: object,
        access: AccessT,
        requirements: ResourceRequirements[SpecT],
    ) -> tuple[RetiredResource[AccessT, SpecT, ResourceT], ...]:
        self._validate_owner(owner)
        self._validate_access(access)
        normalized = self._validate_requirements(requirements)

        with self._lock:
            return tuple(
                self._retired_view(state)
                for state in self._records.values()
                if state.owner is owner
                and state.access == access
                and state.retired
                and self._same_spec_set(state.requirements, normalized)
            )

    def reserve(
        self,
        owner: object,
        attempt: AttemptToken,
        access: AccessT,
        requirements: ResourceRequirements[SpecT],
    ) -> bool:
        """Atomically conflict-check requirements and reserve them for ``attempt``."""

        self._validate_owner(owner)
        self._validate_attempt(attempt)
        self._validate_access(access)
        normalized = self._validate_requirements(requirements)

        with self._lock:
            if attempt in self._requests or attempt in self._records:
                raise RuntimeError("attempt is already registered in this pool")

            for state in self._records.values():
                if self._conflicts(state.requirements, normalized):
                    return False
            for state in self._requests.values():
                if self._conflicts(state.requirements, normalized):
                    return False

            self._requests[attempt] = _RequestState(
                owner=owner,
                attempt=attempt,
                access=access,
                requirements=normalized,
            )
            return True

    def publish(
        self,
        owner: object,
        attempt: AttemptToken,
        resources: ResourceSet[ResourceT],
    ) -> None:
        self._validate_resources(resources)
        with self._lock:
            state = self._require_request_locked(owner, attempt)
            if not state.processing:
                raise RuntimeError("resource attempt is no longer processing")
            state.resources = resources

    def release(
        self,
        owner: object,
        attempt: AttemptToken,
    ) -> AttemptRelease[AccessT, SpecT, ResourceT]:
        """Idempotently request stop/release for any current phase of ``attempt``."""

        self._validate_owner(owner)
        self._validate_attempt(attempt)
        with self._lock:
            request = self._requests.get(attempt)
            if request is not None:
                self._require_owner(request.owner, owner)
                request.interrupted = True
                if request.processing:
                    return AttemptRelease(processing=True)
                retired = self._retire_request_locked(request)
                return AttemptRelease(processing=False, retired=retired)

            record = self._records.get(attempt)
            if record is None:
                return AttemptRelease(processing=False)
            self._require_owner(record.owner, owner)
            record.retired = True
            return AttemptRelease(processing=False, retired=self._retired_view(record))

    def finish(
        self,
        owner: object,
        attempt: AttemptToken,
        resources: ResourceSet[ResourceT] | None = None,
    ) -> RetiredResource[AccessT, SpecT, ResourceT] | None:
        """Publish the final snapshot and end physical processing for ``attempt``."""

        if resources is not None:
            self._validate_resources(resources)
        with self._lock:
            state = self._require_request_locked(owner, attempt)
            if not state.processing:
                raise RuntimeError("resource attempt is already finished")
            if resources is not None:
                state.resources = resources
            state.processing = False
            if state.interrupted:
                return self._retire_request_locked(state)
            return None

    def cancel(self, owner: object, attempt: AttemptToken) -> bool:
        """Cancel a reservation that has not published any Resource yet."""

        self._validate_owner(owner)
        self._validate_attempt(attempt)
        with self._lock:
            state = self._requests.get(attempt)
            if state is None:
                return False
            self._require_owner(state.owner, owner)
            if state.resources is not None:
                raise RuntimeError("cannot cancel an attempt that already has resources")
            del self._requests[attempt]
            return True

    def install(self, owner: object, attempt: AttemptToken) -> None:
        """Move one finished attempt into retained Resource state."""

        with self._lock:
            state = self._require_request_locked(owner, attempt)
            if state.processing:
                raise RuntimeError("resource attempt is still processing")
            if state.interrupted:
                raise RuntimeError("interrupted resource attempt cannot be installed")
            if state.resources is None:
                raise RuntimeError("resource attempt has no final resources")

            self._records[attempt] = _RecordState(
                owner=state.owner,
                attempt=state.attempt,
                access=state.access,
                requirements=state.requirements,
                resources=state.resources,
            )
            del self._requests[attempt]

    def discard(
        self,
        retired: RetiredResource[AccessT, SpecT, ResourceT],
    ) -> None:
        if not isinstance(retired, RetiredResource):
            raise TypeError("retired must be RetiredResource")

        with self._lock:
            state = self._records.get(retired.attempt)
            if state is None:
                raise RuntimeError("retired resource is not retained")
            self._require_owner(state.owner, retired._owner)
            if state.resources is not retired.resources:
                raise RuntimeError("retired resource does not match retained resources")
            if not state.retired:
                raise RuntimeError("resource set must be retired before discard")
            del self._records[retired.attempt]

    @staticmethod
    def _validate_resources(resources: ResourceSet[ResourceT]) -> None:
        if resources is None:
            raise TypeError("resources cannot be None")
        if not isinstance(resources, tuple):
            raise TypeError("resources must be a ResourceSet tuple")

    @staticmethod
    def _require_owner(actual: object, expected: object) -> None:
        if actual is not expected:
            raise RuntimeError("resource attempt belongs to another manager")

    def _require_request_locked(
        self,
        owner: object,
        attempt: AttemptToken,
    ) -> _RequestState[AccessT, SpecT, ResourceT]:
        self._validate_owner(owner)
        self._validate_attempt(attempt)
        state = self._requests.get(attempt)
        if state is None:
            raise RuntimeError("resource attempt is not current")
        self._require_owner(state.owner, owner)
        return state

    @staticmethod
    def _request_record_locked(
        state: _RequestState[AccessT, SpecT, ResourceT],
    ) -> ResourceRequestRecord[AccessT, SpecT, ResourceT]:
        return ResourceRequestRecord(
            state.attempt,
            state.access,
            state.requirements,
            state.resources,
            state.interrupted,
            state.processing,
        )

    def _retire_request_locked(
        self,
        state: _RequestState[AccessT, SpecT, ResourceT],
    ) -> RetiredResource[AccessT, SpecT, ResourceT] | None:
        del self._requests[state.attempt]
        if state.resources is None:
            return None

        record = _RecordState(
            owner=state.owner,
            attempt=state.attempt,
            access=state.access,
            requirements=state.requirements,
            resources=state.resources,
            retired=True,
        )
        self._records[state.attempt] = record
        return self._retired_view(record)

    @staticmethod
    def _retired_view(
        state: _RecordState[AccessT, SpecT, ResourceT],
    ) -> RetiredResource[AccessT, SpecT, ResourceT]:
        return RetiredResource(
            state.attempt,
            state.access,
            state.requirements,
            state.resources,
            state.owner,
        )

    @staticmethod
    def _contains_spec(
        requirements: ResourceRequirements[SpecT],
        spec: SpecT,
    ) -> bool:
        return any(requirement.spec == spec for requirement in requirements)

    @staticmethod
    def _same_spec_set(
        left: ResourceRequirements[SpecT],
        right: ResourceRequirements[SpecT],
    ) -> bool:
        return len(left) == len(right) and all(
            any(candidate.spec == requirement.spec for candidate in right)
            for requirement in left
        )

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
