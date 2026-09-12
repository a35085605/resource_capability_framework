from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any, Generic, TypeVar

from _attempt import AttemptId
from _resource.driver import ResourceSet
from _resource.policy import ResourcePolicy
from _resource.requirement import ResourceRequirement, ResourceRequirements


RequestT = TypeVar("RequestT")
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")


@dataclass(frozen=True, slots=True)
class ResourceRequestRecord(Generic[RequestT, SpecT]):
    """Immutable view of one reserved attempt still being physically acquired.

    Processing attempts intentionally expose no ResourceSet: physical acquisitions
    publish only one terminal ResourceSet when the complete attempt finishes.
    """

    attempt: AttemptId
    request: RequestT
    requirements: ResourceRequirements[SpecT]
    retired: bool


@dataclass(frozen=True, slots=True)
class ResourceRecord(Generic[RequestT, SpecT, ResourceT]):
    """Immutable point-in-time view of one retained or retired attempt."""

    attempt: AttemptId
    request: RequestT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT]
    retired: bool = False


@dataclass(frozen=True, slots=True, eq=False)
class RetiredResource(Generic[RequestT, SpecT, ResourceT]):
    """Exact retired attempt retained until physical cleanup succeeds."""

    attempt: AttemptId
    request: RequestT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT]


@dataclass(frozen=True, slots=True)
class AttemptRelease(Generic[RequestT, SpecT, ResourceT]):
    """Result of asking one attempt to stop or release its retained resources."""

    processing: bool
    retired: RetiredResource[RequestT, SpecT, ResourceT] | None = None


@dataclass(slots=True)
class _Entry(Generic[RequestT, SpecT, ResourceT]):
    attempt: AttemptId
    request: RequestT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT] | None = None
    processing: bool = True
    retired: bool = False


class ResourcePool(Generic[RequestT, SpecT, ResourceT]):
    """Process-wide registry keyed by one Resource acquisition attempt.

    Conflict checking and reservation are atomic.  While an attempt is processing,
    the Pool tracks only its reservation and retirement flag; physical acquisitions
    do not publish intermediate Resources. ``finish`` atomically publishes the one
    final ResourceSet and ends processing. ``retired`` records that retention
    authority has been revoked. Request is kept only to associate an entry with its
    creator; conflicts are determined exclusively by Resource requirements.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._entries: dict[AttemptId, _Entry[RequestT, SpecT, ResourceT]] = {}

    @staticmethod
    def _validate_attempt(attempt: AttemptId) -> None:
        if not isinstance(attempt, AttemptId):
            raise TypeError("attempt must be AttemptId")

    @staticmethod
    def _validate_request(request: RequestT) -> None:
        if request is None:
            raise TypeError("request cannot be None")

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

    def snapshot(self) -> tuple[ResourceRecord[RequestT, SpecT, ResourceT], ...]:
        with self._lock:
            return tuple(
                ResourceRecord(
                    state.attempt,
                    state.request,
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
    ) -> ResourceRequestRecord[RequestT, SpecT] | None:
        self._validate_attempt(attempt)
        with self._lock:
            state = self._entries.get(attempt)
            if state is None or not state.processing:
                return None
            return self._request_record_locked(state)

    def requests(self) -> tuple[ResourceRequestRecord[RequestT, SpecT], ...]:
        with self._lock:
            return tuple(
                self._request_record_locked(state)
                for state in self._entries.values()
                if state.processing
            )

    def lookup(
        self,
        request: RequestT,
        spec: SpecT,
    ) -> tuple[ResourceSet[ResourceT], ...]:
        self._validate_request(request)
        if spec is None:
            raise TypeError("spec cannot be None")
        with self._lock:
            return tuple(
                state.resources
                for state in self._entries.values()
                if not state.processing
                and state.resources is not None
                and state.request == request
                and not state.retired
                and self._contains_spec(state.requirements, spec)
            )

    def retired(
        self,
        attempt: AttemptId,
    ) -> RetiredResource[RequestT, SpecT, ResourceT] | None:
        """Return the exact retired Resource identified by ``attempt``."""

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
        request: RequestT,
        requirements: ResourceRequirements[SpecT],
    ) -> bool:
        """Atomically conflict-check requirements and reserve them for ``attempt``."""

        self._validate_attempt(attempt)
        self._validate_request(request)
        normalized = self._validate_requirements(requirements)

        with self._lock:
            if attempt in self._entries:
                raise RuntimeError("attempt is already registered in this pool")

            for state in self._entries.values():
                if self._conflicts(state.requirements, normalized):
                    return False

            self._entries[attempt] = _Entry(
                attempt=attempt,
                request=request,
                requirements=normalized,
            )
            return True

    def release(
        self,
        attempt: AttemptId,
    ) -> AttemptRelease[RequestT, SpecT, ResourceT]:
        """Idempotently revoke retention authority for ``attempt``."""

        self._validate_attempt(attempt)
        with self._lock:
            state = self._entries.get(attempt)
            if state is None:
                return AttemptRelease(processing=False)

            state.retired = True
            if state.processing:
                return AttemptRelease(processing=True)
            if state.resources is None:
                raise RuntimeError("finished resource attempt has no ResourceSet")
            return AttemptRelease(processing=False, retired=self._retired_view(state))

    def finish(
        self,
        attempt: AttemptId,
        resources: ResourceSet[ResourceT],
    ) -> RetiredResource[RequestT, SpecT, ResourceT] | None:
        """Publish the final ResourceSet and end physical processing for ``attempt``."""

        self._validate_resources(resources)
        with self._lock:
            state = self._require_entry_locked(attempt)
            if not state.processing:
                raise RuntimeError("resource attempt is already finished")
            state.resources = resources
            state.processing = False
            if not state.retired:
                return None
            return self._retired_view(state)

    def cancel(self, attempt: AttemptId) -> bool:
        """Cancel a reservation before any physical acquisition has started."""

        self._validate_attempt(attempt)
        with self._lock:
            state = self._entries.get(attempt)
            if state is None or not state.processing:
                return False
            del self._entries[attempt]
            return True

    def discard(
        self,
        retired: RetiredResource[RequestT, SpecT, ResourceT],
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
    ) -> _Entry[RequestT, SpecT, ResourceT]:
        self._validate_attempt(attempt)
        state = self._entries.get(attempt)
        if state is None:
            raise RuntimeError("resource attempt is not current")
        return state

    @staticmethod
    def _request_record_locked(
        state: _Entry[RequestT, SpecT, ResourceT],
    ) -> ResourceRequestRecord[RequestT, SpecT]:
        return ResourceRequestRecord(
            state.attempt,
            state.request,
            state.requirements,
            state.retired,
        )

    @staticmethod
    def _retired_view(
        state: _Entry[RequestT, SpecT, ResourceT],
    ) -> RetiredResource[RequestT, SpecT, ResourceT]:
        if state.resources is None:
            raise RuntimeError("retired resource has no ResourceSet")
        return RetiredResource(
            state.attempt,
            state.request,
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
