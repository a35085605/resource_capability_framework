from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any, Generic, TypeVar

from _attempt import AttemptId
from _resource.driver import PhysicalResourceSet
from _resource.key import ResourceKey, ResourceKeys
from _resource.policy import ResourcePolicy
from _resource.requirement import ResourceRequirement, ResourceRequirements


RequestT = TypeVar("RequestT")
SpecT = TypeVar("SpecT")
PhysicalResourceT = TypeVar("PhysicalResourceT")


@dataclass(frozen=True, slots=True)
class ResourceRequestRecord(Generic[RequestT, SpecT]):
    """Immutable view of one reserved attempt still being physically acquired."""

    attempt: AttemptId
    request: RequestT
    requirements: ResourceRequirements[SpecT]
    keys: ResourceKeys
    retired: bool


@dataclass(frozen=True, slots=True)
class PhysicalResourceRecord(Generic[RequestT, SpecT, PhysicalResourceT]):
    """Immutable point-in-time view of one retained or retired physical-resource set."""

    attempt: AttemptId
    request: RequestT
    requirements: ResourceRequirements[SpecT]
    keys: ResourceKeys
    resources: PhysicalResourceSet[PhysicalResourceT]
    retired: bool = False


@dataclass(frozen=True, slots=True, eq=False)
class RetiredPhysicalResource(Generic[RequestT, SpecT, PhysicalResourceT]):
    """Exact retired attempt retained until physical cleanup succeeds."""

    attempt: AttemptId
    request: RequestT
    requirements: ResourceRequirements[SpecT]
    keys: ResourceKeys
    resources: PhysicalResourceSet[PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class AttemptRelease(Generic[RequestT, SpecT, PhysicalResourceT]):
    """Result of asking one attempt to stop or release its retained resources."""

    processing: bool
    retired: RetiredPhysicalResource[RequestT, SpecT, PhysicalResourceT] | None = None


@dataclass(slots=True)
class _Entry(Generic[RequestT, SpecT, PhysicalResourceT]):
    attempt: AttemptId
    request: RequestT
    requirements: ResourceRequirements[SpecT]
    keys: ResourceKeys
    resources: PhysicalResourceSet[PhysicalResourceT] | None = None
    processing: bool = True
    retired: bool = False


class ResourcePool(Generic[RequestT, SpecT, PhysicalResourceT]):
    """Process-wide reservation and retention registry.

    Entries remain keyed by ``AttemptId`` for lifecycle ownership. Logical resource
    conflicts are determined only by canonical ``ResourceKey`` values resolved before
    reservation; physical specs are retained for inspection and driver execution but
    their equality no longer defines resource identity.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._entries: dict[AttemptId, _Entry[RequestT, SpecT, PhysicalResourceT]] = {}

    @staticmethod
    def _validate_attempt(attempt: AttemptId) -> None:
        if not isinstance(attempt, AttemptId):
            raise TypeError("attempt must be AttemptId")

    @staticmethod
    def _validate_request(request: RequestT) -> None:
        if request is None:
            raise TypeError("request cannot be None")

    @staticmethod
    def _validate_reservation(
        requirements: ResourceRequirements[SpecT],
        keys: ResourceKeys,
    ) -> tuple[ResourceRequirements[SpecT], ResourceKeys]:
        if not isinstance(requirements, tuple):
            raise TypeError("requirements must be a tuple")
        if not isinstance(keys, tuple):
            raise TypeError("keys must be a ResourceKeys tuple")
        if len(requirements) != len(keys):
            raise ValueError("requirements and keys must have the same length")

        seen_keys: set[ResourceKey] = set()
        for requirement, key in zip(requirements, keys, strict=True):
            if not isinstance(requirement, ResourceRequirement):
                raise TypeError("requirements must contain ResourceRequirement values")
            if not isinstance(key, ResourceKey):
                raise TypeError("keys must contain ResourceKey values")
            if key in seen_keys:
                raise ValueError("requirements cannot contain duplicate resource keys")
            seen_keys.add(key)
        return requirements, keys

    def snapshot(
        self,
    ) -> tuple[PhysicalResourceRecord[RequestT, SpecT, PhysicalResourceT], ...]:
        with self._lock:
            return tuple(
                PhysicalResourceRecord(
                    state.attempt,
                    state.request,
                    state.requirements,
                    state.keys,
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
        key: ResourceKey,
    ) -> tuple[PhysicalResourceSet[PhysicalResourceT], ...]:
        """Return retained physical-resource sets associated with ``request`` + ``key``."""

        self._validate_request(request)
        if not isinstance(key, ResourceKey):
            raise TypeError("key must be ResourceKey")
        with self._lock:
            return tuple(
                state.resources
                for state in self._entries.values()
                if not state.processing
                and state.resources is not None
                and state.request == request
                and not state.retired
                and key in state.keys
            )

    def retired(
        self,
        attempt: AttemptId,
    ) -> RetiredPhysicalResource[RequestT, SpecT, PhysicalResourceT] | None:
        """Return the exact retired physical-resource set identified by ``attempt``."""

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
        keys: ResourceKeys,
    ) -> bool:
        """Atomically conflict-check canonical keys and reserve them for ``attempt``."""

        self._validate_attempt(attempt)
        self._validate_request(request)
        normalized_requirements, normalized_keys = self._validate_reservation(
            requirements,
            keys,
        )

        with self._lock:
            if attempt in self._entries:
                raise RuntimeError("attempt is already registered in this pool")

            for state in self._entries.values():
                if self._conflicts(
                    state.requirements,
                    state.keys,
                    normalized_requirements,
                    normalized_keys,
                ):
                    return False

            self._entries[attempt] = _Entry(
                attempt=attempt,
                request=request,
                requirements=normalized_requirements,
                keys=normalized_keys,
            )
            return True

    def release(
        self,
        attempt: AttemptId,
    ) -> AttemptRelease[RequestT, SpecT, PhysicalResourceT]:
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
                raise RuntimeError("finished resource attempt has no PhysicalResourceSet")
            return AttemptRelease(processing=False, retired=self._retired_view(state))

    def finish(
        self,
        attempt: AttemptId,
        resources: PhysicalResourceSet[PhysicalResourceT],
    ) -> RetiredPhysicalResource[RequestT, SpecT, PhysicalResourceT] | None:
        """Publish the final PhysicalResourceSet and end processing for ``attempt``."""

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
        retired: RetiredPhysicalResource[RequestT, SpecT, PhysicalResourceT],
    ) -> None:
        if not isinstance(retired, RetiredPhysicalResource):
            raise TypeError("retired must be RetiredPhysicalResource")

        with self._lock:
            state = self._entries.get(retired.attempt)
            if state is None:
                raise RuntimeError("retired physical resource is not retained")
            if state.resources is not retired.resources:
                raise RuntimeError(
                    "retired physical resource does not match retained resources"
                )
            if state.processing:
                raise RuntimeError("processing resource attempt cannot be discarded")
            if not state.retired:
                raise RuntimeError("physical resource set must be retired before discard")
            del self._entries[retired.attempt]

    @staticmethod
    def _validate_resources(resources: PhysicalResourceSet[PhysicalResourceT]) -> None:
        if resources is None:
            raise TypeError("resources cannot be None")
        if not isinstance(resources, tuple):
            raise TypeError("resources must be a PhysicalResourceSet tuple")

    def _require_entry_locked(
        self,
        attempt: AttemptId,
    ) -> _Entry[RequestT, SpecT, PhysicalResourceT]:
        self._validate_attempt(attempt)
        state = self._entries.get(attempt)
        if state is None:
            raise RuntimeError("resource attempt is not current")
        return state

    @staticmethod
    def _request_record_locked(
        state: _Entry[RequestT, SpecT, PhysicalResourceT],
    ) -> ResourceRequestRecord[RequestT, SpecT]:
        return ResourceRequestRecord(
            state.attempt,
            state.request,
            state.requirements,
            state.keys,
            state.retired,
        )

    @staticmethod
    def _retired_view(
        state: _Entry[RequestT, SpecT, PhysicalResourceT],
    ) -> RetiredPhysicalResource[RequestT, SpecT, PhysicalResourceT]:
        if state.resources is None:
            raise RuntimeError("retired physical resource has no PhysicalResourceSet")
        return RetiredPhysicalResource(
            state.attempt,
            state.request,
            state.requirements,
            state.keys,
            state.resources,
        )

    @staticmethod
    def _conflicts(
        existing_requirements: ResourceRequirements[SpecT],
        existing_keys: ResourceKeys,
        incoming_requirements: ResourceRequirements[SpecT],
        incoming_keys: ResourceKeys,
    ) -> bool:
        existing = zip(existing_requirements, existing_keys, strict=True)
        existing_pairs = tuple(existing)
        for incoming_requirement, incoming_key in zip(
            incoming_requirements,
            incoming_keys,
            strict=True,
        ):
            for existing_requirement, existing_key in existing_pairs:
                if existing_key != incoming_key:
                    continue
                if existing_requirement.policy is ResourcePolicy.BLOCKING:
                    return True
        return False


GLOBAL_RESOURCE_POOL: ResourcePool[Any, Any, Any] = ResourcePool()


__all__ = [
    "AttemptRelease",
    "GLOBAL_RESOURCE_POOL",
    "PhysicalResourceRecord",
    "ResourcePool",
    "ResourceRequestRecord",
    "RetiredPhysicalResource",
]
