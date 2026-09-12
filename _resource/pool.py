from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any, Generic, TypeVar

from _attempt import AttemptId
from _resource.key import ResourceKey, ResourceKeys
from _resource.policy import ResourcePolicy
from _resource.requirement import ResourceRequirement, ResourceRequirements


RequestT = TypeVar("RequestT")
RequirementT = TypeVar("RequirementT", bound=ResourceRequirement)


@dataclass(frozen=True, slots=True)
class ResourceReservationRecord(Generic[RequestT, RequirementT]):
    """Immutable view of one logical resource reservation."""

    attempt: AttemptId
    request: RequestT
    requirements: ResourceRequirements[RequirementT]
    keys: ResourceKeys


@dataclass(frozen=True, slots=True)
class _Entry(Generic[RequestT, RequirementT]):
    attempt: AttemptId
    request: RequestT
    requirements: ResourceRequirements[RequirementT]
    keys: ResourceKeys


class ResourceReservationTable(Generic[RequestT, RequirementT]):
    """Process-wide logical-resource reservation table.

    Entries remain keyed by ``AttemptId`` until the owning ``ResourceManager`` has
    completed physical cleanup. The table knows nothing about physical acquisition,
    retention, retirement, or cleanup; it only provides atomic conflict checking and
    reservation ownership for canonical ``ResourceKey`` values.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._entries: dict[AttemptId, _Entry[RequestT, RequirementT]] = {}

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
        requirements: ResourceRequirements[RequirementT],
        keys: ResourceKeys,
    ) -> tuple[ResourceRequirements[RequirementT], ResourceKeys]:
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
            if not isinstance(requirement.policy, ResourcePolicy):
                raise TypeError("resource policy must be ResourcePolicy")
            if not isinstance(key, ResourceKey):
                raise TypeError("keys must contain ResourceKey values")
            if key in seen_keys:
                raise ValueError("requirements cannot contain duplicate resource keys")
            seen_keys.add(key)
        return requirements, keys

    def reservation(
        self,
        attempt: AttemptId,
    ) -> ResourceReservationRecord[RequestT, RequirementT] | None:
        self._validate_attempt(attempt)
        with self._lock:
            state = self._entries.get(attempt)
            if state is None:
                return None
            return self._record_locked(state)

    def reservations(self) -> tuple[ResourceReservationRecord[RequestT, RequirementT], ...]:
        with self._lock:
            return tuple(self._record_locked(state) for state in self._entries.values())

    def reserve(
        self,
        attempt: AttemptId,
        request: RequestT,
        requirements: ResourceRequirements[RequirementT],
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
                raise RuntimeError("attempt is already registered in this reservation table")

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

    def release_reservation(self, attempt: AttemptId) -> None:
        """Release an exact reservation after physical cleanup has succeeded."""

        self._validate_attempt(attempt)
        with self._lock:
            if attempt not in self._entries:
                raise RuntimeError("resource attempt is not reserved")
            del self._entries[attempt]

    @staticmethod
    def _record_locked(
        state: _Entry[RequestT, RequirementT],
    ) -> ResourceReservationRecord[RequestT, RequirementT]:
        return ResourceReservationRecord(
            state.attempt,
            state.request,
            state.requirements,
            state.keys,
        )

    @staticmethod
    def _conflicts(
        existing_requirements: ResourceRequirements[RequirementT],
        existing_keys: ResourceKeys,
        incoming_requirements: ResourceRequirements[RequirementT],
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


GLOBAL_RESOURCE_RESERVATION_TABLE: ResourceReservationTable[Any, Any] = (
    ResourceReservationTable()
)

# Compatibility names for callers that still construct/inject the old pool object.
# The aliased object now provides reservation semantics only.
ResourcePool = ResourceReservationTable
GLOBAL_RESOURCE_POOL = GLOBAL_RESOURCE_RESERVATION_TABLE


__all__ = [
    "GLOBAL_RESOURCE_POOL",
    "GLOBAL_RESOURCE_RESERVATION_TABLE",
    "ResourcePool",
    "ResourceReservationRecord",
    "ResourceReservationTable",
]
