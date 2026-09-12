from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any, Generic, TypeVar

from _attempt import AttemptId
from _resource.claim import ResourceClaim, ResourceClaims
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
    claims: ResourceClaims[RequirementT]

    @property
    def requirements(self) -> ResourceRequirements[RequirementT]:
        """Compatibility view of the reserved requirements."""

        return tuple(claim.requirement for claim in self.claims)

    @property
    def keys(self) -> ResourceKeys:
        """Compatibility view of the reserved canonical keys."""

        return tuple(claim.key for claim in self.claims)


@dataclass(frozen=True, slots=True)
class _Entry(Generic[RequestT, RequirementT]):
    attempt: AttemptId
    request: RequestT
    claims: ResourceClaims[RequirementT]


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
    def _validate_claims(
        claims: ResourceClaims[RequirementT],
    ) -> ResourceClaims[RequirementT]:
        if not isinstance(claims, tuple):
            raise TypeError("claims must be a tuple")

        seen_keys: set[ResourceKey] = set()
        for claim in claims:
            if not isinstance(claim, ResourceClaim):
                raise TypeError("claims must contain ResourceClaim values")
            requirement = claim.requirement
            key = claim.key
            if not isinstance(requirement, ResourceRequirement):
                raise TypeError("claims must contain ResourceRequirement values")
            if not isinstance(requirement.policy, ResourcePolicy):
                raise TypeError("resource policy must be ResourcePolicy")
            if not isinstance(key, ResourceKey):
                raise TypeError("claims must contain ResourceKey values")
            if key in seen_keys:
                raise ValueError("claims cannot contain duplicate resource keys")
            seen_keys.add(key)
        return claims

    @classmethod
    def _normalize_claims(
        cls,
        claims_or_requirements: ResourceClaims[RequirementT]
        | ResourceRequirements[RequirementT],
        keys: ResourceKeys | None,
    ) -> ResourceClaims[RequirementT]:
        if keys is None:
            return cls._validate_claims(claims_or_requirements)  # type: ignore[arg-type]

        requirements = claims_or_requirements
        if not isinstance(requirements, tuple):
            raise TypeError("requirements must be a tuple")
        if not isinstance(keys, tuple):
            raise TypeError("keys must be a ResourceKeys tuple")
        if len(requirements) != len(keys):
            raise ValueError("requirements and keys must have the same length")

        claims = tuple(
            ResourceClaim(requirement, key)
            for requirement, key in zip(requirements, keys, strict=True)
        )
        return cls._validate_claims(claims)

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
        claims_or_requirements: ResourceClaims[RequirementT]
        | ResourceRequirements[RequirementT],
        keys: ResourceKeys | None = None,
    ) -> bool:
        """Atomically conflict-check and reserve canonical resource claims.

        Passing ``requirements, keys`` remains supported as a compatibility form; the
        table normalizes that input to ``ResourceClaim`` values before storing it.
        """

        self._validate_attempt(attempt)
        self._validate_request(request)
        claims = self._normalize_claims(claims_or_requirements, keys)

        with self._lock:
            if attempt in self._entries:
                raise RuntimeError("attempt is already registered in this reservation table")

            for state in self._entries.values():
                if self._conflicts(state.claims, claims):
                    return False

            self._entries[attempt] = _Entry(
                attempt=attempt,
                request=request,
                claims=claims,
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
            state.claims,
        )

    @staticmethod
    def _conflicts(
        existing_claims: ResourceClaims[RequirementT],
        incoming_claims: ResourceClaims[RequirementT],
    ) -> bool:
        for incoming in incoming_claims:
            for existing in existing_claims:
                if existing.key != incoming.key:
                    continue
                # Conflict policy is intentionally directional: only the already-held
                # reservation decides whether a later matching claim is blocked.
                if existing.requirement.policy is ResourcePolicy.BLOCKING:
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
