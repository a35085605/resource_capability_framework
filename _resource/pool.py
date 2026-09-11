from __future__ import annotations

from dataclasses import dataclass, field
from itertools import count
from threading import Lock
from typing import Any, Generic, Hashable, TypeVar

from adb._resource.requirement import ResourcePolicy, ResourceRequirement
from adb._resource.driver import ResourceSet


ScopeT = TypeVar("ScopeT")
ResourceT = TypeVar("ResourceT")


@dataclass(frozen=True, slots=True, order=True)
class RequestId:
    """Pool-local identity for exactly one physical acquisition attempt."""

    value: int


@dataclass(frozen=True, slots=True)
class ResourceRequestRecord(Generic[ScopeT, ResourceT]):
    """Immutable point-in-time view of one physical acquisition request."""

    request_id: RequestId
    scope: ScopeT
    requirement_key: Hashable
    policy: ResourcePolicy
    resources: ResourceSet[ResourceT] | None
    interrupted: bool
    processing: bool


@dataclass(frozen=True, slots=True)
class ResourceRecord(Generic[ScopeT, ResourceT]):
    """Immutable point-in-time view of one retained physical ResourceSet."""

    scope: ScopeT
    requirement_key: Hashable
    policy: ResourcePolicy
    resources: ResourceSet[ResourceT]
    leases: int
    retired: bool = False
    request_id: RequestId | None = None


@dataclass(frozen=True, slots=True, eq=False)
class ResourceRequest(Generic[ScopeT]):
    """Identity token for one Pool-tracked physical acquisition request."""

    request_id: RequestId
    scope: ScopeT
    requirement_key: Hashable
    policy: ResourcePolicy
    _request_token: object


# Compatibility name for callers that still describe the pre-acquire claim as a
# reservation. A reservation is now a processing ResourceRequest from creation.
ResourceReservation = ResourceRequest


@dataclass(frozen=True, slots=True, eq=False)
class ResourceLease(Generic[ScopeT, ResourceT]):
    """Identity token retaining one installed ResourceSet for a consumer."""

    scope: ScopeT
    requirement_key: Hashable
    resources: ResourceSet[ResourceT]
    _record_token: object
    _lease_token: object


@dataclass(frozen=True, slots=True, eq=False)
class RetiredResource(Generic[ScopeT, ResourceT]):
    """Exact retired record that remains retained until cleanup succeeds."""

    scope: ScopeT
    requirement_key: Hashable
    resources: ResourceSet[ResourceT]
    _record_token: object
    request_id: RequestId | None = None


@dataclass(frozen=True, slots=True)
class RequestInterruption(Generic[ScopeT, ResourceT]):
    """Result of asking one physical request to stop."""

    processing: bool
    retired: RetiredResource[ScopeT, ResourceT] | None = None


@dataclass(slots=True)
class _RequestState(Generic[ScopeT, ResourceT]):
    request_id: RequestId
    scope: ScopeT
    requirement_key: Hashable
    policy: ResourcePolicy
    token: object
    resources: ResourceSet[ResourceT] | None = None
    interrupted: bool = False
    processing: bool = True


@dataclass(slots=True)
class _RecordState(Generic[ScopeT, ResourceT]):
    scope: ScopeT
    requirement_key: Hashable
    policy: ResourcePolicy
    resources: ResourceSet[ResourceT]
    token: object
    request_id: RequestId | None = None
    lease_tokens: set[object] = field(default_factory=set)
    retired: bool = False


class ResourcePool(Generic[ScopeT, ResourceT]):
    """Process-wide registry for request processing and retained ResourceSets.

    A new physical acquisition is represented in the Pool before I/O begins.
    ``interrupted`` and ``processing`` are intentionally independent: an
    interrupted producer may still publish late ResourceSet snapshots until its
    final ``finish`` call atomically publishes the last snapshot and clears
    ``processing``.

    EXCLUSIVE requirements block every retained or processing request for the
    same scope. SHARED requirements reuse only an installed active ResourceSet;
    a same-key request that is still being prepared blocks another SHARED
    acquisition. PARALLEL requirements may start independent requests. Retired
    records remain conflict-visible until physical cleanup succeeds.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._request_ids = count(1)
        self._records: list[_RecordState[ScopeT, ResourceT]] = []
        self._requests: list[_RequestState[ScopeT, ResourceT]] = []

    @staticmethod
    def _validate_scope(scope: ScopeT) -> None:
        if scope is None:
            raise TypeError("scope cannot be None")
        try:
            hash(scope)
        except TypeError as exc:
            raise TypeError("scope must be hashable") from exc

    @staticmethod
    def _validate_requirement(requirement: ResourceRequirement) -> None:
        if not isinstance(requirement, ResourceRequirement):
            raise TypeError("requirement must be ResourceRequirement")

    def snapshot(self) -> tuple[ResourceRecord[ScopeT, ResourceT], ...]:
        """Return immutable views of every retained physical ResourceSet."""

        with self._lock:
            return tuple(
                ResourceRecord(
                    state.scope,
                    state.requirement_key,
                    state.policy,
                    state.resources,
                    len(state.lease_tokens),
                    state.retired,
                    state.request_id,
                )
                for state in self._records
            )

    def request_snapshot(
        self,
        request: ResourceRequest[ScopeT],
    ) -> ResourceRequestRecord[ScopeT, ResourceT] | None:
        """Return the current request view, or ``None`` after it is consumed."""

        self._validate_request(request)
        with self._lock:
            state = self._find_request_locked(request)
            if state is None:
                return None
            return self._request_record_locked(state)

    def requests(self) -> tuple[ResourceRequestRecord[ScopeT, ResourceT], ...]:
        """Return immutable views of all requests not yet installed or retired."""

        with self._lock:
            return tuple(self._request_record_locked(state) for state in self._requests)

    def lookup(
        self,
        scope: ScopeT,
        requirement_key: Hashable,
    ) -> tuple[ResourceSet[ResourceT], ...]:
        """Return all active ResourceSets matching one scope + requirement key."""

        self._validate_scope(scope)
        if requirement_key is None:
            raise TypeError("requirement_key cannot be None")
        with self._lock:
            return tuple(
                state.resources
                for state in self._records
                if state.scope == scope
                and state.requirement_key == requirement_key
                and not state.retired
            )

    def retired(
        self,
        scope: ScopeT,
        requirement_key: Hashable,
    ) -> tuple[RetiredResource[ScopeT, ResourceT], ...]:
        """Return retired records matching one scope + requirement key."""

        self._validate_scope(scope)
        if requirement_key is None:
            raise TypeError("requirement_key cannot be None")
        with self._lock:
            return tuple(
                RetiredResource(
                    state.scope,
                    state.requirement_key,
                    state.resources,
                    state.token,
                    state.request_id,
                )
                for state in self._records
                if state.scope == scope
                and state.requirement_key == requirement_key
                and state.retired
            )

    def reserve(
        self,
        scope: ScopeT,
        requirement: ResourceRequirement,
    ) -> ResourceRequest[ScopeT] | ResourceLease[ScopeT, ResourceT] | None:
        """Reserve/reuse one requirement according to its same-scope policy.

        A newly reserved acquisition is immediately a ``processing`` request in
        the Pool, even before it has resources. SHARED may instead return an
        already-installed lease. ``None`` means the request conflicts with a
        retained/requested resource or a same-key SHARED acquisition is pending.
        """

        self._validate_scope(scope)
        self._validate_requirement(requirement)

        with self._lock:
            same_scope_records = [
                state for state in self._records if state.scope == scope
            ]
            same_scope_requests = [
                state for state in self._requests if state.scope == scope
            ]

            if requirement.policy is ResourcePolicy.EXCLUSIVE:
                if same_scope_records or same_scope_requests:
                    return None
                return self._reserve_locked(scope, requirement)

            if any(
                state.policy is ResourcePolicy.EXCLUSIVE
                for state in same_scope_records
            ) or any(
                state.policy is ResourcePolicy.EXCLUSIVE
                for state in same_scope_requests
            ):
                return None

            same_key_records = [
                state
                for state in same_scope_records
                if state.requirement_key == requirement.key
            ]
            same_key_requests = [
                state
                for state in same_scope_requests
                if state.requirement_key == requirement.key
            ]

            # One resource identity must not change coexistence semantics while any
            # record/request for that identity still exists.
            if any(state.policy is not requirement.policy for state in same_key_records):
                return None
            if any(state.policy is not requirement.policy for state in same_key_requests):
                return None

            if requirement.policy is ResourcePolicy.SHARED:
                if any(state.retired for state in same_key_records):
                    return None
                for state in same_key_records:
                    if not state.retired:
                        return self._lease_locked(state)
                if same_key_requests:
                    return None

            return self._reserve_locked(scope, requirement)

    def _reserve_locked(
        self,
        scope: ScopeT,
        requirement: ResourceRequirement,
    ) -> ResourceRequest[ScopeT]:
        request_id = RequestId(next(self._request_ids))
        token = object()
        state = _RequestState(
            request_id=request_id,
            scope=scope,
            requirement_key=requirement.key,
            policy=requirement.policy,
            token=token,
        )
        self._requests.append(state)
        return ResourceRequest(
            request_id,
            scope,
            requirement.key,
            requirement.policy,
            token,
        )

    def is_interrupted(self, request: ResourceRequest[ScopeT]) -> bool:
        """Return whether the request should stop; stale requests are stopped."""

        self._validate_request(request)
        with self._lock:
            state = self._find_request_locked(request)
            return True if state is None else state.interrupted

    def publish(
        self,
        request: ResourceRequest[ScopeT],
        resources: ResourceSet[ResourceT],
    ) -> None:
        """Publish a partial/current ResourceSet while the producer is processing.

        Publishing remains legal after interruption so late resources keep their
        Request ID ownership. It is illegal after ``finish``.
        """

        self._validate_request(request)
        if resources is None:
            raise TypeError("resources cannot be None")
        with self._lock:
            state = self._require_request_locked(request)
            if not state.processing:
                raise RuntimeError("resource request is no longer processing")
            state.resources = resources

    def interrupt(
        self,
        request: ResourceRequest[ScopeT],
    ) -> RequestInterruption[ScopeT, ResourceT]:
        """Mark a request interrupted without pretending its producer has stopped.

        If processing has already ended, interruption immediately retires the
        ResourceSet (or drops an empty request) and transfers cleanup ownership to
        the returned ``RetiredResource``. Repeated/stale interruption is harmless.
        """

        self._validate_request(request)
        with self._lock:
            state = self._find_request_locked(request)
            if state is None:
                return RequestInterruption(processing=False)

            state.interrupted = True
            if state.processing:
                return RequestInterruption(processing=True)

            retired = self._retire_request_locked(state)
            return RequestInterruption(processing=False, retired=retired)

    def finish(
        self,
        request: ResourceRequest[ScopeT],
        resources: ResourceSet[ResourceT] | None = None,
    ) -> RetiredResource[ScopeT, ResourceT] | None:
        """Atomically publish the final snapshot and set ``processing=False``.

        An interrupted request is retired immediately once processing ends. If it
        never produced resources, the request simply disappears. A successful,
        non-interrupted request remains in the Pool until ``install`` consumes it.
        """

        self._validate_request(request)
        with self._lock:
            state = self._require_request_locked(request)
            if not state.processing:
                raise RuntimeError("resource request is already finished")
            if resources is not None:
                state.resources = resources
            state.processing = False
            if state.interrupted:
                return self._retire_request_locked(state)
            return None

    def cancel(self, request: ResourceRequest[ScopeT]) -> bool:
        """Cancel a request that is known not to have started producing resources.

        This compatibility operation is intentionally strict: once any ResourceSet
        has been published, callers must use ``interrupt`` + ``finish`` so late
        resources cannot lose ownership.
        """

        self._validate_request(request)
        with self._lock:
            state = self._find_request_locked(request)
            if state is None:
                return False
            if state.resources is not None:
                raise RuntimeError("cannot cancel a request that already has resources")
            self._requests.remove(state)
            return True

    def install(
        self,
        request: ResourceRequest[ScopeT],
        resources: ResourceSet[ResourceT] | None = None,
    ) -> ResourceLease[ScopeT, ResourceT]:
        """Consume one finished request and return the first retained lease.

        ``resources`` is accepted only for compatibility with the old reservation
        API. When supplied it must be the exact final ResourceSet snapshot already
        recorded by ``finish``.
        """

        self._validate_request(request)
        with self._lock:
            state = self._require_request_locked(request)
            if state.processing:
                raise RuntimeError("resource request is still processing")
            if state.interrupted:
                raise RuntimeError("interrupted resource request cannot be installed")
            if state.resources is None:
                raise RuntimeError("resource request has no final resources")
            if resources is not None and resources is not state.resources:
                raise RuntimeError("resources do not match the request's final snapshot")

            token = object()
            record = _RecordState(
                scope=state.scope,
                requirement_key=state.requirement_key,
                policy=state.policy,
                resources=state.resources,
                token=token,
                request_id=state.request_id,
            )
            self._records.append(record)
            self._requests.remove(state)
            return self._lease_locked(record)

    @staticmethod
    def _lease_locked(
        state: _RecordState[ScopeT, ResourceT],
    ) -> ResourceLease[ScopeT, ResourceT]:
        lease_token = object()
        state.lease_tokens.add(lease_token)
        return ResourceLease(
            state.scope,
            state.requirement_key,
            state.resources,
            state.token,
            lease_token,
        )

    def retire(
        self,
        lease: ResourceLease[ScopeT, ResourceT],
    ) -> RetiredResource[ScopeT, ResourceT] | None:
        """Release one lease; retire the ResourceSet when its last lease detaches."""

        if not isinstance(lease, ResourceLease):
            raise TypeError("lease must be ResourceLease")

        with self._lock:
            state = next(
                (state for state in self._records if state.token is lease._record_token),
                None,
            )
            if state is None:
                raise RuntimeError("resource lease is not retained")
            if state.retired:
                raise RuntimeError("resource lease is already retired")
            if state.resources is not lease.resources:
                raise RuntimeError("resource lease does not match retained resources")
            if lease._lease_token not in state.lease_tokens:
                raise RuntimeError("resource lease is not current")

            state.lease_tokens.remove(lease._lease_token)
            if state.lease_tokens:
                return None

            state.retired = True
            return RetiredResource(
                state.scope,
                state.requirement_key,
                state.resources,
                state.token,
                state.request_id,
            )

    def discard(self, retired: RetiredResource[ScopeT, ResourceT]) -> None:
        """Forget one exact retired record after physical cleanup succeeds."""

        if not isinstance(retired, RetiredResource):
            raise TypeError("retired must be RetiredResource")

        with self._lock:
            record_index = next(
                (
                    index
                    for index, state in enumerate(self._records)
                    if state.token is retired._record_token
                ),
                None,
            )
            if record_index is None:
                raise RuntimeError("retired resource is not retained")
            state = self._records[record_index]
            if state.resources is not retired.resources:
                raise RuntimeError("retired resource does not match retained resources")
            if not state.retired:
                raise RuntimeError("resource set must be retired before discard")
            del self._records[record_index]

    @staticmethod
    def _validate_request(request: ResourceRequest[ScopeT]) -> None:
        if not isinstance(request, ResourceRequest):
            raise TypeError("request must be ResourceRequest")

    def _find_request_locked(
        self,
        request: ResourceRequest[ScopeT],
    ) -> _RequestState[ScopeT, ResourceT] | None:
        return next(
            (
                state
                for state in self._requests
                if state.token is request._request_token
                and state.request_id == request.request_id
            ),
            None,
        )

    def _require_request_locked(
        self,
        request: ResourceRequest[ScopeT],
    ) -> _RequestState[ScopeT, ResourceT]:
        state = self._find_request_locked(request)
        if state is None:
            raise RuntimeError("resource request is not current")
        return state

    @staticmethod
    def _request_record_locked(
        state: _RequestState[ScopeT, ResourceT],
    ) -> ResourceRequestRecord[ScopeT, ResourceT]:
        return ResourceRequestRecord(
            state.request_id,
            state.scope,
            state.requirement_key,
            state.policy,
            state.resources,
            state.interrupted,
            state.processing,
        )

    def _retire_request_locked(
        self,
        state: _RequestState[ScopeT, ResourceT],
    ) -> RetiredResource[ScopeT, ResourceT] | None:
        self._requests.remove(state)
        if state.resources is None:
            return None

        token = object()
        record = _RecordState(
            scope=state.scope,
            requirement_key=state.requirement_key,
            policy=state.policy,
            resources=state.resources,
            token=token,
            request_id=state.request_id,
            retired=True,
        )
        self._records.append(record)
        return RetiredResource(
            record.scope,
            record.requirement_key,
            record.resources,
            record.token,
            record.request_id,
        )


GLOBAL_RESOURCE_POOL: ResourcePool[Any, Any] = ResourcePool()


__all__ = [
    "GLOBAL_RESOURCE_POOL",
    "RequestId",
    "RequestInterruption",
    "ResourceLease",
    "ResourcePool",
    "ResourceRecord",
    "ResourceRequest",
    "ResourceRequestRecord",
    "ResourceReservation",
    "RetiredResource",
]
