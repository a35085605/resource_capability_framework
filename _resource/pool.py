from __future__ import annotations

from dataclasses import dataclass
from itertools import count
from threading import Lock
from typing import Any, Generic, Hashable, TypeVar

from _resource.driver import ResourceSet
from _resource.policy import ResourcePolicy
from _resource.requirement import ResourceRequirement, ResourceRequirements


AccessKeyT = TypeVar("AccessKeyT")
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")


@dataclass(frozen=True, slots=True, order=True)
class RequestId:
    """Pool-local identity for exactly one Access resource acquisition attempt."""

    value: int


@dataclass(frozen=True, slots=True)
class ResourceRequestRecord(Generic[AccessKeyT, SpecT, ResourceT]):
    """Immutable point-in-time view of one Access resource request."""

    request_id: RequestId
    access_key: AccessKeyT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT] | None
    interrupted: bool
    processing: bool


@dataclass(frozen=True, slots=True)
class ResourceRecord(Generic[AccessKeyT, SpecT, ResourceT]):
    """Immutable point-in-time view of one retained Access ResourceSet."""

    access_key: AccessKeyT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT]
    retired: bool = False
    request_id: RequestId | None = None


@dataclass(frozen=True, slots=True, eq=False)
class ResourceRequest(Generic[AccessKeyT, SpecT]):
    """Identity token for one Pool-tracked Access resource request."""

    request_id: RequestId
    access_key: AccessKeyT
    requirements: ResourceRequirements[SpecT]
    _request_token: object


@dataclass(frozen=True, slots=True, eq=False)
class ResourceLease(Generic[AccessKeyT, SpecT, ResourceT]):
    """Identity token retaining one installed Access ResourceSet."""

    access_key: AccessKeyT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT]
    _record_token: object


@dataclass(frozen=True, slots=True, eq=False)
class RetiredResource(Generic[AccessKeyT, SpecT, ResourceT]):
    """Exact retired record that remains retained until cleanup succeeds."""

    access_key: AccessKeyT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT]
    _record_token: object
    request_id: RequestId | None = None


@dataclass(frozen=True, slots=True)
class RequestInterruption(Generic[AccessKeyT, SpecT, ResourceT]):
    """Result of asking one Access resource request to stop."""

    processing: bool
    retired: RetiredResource[AccessKeyT, SpecT, ResourceT] | None = None


@dataclass(slots=True)
class _RequestState(Generic[AccessKeyT, SpecT, ResourceT]):
    request_id: RequestId
    access_key: AccessKeyT
    requirements: ResourceRequirements[SpecT]
    token: object
    resources: ResourceSet[ResourceT] | None = None
    interrupted: bool = False
    processing: bool = True


@dataclass(slots=True)
class _RecordState(Generic[AccessKeyT, SpecT, ResourceT]):
    access_key: AccessKeyT
    requirements: ResourceRequirements[SpecT]
    resources: ResourceSet[ResourceT]
    token: object
    request_id: RequestId | None = None
    retired: bool = False


class ResourcePool(Generic[AccessKeyT, SpecT, ResourceT]):
    """Process-wide registry for Access-keyed Resource requests and leases.

    Resource identity is ``ResourceRequirement.spec`` from the implementation's
    Access plan. Policy is considered only when the same Access key asks for the
    same Resource spec:

    * BLOCKING: the Resource cannot coexist with another matching Resource.
    * NON_BLOCKING: matching Resources may be acquired independently.

    Conflict is determined by the Resource that already exists: an existing
    BLOCKING Resource blocks another matching request, while an existing
    NON_BLOCKING Resource does not. Different Access keys never conflict here.
    Retired records remain conflict-visible until physical cleanup succeeds.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._request_ids = count(1)
        self._records: list[_RecordState[AccessKeyT, SpecT, ResourceT]] = []
        self._requests: list[_RequestState[AccessKeyT, SpecT, ResourceT]] = []

    @staticmethod
    def _validate_access_key(access_key: AccessKeyT) -> None:
        if access_key is None:
            raise TypeError("access_key cannot be None")
        try:
            hash(access_key)
        except TypeError as exc:
            raise TypeError("access_key must be hashable") from exc

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

    def snapshot(self) -> tuple[ResourceRecord[AccessKeyT, SpecT, ResourceT], ...]:
        """Return immutable views of every retained Access ResourceSet."""

        with self._lock:
            return tuple(
                ResourceRecord(
                    state.access_key,
                    state.requirements,
                    state.resources,
                    state.retired,
                    state.request_id,
                )
                for state in self._records
            )

    def request_snapshot(
        self,
        request: ResourceRequest[AccessKeyT, SpecT],
    ) -> ResourceRequestRecord[AccessKeyT, SpecT, ResourceT] | None:
        """Return the current request view, or ``None`` after it is consumed."""

        self._validate_request(request)
        with self._lock:
            state = self._find_request_locked(request)
            if state is None:
                return None
            return self._request_record_locked(state)

    def requests(
        self,
    ) -> tuple[ResourceRequestRecord[AccessKeyT, SpecT, ResourceT], ...]:
        """Return immutable views of all requests not yet installed or retired."""

        with self._lock:
            return tuple(self._request_record_locked(state) for state in self._requests)

    def lookup(
        self,
        access_key: AccessKeyT,
        spec: SpecT,
    ) -> tuple[ResourceSet[ResourceT], ...]:
        """Return active ResourceSets containing one Access-keyed Resource spec."""

        self._validate_access_key(access_key)
        if spec is None:
            raise TypeError("spec cannot be None")
        with self._lock:
            return tuple(
                state.resources
                for state in self._records
                if state.access_key == access_key
                and not state.retired
                and self._contains_spec(state.requirements, spec)
            )

    def retired(
        self,
        access_key: AccessKeyT,
        requirements: ResourceRequirements[SpecT],
    ) -> tuple[RetiredResource[AccessKeyT, SpecT, ResourceT], ...]:
        """Return retired records matching one Access key and Resource spec set."""

        self._validate_access_key(access_key)
        normalized = self._validate_requirements(requirements)

        with self._lock:
            return tuple(
                RetiredResource(
                    state.access_key,
                    state.requirements,
                    state.resources,
                    state.token,
                    state.request_id,
                )
                for state in self._records
                if state.access_key == access_key
                and state.retired
                and self._same_spec_set(state.requirements, normalized)
            )

    def reserve(
        self,
        access_key: AccessKeyT,
        requirements: ResourceRequirements[SpecT],
    ) -> ResourceRequest[AccessKeyT, SpecT] | None:
        """Atomically reserve every Resource required by one Access.

        ``None`` means at least one Resource conflicts with a retained, retired, or
        processing Resource for the same Access key. NON_BLOCKING Resources never
        reuse an existing ResourceSet; a successful reserve always creates a fresh
        request.
        """

        self._validate_access_key(access_key)
        normalized = self._validate_requirements(requirements)

        with self._lock:
            for state in self._records:
                if state.access_key == access_key and self._conflicts(
                    state.requirements, normalized
                ):
                    return None
            for state in self._requests:
                if state.access_key == access_key and self._conflicts(
                    state.requirements, normalized
                ):
                    return None
            return self._reserve_locked(access_key, normalized)

    def _reserve_locked(
        self,
        access_key: AccessKeyT,
        requirements: ResourceRequirements[SpecT],
    ) -> ResourceRequest[AccessKeyT, SpecT]:
        request_id = RequestId(next(self._request_ids))
        token = object()
        state = _RequestState(
            request_id=request_id,
            access_key=access_key,
            requirements=requirements,
            token=token,
        )
        self._requests.append(state)
        return ResourceRequest(request_id, access_key, requirements, token)

    def is_interrupted(self, request: ResourceRequest[AccessKeyT, SpecT]) -> bool:
        """Return whether the request should stop; stale requests are stopped."""

        self._validate_request(request)
        with self._lock:
            state = self._find_request_locked(request)
            return True if state is None else state.interrupted

    def publish(
        self,
        request: ResourceRequest[AccessKeyT, SpecT],
        resources: ResourceSet[ResourceT],
    ) -> None:
        """Publish the current aggregate ResourceSet while processing."""

        self._validate_request(request)
        if resources is None:
            raise TypeError("resources cannot be None")
        if not isinstance(resources, tuple):
            raise TypeError("resources must be a ResourceSet tuple")
        with self._lock:
            state = self._require_request_locked(request)
            if not state.processing:
                raise RuntimeError("resource request is no longer processing")
            state.resources = resources

    def interrupt(
        self,
        request: ResourceRequest[AccessKeyT, SpecT],
    ) -> RequestInterruption[AccessKeyT, SpecT, ResourceT]:
        """Mark a request interrupted without pretending its producers stopped."""

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
        request: ResourceRequest[AccessKeyT, SpecT],
        resources: ResourceSet[ResourceT] | None = None,
    ) -> RetiredResource[AccessKeyT, SpecT, ResourceT] | None:
        """Atomically publish the final aggregate snapshot and end processing."""

        self._validate_request(request)
        if resources is not None and not isinstance(resources, tuple):
            raise TypeError("resources must be a ResourceSet tuple")
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

    def cancel(self, request: ResourceRequest[AccessKeyT, SpecT]) -> bool:
        """Cancel a reserved request before any physical Resource is published."""

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
        request: ResourceRequest[AccessKeyT, SpecT],
    ) -> ResourceLease[AccessKeyT, SpecT, ResourceT]:
        """Consume one finished request and retain its aggregate ResourceSet."""

        self._validate_request(request)
        with self._lock:
            state = self._require_request_locked(request)
            if state.processing:
                raise RuntimeError("resource request is still processing")
            if state.interrupted:
                raise RuntimeError("interrupted resource request cannot be installed")
            if state.resources is None:
                raise RuntimeError("resource request has no final resources")

            token = object()
            record = _RecordState(
                access_key=state.access_key,
                requirements=state.requirements,
                resources=state.resources,
                token=token,
                request_id=state.request_id,
            )
            self._records.append(record)
            self._requests.remove(state)
            return ResourceLease(
                record.access_key,
                record.requirements,
                record.resources,
                record.token,
            )

    def retire(
        self,
        lease: ResourceLease[AccessKeyT, SpecT, ResourceT],
    ) -> RetiredResource[AccessKeyT, SpecT, ResourceT]:
        """Release one lease and transfer its ResourceSet to cleanup ownership."""

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

            state.retired = True
            return RetiredResource(
                state.access_key,
                state.requirements,
                state.resources,
                state.token,
                state.request_id,
            )

    def discard(
        self,
        retired: RetiredResource[AccessKeyT, SpecT, ResourceT],
    ) -> None:
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
    def _validate_request(request: ResourceRequest[AccessKeyT, SpecT]) -> None:
        if not isinstance(request, ResourceRequest):
            raise TypeError("request must be ResourceRequest")

    def _find_request_locked(
        self,
        request: ResourceRequest[AccessKeyT, SpecT],
    ) -> _RequestState[AccessKeyT, SpecT, ResourceT] | None:
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
        request: ResourceRequest[AccessKeyT, SpecT],
    ) -> _RequestState[AccessKeyT, SpecT, ResourceT]:
        state = self._find_request_locked(request)
        if state is None:
            raise RuntimeError("resource request is not current")
        return state

    @staticmethod
    def _request_record_locked(
        state: _RequestState[AccessKeyT, SpecT, ResourceT],
    ) -> ResourceRequestRecord[AccessKeyT, SpecT, ResourceT]:
        return ResourceRequestRecord(
            state.request_id,
            state.access_key,
            state.requirements,
            state.resources,
            state.interrupted,
            state.processing,
        )

    def _retire_request_locked(
        self,
        state: _RequestState[AccessKeyT, SpecT, ResourceT],
    ) -> RetiredResource[AccessKeyT, SpecT, ResourceT] | None:
        self._requests.remove(state)
        if state.resources is None:
            return None

        token = object()
        record = _RecordState(
            access_key=state.access_key,
            requirements=state.requirements,
            resources=state.resources,
            token=token,
            request_id=state.request_id,
            retired=True,
        )
        self._records.append(record)
        return RetiredResource(
            record.access_key,
            record.requirements,
            record.resources,
            record.token,
            record.request_id,
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
    "GLOBAL_RESOURCE_POOL",
    "RequestId",
    "RequestInterruption",
    "ResourceLease",
    "ResourcePool",
    "ResourceRecord",
    "ResourceRequest",
    "ResourceRequestRecord",
    "RetiredResource",
]
