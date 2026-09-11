from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Generic, Hashable, Protocol, TypeVar

from _resource.driver import PhysicalAcquisition, ResourceDriver, ResourceSet
from _resource.policy import ResourcePolicy
from _resource.pool import (
    GLOBAL_RESOURCE_POOL,
    ResourceLease,
    ResourcePolicies,
    ResourcePool,
    ResourceRequest,
    RetiredResource,
)


SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")


@dataclass(slots=True)
class _AcquisitionPart(Generic[SpecT, ResourceT]):
    spec: SpecT
    physical: PhysicalAcquisition[ResourceT]
    resources: ResourceSet[ResourceT] | None = None
    finished: bool = False


@dataclass(slots=True, eq=False)
class ResourceAcquisition(Generic[SpecT, ResourceT]):
    """Opaque handle for one Access resource acquisition not yet committed."""

    _access_key: Hashable
    _resource_policies: ResourcePolicies[SpecT]
    _request: ResourceRequest[Hashable, SpecT]
    _parts: tuple[_AcquisitionPart[SpecT, ResourceT], ...]
    _manager_token: object
    _resources: ResourceSet[ResourceT] | None = None


class ResourceManagement(Protocol[SpecT, ResourceT]):
    """Upper-facing resource-management boundary used by ManagedCoordinator."""

    def claim(
        self,
        access_key: Hashable,
        resource_policies: Mapping[SpecT, ResourcePolicy],
    ) -> ResourceAcquisition[SpecT, ResourceT] | None: ...

    def acquire(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
    ) -> ResourceSet[ResourceT]: ...

    def commit(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
        resources: ResourceSet[ResourceT],
    ) -> ResourceLease[Hashable, SpecT, ResourceT]: ...

    def cancel(self, acquisition: ResourceAcquisition[SpecT, ResourceT]) -> bool: ...

    def interrupt(self, acquisition: ResourceAcquisition[SpecT, ResourceT]) -> None: ...

    def abandon(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
        resources: ResourceSet[ResourceT] | None = None,
    ) -> None: ...

    def release(self, lease: ResourceLease[Hashable, SpecT, ResourceT]) -> None: ...

    def cleanup_retired(
        self,
        access_key: Hashable,
        resource_policies: Mapping[SpecT, ResourcePolicy],
    ) -> bool: ...


class ResourceManager(Generic[SpecT, ResourceT]):
    """Own Access-keyed Resource conflicts and physical-resource bookkeeping.

    One claim represents every Resource required by an Access. Each Resource spec
    gets its own physical acquisition handle, while the Pool retains one aggregate
    request/lease so the Access claim commits and releases as one unit.
    """

    def __init__(
        self,
        driver: ResourceDriver[SpecT, ResourceT],
        *,
        resource_pool: ResourcePool[Hashable, SpecT, ResourceT] = GLOBAL_RESOURCE_POOL,
    ) -> None:
        self._driver = driver
        if not isinstance(resource_pool, ResourcePool):
            raise TypeError("resource_pool must be ResourcePool")
        self._resource_pool = resource_pool
        self._token = object()

    @property
    def driver(self) -> ResourceDriver[SpecT, ResourceT]:
        return self._driver

    @property
    def resource_pool(self) -> ResourcePool[Hashable, SpecT, ResourceT]:
        return self._resource_pool

    def claim(
        self,
        access_key: Hashable,
        resource_policies: Mapping[SpecT, ResourcePolicy],
    ) -> ResourceAcquisition[SpecT, ResourceT] | None:
        request = self._resource_pool.reserve(access_key, resource_policies)
        if request is None:
            return None

        parts: list[_AcquisitionPart[SpecT, ResourceT]] = []
        try:
            for spec, _policy in request.resource_policies:
                physical = self._driver.prepare(spec)
                if physical is None:
                    raise TypeError("ResourceDriver.prepare() cannot return None")
                parts.append(_AcquisitionPart(spec, physical))
        except BaseException:
            self._resource_pool.cancel(request)
            raise

        return ResourceAcquisition(
            access_key,
            request.resource_policies,
            request,
            tuple(parts),
            self._token,
        )

    def acquire(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
    ) -> ResourceSet[ResourceT]:
        self._validate_acquisition(acquisition)

        if not acquisition._parts:
            resources: ResourceSet[ResourceT] = ()
            acquisition._resources = resources
            self._resource_pool.publish(acquisition._request, resources)
            return resources

        for part in acquisition._parts:
            snapshots = part.physical.acquire()
            if not isinstance(snapshots, Iterator):
                raise TypeError("PhysicalAcquisition.acquire() must return an Iterator")

            yielded = False
            try:
                for snapshot in snapshots:
                    yielded = True
                    if snapshot is None:
                        raise TypeError("PhysicalAcquisition.acquire() cannot yield None")
                    if not isinstance(snapshot, tuple):
                        raise TypeError(
                            "PhysicalAcquisition.acquire() must yield ResourceSet tuples"
                        )
                    part.resources = snapshot
                    self._resource_pool.publish(
                        acquisition._request,
                        self._current_resources(acquisition),
                    )
            finally:
                part.finished = True

            if not yielded:
                raise RuntimeError(
                    "PhysicalAcquisition.acquire() must yield at least one ResourceSet"
                )

        resources = self._current_resources(acquisition)
        acquisition._resources = resources
        return resources

    def commit(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
        resources: ResourceSet[ResourceT],
    ) -> ResourceLease[Hashable, SpecT, ResourceT]:
        self._validate_acquisition(acquisition)
        if resources is None:
            raise TypeError("resources cannot be None")
        if acquisition._resources is None:
            raise RuntimeError("resource acquisition has not completed")
        if resources is not acquisition._resources:
            raise RuntimeError("resources do not match the acquisition's final snapshot")

        retired = self._resource_pool.finish(acquisition._request, resources)
        if retired is not None:
            self._cleanup_retired(retired)
            raise RuntimeError("interrupted resource acquisition cannot be committed")
        return self._resource_pool.install(acquisition._request)

    def cancel(self, acquisition: ResourceAcquisition[SpecT, ResourceT]) -> bool:
        """Cancel a claimed Access before any physical producer starts."""

        self._validate_acquisition(acquisition)
        return self._resource_pool.cancel(acquisition._request)

    def interrupt(self, acquisition: ResourceAcquisition[SpecT, ResourceT]) -> None:
        """Request best-effort interruption of every still-running producer."""

        self._validate_acquisition(acquisition)
        interruption = self._resource_pool.interrupt(acquisition._request)
        if interruption.processing:
            for part in acquisition._parts:
                if not part.finished:
                    part.physical.interrupt()
        elif interruption.retired is not None:
            self._cleanup_retired(interruption.retired)

    def abandon(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
        resources: ResourceSet[ResourceT] | None = None,
    ) -> None:
        """Finish and clean an acquisition after upper-layer ownership is lost."""

        self._validate_acquisition(acquisition)
        interruption = self._resource_pool.interrupt(acquisition._request)
        if interruption.retired is not None:
            self._cleanup_retired(interruption.retired)
            return
        if not interruption.processing:
            return

        current_resources = self._current_resources_or_none(acquisition)
        retired = self._resource_pool.finish(acquisition._request, current_resources)
        if retired is not None:
            self._cleanup_retired(retired)

    def release(self, lease: ResourceLease[Hashable, SpecT, ResourceT]) -> None:
        retired = self._resource_pool.retire(lease)
        self._cleanup_retired(retired)

    def cleanup_retired(
        self,
        access_key: Hashable,
        resource_policies: Mapping[SpecT, ResourcePolicy],
    ) -> bool:
        retired_records = self._resource_pool.retired(access_key, resource_policies)
        if not retired_records:
            return False
        for retired in retired_records:
            self._cleanup_retired(retired)
        return True

    def _validate_acquisition(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
    ) -> None:
        if not isinstance(acquisition, ResourceAcquisition):
            raise TypeError("acquisition must be ResourceAcquisition")
        if acquisition._manager_token is not self._token:
            raise RuntimeError("resource acquisition belongs to another manager")

    @staticmethod
    def _current_resources(
        acquisition: ResourceAcquisition[SpecT, ResourceT],
    ) -> ResourceSet[ResourceT]:
        return tuple(
            resource
            for part in acquisition._parts
            if part.resources is not None
            for resource in part.resources
        )

    @classmethod
    def _current_resources_or_none(
        cls,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
    ) -> ResourceSet[ResourceT] | None:
        if not acquisition._parts:
            return ()
        if not any(part.resources is not None for part in acquisition._parts):
            return None
        return cls._current_resources(acquisition)

    def _cleanup_retired(
        self,
        retired: RetiredResource[Hashable, SpecT, ResourceT],
    ) -> None:
        self._driver.cleanup(retired.resources)
        self._resource_pool.discard(retired)


__all__ = ["ResourceAcquisition", "ResourceManagement", "ResourceManager"]
