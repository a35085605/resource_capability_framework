from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Generic, Hashable, Protocol, TypeVar

from adb._resource.driver import AcquisitionContext, ResourceDriver, ResourceSet
from adb._resource.plan import ResourcePlan
from adb._resource.pool import (
    GLOBAL_RESOURCE_POOL,
    ResourceLease,
    ResourcePool,
    ResourceRequest,
    RetiredResource,
)


SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")


@dataclass(frozen=True, slots=True, eq=False)
class ResourceAcquisition(Generic[SpecT, ResourceT]):
    """Opaque handle for one resource acquisition that has not been committed."""

    _plan: ResourcePlan[SpecT]
    _request: ResourceRequest[Hashable]
    _manager_token: object


class ResourceManagement(Protocol[SpecT, ResourceT]):
    """Upper-facing resource-management boundary used by ManagedCoordinator."""

    def claim(
        self,
        plan: ResourcePlan[SpecT],
    ) -> ResourceAcquisition[SpecT, ResourceT] | ResourceLease[Hashable, ResourceT] | None: ...

    def acquire(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
    ) -> ResourceSet[ResourceT]: ...

    def commit(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
        resources: ResourceSet[ResourceT],
    ) -> ResourceLease[Hashable, ResourceT]: ...

    def cancel(self, acquisition: ResourceAcquisition[SpecT, ResourceT]) -> bool: ...

    def interrupt(self, acquisition: ResourceAcquisition[SpecT, ResourceT]) -> None: ...

    def abandon(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
        resources: ResourceSet[ResourceT] | None = None,
    ) -> None: ...

    def release(self, lease: ResourceLease[Hashable, ResourceT]) -> None: ...

    def cleanup_retired(self, plan: ResourcePlan[SpecT]) -> bool: ...


class _PoolAcquisitionContext:
    """Driver-facing view backed by one Pool ResourceRequest."""

    def __init__(
        self,
        pool: ResourcePool[Hashable, ResourceT],
        request: ResourceRequest[Hashable],
    ) -> None:
        self._pool = pool
        self._request = request

    @property
    def interrupted(self) -> bool:
        return self._pool.is_interrupted(self._request)


class ResourceManager(Generic[SpecT, ResourceT]):
    """Own conflict/reuse policy execution and physical-resource bookkeeping.

    The manager is the only layer that coordinates ResourcePool request identity,
    leases, interruption, retirement, cleanup ownership, and ResourceDriver I/O.
    An acquisition remains ``processing`` through upper-layer capability
    projection; ``commit`` is the explicit hand-off that finishes the request and
    installs its lease.
    """

    def __init__(
        self,
        driver: ResourceDriver[SpecT, ResourceT],
        *,
        resource_pool: ResourcePool[Hashable, ResourceT] = GLOBAL_RESOURCE_POOL,
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
    def resource_pool(self) -> ResourcePool[Hashable, ResourceT]:
        return self._resource_pool

    def claim(
        self,
        plan: ResourcePlan[SpecT],
    ) -> ResourceAcquisition[SpecT, ResourceT] | ResourceLease[Hashable, ResourceT] | None:
        if not isinstance(plan, ResourcePlan):
            raise TypeError("plan must be ResourcePlan")

        claim = self._resource_pool.reserve(plan.scope, plan.requirement)
        if claim is None or isinstance(claim, ResourceLease):
            return claim
        return ResourceAcquisition(plan, claim, self._token)

    def acquire(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
    ) -> ResourceSet[ResourceT]:
        self._validate_acquisition(acquisition)
        context = self._context(acquisition)
        snapshots = self._driver.acquire(acquisition._plan.spec, context)
        if not isinstance(snapshots, Iterator):
            raise TypeError("ResourceDriver.acquire() must return an Iterator")

        resources: ResourceSet[ResourceT] | None = None
        for snapshot in snapshots:
            if snapshot is None:
                raise TypeError("ResourceDriver.acquire() cannot yield None")
            if not isinstance(snapshot, tuple):
                raise TypeError("ResourceDriver.acquire() must yield ResourceSet tuples")
            resources = snapshot
            self._resource_pool.publish(acquisition._request, snapshot)

        if resources is None:
            raise RuntimeError(
                "ResourceDriver.acquire() must yield at least one ResourceSet"
            )

        # Keep processing=true until upper-layer capability projection has
        # completed and commit authority is rechecked. The final yielded snapshot
        # is returned to the upper layer while the request remains processing.
        return resources

    def commit(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
        resources: ResourceSet[ResourceT],
    ) -> ResourceLease[Hashable, ResourceT]:
        self._validate_acquisition(acquisition)
        if resources is None:
            raise TypeError("resources cannot be None")

        retired = self._resource_pool.finish(acquisition._request, resources)
        if retired is not None:
            self._cleanup_retired(retired)
            raise RuntimeError("interrupted resource acquisition cannot be committed")
        return self._resource_pool.install(acquisition._request)

    def cancel(self, acquisition: ResourceAcquisition[SpecT, ResourceT]) -> bool:
        """Cancel a claimed acquisition before its physical producer starts."""

        self._validate_acquisition(acquisition)
        return self._resource_pool.cancel(acquisition._request)

    def interrupt(self, acquisition: ResourceAcquisition[SpecT, ResourceT]) -> None:
        """Request best-effort physical interruption without ending processing."""

        self._validate_acquisition(acquisition)
        interruption = self._resource_pool.interrupt(acquisition._request)
        if interruption.processing:
            self._driver.interrupt(acquisition._plan.spec, self._context(acquisition))
        elif interruption.retired is not None:
            self._cleanup_retired(interruption.retired)

    def abandon(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
        resources: ResourceSet[ResourceT] | None = None,
    ) -> None:
        """Finish and clean a producer after upper-layer ownership was lost.

        This does not call ``driver.interrupt``: by the time the upper layer uses
        this operation, the acquire call has already returned or raised. A prior
        concurrent ``interrupt`` may already have asked the backend to abort.
        """

        self._validate_acquisition(acquisition)
        interruption = self._resource_pool.interrupt(acquisition._request)
        if interruption.retired is not None:
            self._cleanup_retired(interruption.retired)
            return
        if not interruption.processing:
            return

        retired = self._resource_pool.finish(acquisition._request, resources)
        if retired is not None:
            self._cleanup_retired(retired)

    def release(self, lease: ResourceLease[Hashable, ResourceT]) -> None:
        retired = self._resource_pool.retire(lease)
        if retired is not None:
            self._cleanup_retired(retired)

    def cleanup_retired(self, plan: ResourcePlan[SpecT]) -> bool:
        if not isinstance(plan, ResourcePlan):
            raise TypeError("plan must be ResourcePlan")
        retired_records = self._resource_pool.retired(plan.scope, plan.requirement.key)
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

    def _context(
        self,
        acquisition: ResourceAcquisition[SpecT, ResourceT],
    ) -> AcquisitionContext:
        return _PoolAcquisitionContext(self._resource_pool, acquisition._request)

    def _cleanup_retired(
        self,
        retired: RetiredResource[Hashable, ResourceT],
    ) -> None:
        self._driver.cleanup(retired.resources)
        self._resource_pool.discard(retired)


__all__ = ["ResourceAcquisition", "ResourceManagement", "ResourceManager"]
