from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol, TypeVar


SpecT = TypeVar("SpecT", contravariant=True)
ResourceT = TypeVar("ResourceT")


type ResourceSet[T] = tuple[T, ...]


class PhysicalAcquisition(Protocol[ResourceT]):
    """Driver-owned identity and lifecycle for one physical acquisition.

    The acquisition owns any backend-specific operation state needed to stop this
    exact producer (for example a process, socket, device operation, or cancellation
    event). ``interrupt`` is a best-effort physical stop request: an interrupted
    producer may still yield late immutable ResourceSet snapshots until its acquire
    stream ends or raises.
    """

    def acquire(self) -> Iterator[ResourceSet[ResourceT]]: ...

    def interrupt(self) -> None: ...


class ResourceDriver(Protocol[SpecT, ResourceT]):
    """Physical resource I/O driven only by a lower-layer resource specification.

    Drivers do not know about managed Access, generations, authority, conflict
    policy, pool request identity, leases, or capability projection. ``prepare``
    creates a dedicated physical-operation handle without starting resource
    production; iterating its ``acquire`` stream performs the I/O. Resource
    management receives every immutable ResourceSet snapshot and remains solely
    responsible for Pool publication and ownership bookkeeping.
    """

    def prepare(self, spec: SpecT) -> PhysicalAcquisition[ResourceT]: ...

    def cleanup(self, resources: ResourceSet[ResourceT]) -> None: ...


__all__ = ["PhysicalAcquisition", "ResourceDriver", "ResourceSet"]
