from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar


SpecT = TypeVar("SpecT", contravariant=True)
ResourceT = TypeVar("ResourceT")


type ResourceSet[T] = tuple[T, ...]


@dataclass(frozen=True, slots=True)
class PhysicalAcquired(Generic[ResourceT]):
    """Physical acquisition completed normally with its final ResourceSet."""

    resources: ResourceSet[ResourceT]


@dataclass(frozen=True, slots=True)
class PhysicalInterrupted(Generic[ResourceT]):
    """Physical acquisition stopped after interruption.

    ``resources`` contains every Resource created before the operation reached its
    terminal interrupted state.  ResourceManager owns cleanup of those Resources.
    """

    resources: ResourceSet[ResourceT]


@dataclass(frozen=True, slots=True)
class PhysicalFailed(Generic[ResourceT]):
    """Physical acquisition reached a terminal operational failure.

    ``resources`` contains every Resource created before failure so ResourceManager
    can retire and clean them up together with Resources acquired by earlier parts.
    """

    error: Exception
    resources: ResourceSet[ResourceT]


type PhysicalAcquireOutcome[T] = (
    PhysicalAcquired[T] | PhysicalInterrupted[T] | PhysicalFailed[T]
)


class PhysicalAcquisition(Protocol[ResourceT]):
    """Driver-owned identity and lifecycle for one physical acquisition.

    ``acquire`` performs the physical operation and returns exactly once with a
    terminal typed outcome.  It never publishes intermediate Resource snapshots.

    ``interrupt`` requests termination of an in-progress operation and must not
    transfer Resource ownership or perform ResourceManager cleanup.  The thread
    already executing ``acquire`` remains blocked until the physical operation has
    actually reached a terminal state, then returns a ``PhysicalInterrupted`` (or
    another terminal outcome) containing every Resource produced by the operation.
    ``interrupt`` itself may return before ``acquire`` does.

    Operational failures must be represented as ``PhysicalFailed``.  Exceptions
    escaping ``acquire`` are treated as driver contract/invariant failures.
    """

    def acquire(self) -> PhysicalAcquireOutcome[ResourceT]: ...

    def interrupt(self) -> None: ...


class ResourceDriver(Protocol[SpecT, ResourceT]):
    """Physical resource I/O driven only by a lower-layer resource specification.

    Drivers do not know about managed Access, generations, authority, conflict
    policy, pool request identity, leases, or capability projection. ``prepare``
    creates a dedicated physical-operation handle without starting Resource
    production; calling its ``acquire`` method performs the I/O and returns one final
    typed outcome. ResourceManager remains solely responsible for Pool publication,
    ownership bookkeeping, retirement, and cleanup.
    """

    def prepare(self, spec: SpecT) -> PhysicalAcquisition[ResourceT]: ...

    def cleanup(self, resources: ResourceSet[ResourceT]) -> None: ...


__all__ = [
    "PhysicalAcquireOutcome",
    "PhysicalAcquired",
    "PhysicalAcquisition",
    "PhysicalFailed",
    "PhysicalInterrupted",
    "ResourceDriver",
    "ResourceSet",
]
