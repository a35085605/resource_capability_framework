from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar


SpecT = TypeVar("SpecT", contravariant=True)
PhysicalResourceT = TypeVar("PhysicalResourceT")


type PhysicalResourceSet[T] = tuple[T, ...]


@dataclass(frozen=True, slots=True)
class PhysicalAcquired(Generic[PhysicalResourceT]):
    """Physical acquisition completed normally with its final PhysicalResourceSet."""

    resources: PhysicalResourceSet[PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class PhysicalInterrupted(Generic[PhysicalResourceT]):
    """Physical acquisition stopped after interruption.

    ``resources`` contains every PhysicalResource created before the operation reached its
    terminal interrupted state.  ResourceManager owns cleanup of those PhysicalResources.
    """

    resources: PhysicalResourceSet[PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class PhysicalFailed(Generic[PhysicalResourceT]):
    """Physical acquisition reached a terminal operational failure.

    ``resources`` contains every PhysicalResource created before failure so ResourceManager
    can retire and clean them up together with PhysicalResources acquired by earlier parts.
    """

    error: Exception
    resources: PhysicalResourceSet[PhysicalResourceT]


type PhysicalAcquireOutcome[T] = (
    PhysicalAcquired[T] | PhysicalInterrupted[T] | PhysicalFailed[T]
)


class PhysicalAcquisition(Protocol[PhysicalResourceT]):
    """Driver-owned identity and lifecycle for one physical acquisition.

    ``acquire`` performs the physical operation and returns exactly once with a
    terminal typed outcome.  It never publishes intermediate PhysicalResource snapshots.

    ``interrupt`` requests termination of an in-progress operation and must not
    transfer Resource ownership or perform ResourceManager cleanup.  The thread
    already executing ``acquire`` remains blocked until the physical operation has
    actually reached a terminal state, then returns a ``PhysicalInterrupted`` (or
    another terminal outcome) containing every Resource produced by the operation.
    ``interrupt`` itself may return before ``acquire`` does.

    Operational failures must be represented as ``PhysicalFailed``.  Exceptions
    escaping ``acquire`` are treated as driver contract/invariant failures.
    """

    def acquire(self) -> PhysicalAcquireOutcome[PhysicalResourceT]: ...

    def interrupt(self) -> None: ...


class ResourceDriver(Protocol[SpecT, PhysicalResourceT]):
    """Physical resource I/O driven only by a lower-layer resource specification.

    Drivers do not know about managed Request, generations, authority, conflict
    policy, pool request identity, leases, or capability projection. ``prepare``
    creates a dedicated physical-operation handle without starting PhysicalResource
    production; calling its ``acquire`` method performs the I/O and returns one final
    typed outcome. ResourceManager remains solely responsible for Pool publication,
    ownership bookkeeping, retirement, and cleanup.
    """

    def prepare(self, spec: SpecT) -> PhysicalAcquisition[PhysicalResourceT]: ...

    def cleanup(self, resources: PhysicalResourceSet[PhysicalResourceT]) -> None: ...


__all__ = [
    "PhysicalAcquireOutcome",
    "PhysicalAcquired",
    "PhysicalAcquisition",
    "PhysicalFailed",
    "PhysicalInterrupted",
    "ResourceDriver",
    "PhysicalResourceSet",
]
