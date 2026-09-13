from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

from _resource.requirement import ResourceRequirement


RequirementT = TypeVar("RequirementT", bound=ResourceRequirement, contravariant=True)
PhysicalResourceT = TypeVar("PhysicalResourceT")


type PhysicalResourceSet[T] = tuple[T, ...]


@dataclass(frozen=True, slots=True)
class PhysicalAcquired(Generic[PhysicalResourceT]):
    """Physical acquisition completed normally with its final PhysicalResourceSet."""

    resources: PhysicalResourceSet[PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class PhysicalFailed(Generic[PhysicalResourceT]):
    """Physical acquisition reached a terminal operational failure.

    ``resources`` contains every PhysicalResource created by this requirement before
    failure. ResourceManager combines them with resources from earlier requirements.
    """

    error: Exception
    resources: PhysicalResourceSet[PhysicalResourceT]


type PhysicalAcquireOutcome[T] = PhysicalAcquired[T] | PhysicalFailed[T]


class ResourceDriver(Protocol[RequirementT, PhysicalResourceT]):
    """Synchronous physical resource I/O driven only by a resource requirement.

    Drivers do not know about Managed requests, generations, lifecycle state, leases,
    or capability projection. ``acquire`` performs one requirement's physical I/O and
    returns one terminal outcome. ResourceManager only aggregates those outcomes.

    ``cleanup`` must be safe to retry with the same ``resources`` after it raises. A
    cleanup implementation may therefore be called again after partially completing a
    previous cleanup attempt, and must tolerate already-cleaned members.
    """

    def acquire(
        self,
        requirement: RequirementT,
    ) -> PhysicalAcquireOutcome[PhysicalResourceT]: ...

    def cleanup(self, resources: PhysicalResourceSet[PhysicalResourceT]) -> None: ...


__all__ = [
    "PhysicalAcquireOutcome",
    "PhysicalAcquired",
    "PhysicalFailed",
    "ResourceDriver",
    "PhysicalResourceSet",
]
