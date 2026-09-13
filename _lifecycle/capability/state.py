from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar

from _lifecycle.resource.driver import PhysicalResources


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
PhysicalResourceT = TypeVar("PhysicalResourceT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(frozen=True, slots=True)
class Idle(Generic[GenerationT]):
    """Internal state for a generation with no request in progress."""

    generation: GenerationT


@dataclass(frozen=True, slots=True)
class Acquiring(Generic[GenerationT, RequestT]):
    """Internal state while acquiring resources and projecting a capability."""

    generation: GenerationT
    request: RequestT


@dataclass(frozen=True, slots=True)
class Active(Generic[GenerationT, RequestT, PhysicalResourceT, CapabilityT]):
    """Internal state for the active request, capability, and retained resources."""

    generation: GenerationT
    request: RequestT
    capability: CapabilityT
    resources: PhysicalResources[PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class Releasing(Generic[GenerationT, RequestT, PhysicalResourceT]):
    """Internal state while releasing resources or issuing the next generation."""

    generation: GenerationT
    request: RequestT
    resources: PhysicalResources[PhysicalResourceT]


@dataclass(frozen=True, slots=True)
class ReleaseRequired(Generic[GenerationT, RequestT, PhysicalResourceT]):
    """Internal state for a generation that still requires successful release.

    ``resources`` may already be empty when physical cleanup succeeded but issuing the
    next generation failed. Retrying release must therefore not assume cleanup remains.
    """

    generation: GenerationT
    request: RequestT
    resources: PhysicalResources[PhysicalResourceT]
    last_error: BaseException


LifecycleState: TypeAlias = (
    Idle[GenerationT]
    | Acquiring[GenerationT, RequestT]
    | Active[GenerationT, RequestT, PhysicalResourceT, CapabilityT]
    | Releasing[GenerationT, RequestT, PhysicalResourceT]
    | ReleaseRequired[GenerationT, RequestT, PhysicalResourceT]
)


__all__ = [
    "Acquiring",
    "Active",
    "Idle",
    "LifecycleState",
    "ReleaseRequired",
    "Releasing",
]
