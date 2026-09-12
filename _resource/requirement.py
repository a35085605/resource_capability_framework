from __future__ import annotations

from typing import Protocol, runtime_checkable

from _resource.policy import ResourcePolicy


@runtime_checkable
class ResourceRequirement(Protocol):
    """Implementation-specific descriptor for one logical resource requirement.

    Concrete resource domains carry their acquisition inputs directly on the
    requirement value. ``policy`` remains the only framework-defined field.
    """

    @property
    def policy(self) -> ResourcePolicy: ...


type ResourceRequirements[T: ResourceRequirement] = tuple[T, ...]


__all__ = ["ResourceRequirement", "ResourceRequirements"]
