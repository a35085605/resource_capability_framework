from __future__ import annotations

from typing import Protocol


class ResourceRequirement(Protocol):
    """Implementation-specific descriptor for one physical resource requirement.

    Concrete resource domains carry every acquisition input directly on the requirement
    value. The framework imposes no policy or identity fields on requirements.
    """


type ResourceRequirements[T: ResourceRequirement] = tuple[T, ...]


__all__ = ["ResourceRequirement", "ResourceRequirements"]
