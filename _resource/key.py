from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Hashable, Protocol, TypeVar

from _resource.requirement import ResourceRequirement


RequirementT = TypeVar("RequirementT", bound=ResourceRequirement, contravariant=True)
IdentityT = TypeVar("IdentityT", bound=Hashable)


@dataclass(frozen=True, slots=True)
class ResourceKey(Generic[IdentityT]):
    """Canonical logical identity for one resource sharing/conflict boundary.

    ``namespace`` separates unrelated physical-resource domains in the process-wide
    pool. ``identity`` contains only stable values that determine whether two
    requirements refer to the same logical resource demand.

    Physical identities discovered during acquisition (for example a socket fd or a
    DNS result) do not belong here unless they are already part of the logical
    sharing boundary before physical I/O starts.
    """

    namespace: Hashable
    identity: IdentityT

    def __post_init__(self) -> None:
        if self.namespace is None:
            raise TypeError("resource key namespace cannot be None")
        if self.identity is None:
            raise TypeError("resource key identity cannot be None")
        try:
            hash(self.namespace)
            hash(self.identity)
        except TypeError as exc:
            raise TypeError(
                "resource key namespace and identity must be hashable"
            ) from exc


class ResourceKeyModel(Protocol[RequirementT]):
    """Canonicalize a resource requirement before any physical I/O starts."""

    def key_for(self, requirement: RequirementT) -> ResourceKey: ...


__all__ = ["ResourceKey", "ResourceKeyModel"]
