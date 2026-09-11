from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event, Lock
from typing import Generic, Hashable, TypeAlias, TypeVar

from adb._resource.manager import ResourceAcquisition
from adb._resource.pool import ResourceLease


GenerationT = TypeVar("GenerationT")
AccessT = TypeVar("AccessT")
SpecT = TypeVar("SpecT")
ResourceT = TypeVar("ResourceT")
CapabilityT = TypeVar("CapabilityT")


@dataclass(slots=True, eq=False)
class ManagedAttempt(Generic[GenerationT, AccessT]):
    """Coordinator-owned state for one in-flight Access authority attempt.

    ``revoke`` removes capability commit authority immediately. Physical resource
    interruption is deliberately separate and is delegated through the opaque
    acquisition handle held by ``Preparing``.
    """

    generation: GenerationT
    access: AccessT
    _cancellation: Event = field(default_factory=Event, init=False, repr=False)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _revoked: bool = field(default=False, init=False, repr=False)
    _finished: bool = field(default=False, init=False, repr=False)

    @property
    def cancellation(self) -> Event:
        return self._cancellation

    @property
    def revoked(self) -> bool:
        with self._lock:
            return self._revoked

    @property
    def finished(self) -> bool:
        with self._lock:
            return self._finished

    def revoke(self) -> None:
        with self._lock:
            if self._revoked:
                return
            self._revoked = True
            self._cancellation.set()

    def finish(self) -> None:
        with self._lock:
            self._finished = True


@dataclass(frozen=True, slots=True)
class Idle(Generic[GenerationT]):
    generation: GenerationT


@dataclass(frozen=True, slots=True)
class Preparing(Generic[GenerationT, AccessT, SpecT, ResourceT]):
    generation: GenerationT
    access: AccessT
    attempt: ManagedAttempt[GenerationT, AccessT]
    acquisition: ResourceAcquisition[SpecT, ResourceT] | None = None


@dataclass(frozen=True, slots=True)
class Current(Generic[GenerationT, AccessT, ResourceT, CapabilityT]):
    generation: GenerationT
    access: AccessT
    capability: CapabilityT
    resource_lease: ResourceLease[Hashable, ResourceT]


ManagedState: TypeAlias = (
    Idle[GenerationT]
    | Preparing[GenerationT, AccessT, SpecT, ResourceT]
    | Current[GenerationT, AccessT, ResourceT, CapabilityT]
)


__all__ = ["Current", "Idle", "ManagedAttempt", "ManagedState", "Preparing"]
