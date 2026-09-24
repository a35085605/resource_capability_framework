from __future__ import annotations

from typing import Protocol, TypeVar, runtime_checkable

from lifecycle.capability.result import AcquireResult, RecoveryResult, ReleaseResult
from lifecycle.capability.snapshot import LifecycleSnapshot


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


@runtime_checkable
class LifecycleSnapshotReader(Protocol[GenerationT, RequestT, CapabilityT]):
    def read(self) -> LifecycleSnapshot[GenerationT, RequestT, CapabilityT]: ...


@runtime_checkable
class CapabilityLifecycle(
    LifecycleSnapshotReader[GenerationT, RequestT, CapabilityT],
    Protocol[GenerationT, RequestT, CapabilityT],
):
    """Read, acquire, release, and recover one generation-scoped capability lifecycle."""

    def acquire(
        self,
        expected_generation: GenerationT,
        request: RequestT,
    ) -> AcquireResult[GenerationT, RequestT, CapabilityT]: ...

    def release(
        self,
        expected_generation: GenerationT,
        request: RequestT,
    ) -> ReleaseResult[GenerationT, RequestT, CapabilityT]: ...

    def recover(
        self,
        expected_generation: GenerationT,
        request: RequestT,
    ) -> RecoveryResult[GenerationT, RequestT, CapabilityT]: ...


__all__ = ["CapabilityLifecycle", "LifecycleSnapshotReader"]
