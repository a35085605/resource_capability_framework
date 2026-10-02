from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, TypeVar, runtime_checkable

from lifecycle.capability.result import AcquireResult, ReleaseResult
from lifecycle.capability.snapshot import LifecycleSnapshot
from lifecycle.resource import Resource


RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")
ChildRequestT = TypeVar("ChildRequestT")
ChildCapabilityT = TypeVar("ChildCapabilityT")


@runtime_checkable
class LifecycleSnapshotReader(Protocol[RequestT, CapabilityT]):
    def read(self) -> LifecycleSnapshot[RequestT, CapabilityT]: ...


@runtime_checkable
class CapabilityLifecycle(
    LifecycleSnapshotReader[RequestT, CapabilityT],
    Protocol[RequestT, CapabilityT],
):
    """Read, acquire, release, and derive child capability lifecycles."""

    def acquire(
        self,
        expected_generation: int,
        request: RequestT,
    ) -> AcquireResult[RequestT, CapabilityT]: ...

    def release(
        self,
        expected_generation: int,
        request: RequestT,
    ) -> ReleaseResult[RequestT, CapabilityT]: ...

    def create_child(
        self,
        expected_generation: int,
        factory: Callable[[CapabilityT, ChildRequestT], Resource[ChildCapabilityT]],
    ) -> "CapabilityLifecycle[ChildRequestT, ChildCapabilityT]": ...


__all__ = ["CapabilityLifecycle", "LifecycleSnapshotReader"]
