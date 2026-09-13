from dataclasses import dataclass, field
from typing import Protocol, TypeAlias, runtime_checkable

from _managed.result import AcquireResult, ReleaseResult
from _managed.snapshot import ManagedPhase, Snapshot
from api.epoch import Epoch, EpochSequence
from api.networking_address import TcpAddress


class _AdbServerGenerationEpoch(Epoch):
    __slots__ = ()


@dataclass(frozen=True, slots=True)
class AdbServerGeneration:
    _epoch: _AdbServerGenerationEpoch = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self._epoch, _AdbServerGenerationEpoch):
            raise TypeError("_epoch must be _AdbServerGenerationEpoch")

    def __str__(self) -> str:
        return str(self._epoch)


class AdbServerGenerationIssuer:
    __slots__ = ("_sequence",)

    def __init__(self, *, after: AdbServerGeneration | None = None) -> None:
        if after is not None and not isinstance(after, AdbServerGeneration):
            raise TypeError("after must be AdbServerGeneration or None")
        initial_value = 0 if after is None else after._epoch.value
        self._sequence = EpochSequence(_AdbServerGenerationEpoch, initial_value=initial_value)

    def issue(self) -> AdbServerGeneration:
        """Issue a fresh server generation."""

        return AdbServerGeneration(self._sequence.issue())


@dataclass(frozen=True, slots=True)
class AdbServerRequest:

    server_address: TcpAddress


@dataclass(frozen=True, slots=True)
class AdbServerCapability:

    server_address: TcpAddress


AdbServerPhase: TypeAlias = ManagedPhase


AdbServerState: TypeAlias = Snapshot[
    AdbServerGeneration,
    AdbServerRequest,
    AdbServerCapability,
]


AdbServerAcquireResult: TypeAlias = AcquireResult[
    AdbServerGeneration,
    AdbServerRequest,
    AdbServerCapability,
]


AdbServerReleaseResult: TypeAlias = ReleaseResult[
    AdbServerGeneration,
    AdbServerRequest,
    AdbServerCapability,
]


@runtime_checkable
class AdbServerStateView(Protocol):
    """Read a linearizable snapshot of current server authority and usable capability."""

    def read(self) -> AdbServerState:
        """Return one atomic generation/request/capability snapshot without leasing capability."""
        ...


class AdbServerLifecycle(AdbServerStateView, Protocol):
    def acquire(
        self,
        expected_generation: AdbServerGeneration,
        request: AdbServerRequest,
    ) -> AdbServerAcquireResult: ...

    def release(
        self,
        expected_generation: AdbServerGeneration,
        request: AdbServerRequest,
    ) -> AdbServerReleaseResult: ...


class AdbServerLifecycleFactory(Protocol):
    """Construct one runtime-scoped ADB server lifecycle."""

    def __call__(
        self,
        generation_issuer: AdbServerGenerationIssuer,
    ) -> AdbServerLifecycle: ...
