from dataclasses import dataclass, field
from typing import Protocol, TypeAlias, runtime_checkable

from _lifecycle.capability.result import AcquireResult, ReleaseResult
from _lifecycle.capability.snapshot import LifecyclePhase, LifecycleSnapshot
from api.epoch import Epoch, EpochSequence
from api.networking_address import TcpEndpoint


class _AdbServerGenerationEpoch(Epoch):
    __slots__ = ()


@dataclass(frozen=True, slots=True)
class AdbServerGeneration:
    """Identify one ADB server lifecycle generation."""

    _epoch: _AdbServerGenerationEpoch = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self._epoch, _AdbServerGenerationEpoch):
            raise TypeError("_epoch must be _AdbServerGenerationEpoch")

    def __str__(self) -> str:
        return str(self._epoch)


class AdbServerGenerationIssuer:
    """Issue monotonically increasing ADB server generations."""

    __slots__ = ("_sequence",)

    def __init__(self, *, after: AdbServerGeneration | None = None) -> None:
        if after is not None and not isinstance(after, AdbServerGeneration):
            raise TypeError("after must be AdbServerGeneration or None")
        initial_value = 0 if after is None else after._epoch.value
        self._sequence = EpochSequence(_AdbServerGenerationEpoch, initial_value=initial_value)

    def issue(self) -> AdbServerGeneration:
        """Issue a generation newer than every generation previously issued here."""

        return AdbServerGeneration(self._sequence.issue())


@dataclass(frozen=True, slots=True)
class AdbServerRequest:
    """Request an ADB server at a specific TCP address."""

    server_address: TcpEndpoint


@dataclass(frozen=True, slots=True)
class AdbServerCapability:
    """Describe the TCP address provided by an active ADB server lifecycle."""

    server_address: TcpEndpoint


AdbServerPhase: TypeAlias = LifecyclePhase


AdbServerSnapshot: TypeAlias = LifecycleSnapshot[
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
class AdbServerSnapshotReader(Protocol):
    """Read a consistent point-in-time ADB server lifecycle snapshot."""

    def read(self) -> AdbServerSnapshot:
        """Return the current generation, phase, and phase-specific fields."""
        ...


class AdbServerLifecycle(AdbServerSnapshotReader, Protocol):
    """Acquire and release one generation-scoped ADB server capability."""

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
    """Build an ADB server lifecycle using the supplied generation issuer."""

    def __call__(
        self,
        generation_issuer: AdbServerGenerationIssuer,
    ) -> AdbServerLifecycle: ...
