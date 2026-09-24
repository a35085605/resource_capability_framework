"""Generation-scoped acquire, release and recovery supervision."""

from lifecycle.capability.supervision.acquire import (
    AcquireDisposition,
    AcquireSupervisionResult,
    AcquireSupervisor,
    classify_acquire_result,
)
from lifecycle.capability.supervision.control import (
    CancellationSignal,
    SupervisionStopped,
    SupervisionStopReason,
)
from lifecycle.capability.supervision.policy import (
    AcquireSupervisionPolicy,
    RecoverySupervisionPolicy,
    ReleaseSupervisionPolicy,
)
from lifecycle.capability.supervision.recovery import (
    RecoveryDisposition,
    RecoverySupervisionResult,
    RecoverySupervisor,
    classify_recovery_result,
)
from lifecycle.capability.supervision.release import (
    ReleaseDisposition,
    ReleaseSupervisionResult,
    ReleaseSupervisor,
    classify_release_result,
)

__all__ = [
    "AcquireDisposition",
    "AcquireSupervisionPolicy",
    "AcquireSupervisionResult",
    "AcquireSupervisor",
    "CancellationSignal",
    "RecoveryDisposition",
    "RecoverySupervisionPolicy",
    "RecoverySupervisionResult",
    "RecoverySupervisor",
    "ReleaseDisposition",
    "ReleaseSupervisionPolicy",
    "ReleaseSupervisionResult",
    "ReleaseSupervisor",
    "SupervisionStopped",
    "SupervisionStopReason",
    "classify_acquire_result",
    "classify_recovery_result",
    "classify_release_result",
]
