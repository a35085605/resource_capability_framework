from __future__ import annotations

from dataclasses import dataclass

from lifecycle.resource import ReleaseReport


@dataclass(frozen=True, slots=True)
class LifecycleDiagnostics:
    """Operation failure and resource-layer finalization diagnostics."""

    acquire_error: BaseException | None = None
    release_report: ReleaseReport | None = None

    def __post_init__(self) -> None:
        if self.acquire_error is not None and not isinstance(
            self.acquire_error, BaseException
        ):
            raise TypeError("acquire_error must be a BaseException or None")
        if self.release_report is not None and not isinstance(
            self.release_report, ReleaseReport
        ):
            raise TypeError("release_report must be a ReleaseReport or None")


__all__ = ["LifecycleDiagnostics"]
