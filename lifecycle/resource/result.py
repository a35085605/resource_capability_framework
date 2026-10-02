from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ReleaseReport:
    """Summary of one completed resource-scope finalization round.

    A completed release means that every cleanup responsibility owned by the scope was
    either finalized successfully or transferred to the recovery pool. It does not
    guarantee that every underlying physical resource disappeared successfully.
    """

    errors: tuple[BaseException, ...] = ()
    pooled_count: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.errors, tuple) or not all(
            isinstance(error, BaseException) for error in self.errors
        ):
            raise TypeError("errors must be a tuple of BaseException values")
        if not isinstance(self.pooled_count, int) or self.pooled_count < 0:
            raise TypeError("pooled_count must be a non-negative int")

    def merged(self, *others: "ReleaseReport") -> "ReleaseReport":
        errors = list(self.errors)
        pooled_count = self.pooled_count
        for other in others:
            if not isinstance(other, ReleaseReport):
                raise TypeError("merged reports must be ReleaseReport values")
            errors.extend(other.errors)
            pooled_count += other.pooled_count
        return ReleaseReport(tuple(errors), pooled_count)


class ScopeClosedError(RuntimeError):
    """An operation requires a scope whose entire ancestor chain is still open."""


class ResourceAllocationError(RuntimeError):
    """An allocation failed after the resource layer completed controlled rollback."""

    __slots__ = ("cause", "release_report")

    def __init__(self, cause: Exception, release_report: ReleaseReport) -> None:
        if not isinstance(cause, Exception):
            raise TypeError("cause must be an Exception")
        if not isinstance(release_report, ReleaseReport):
            raise TypeError("release_report must be a ReleaseReport")
        super().__init__(f"resource allocation failed: {cause}")
        self.cause = cause
        self.release_report = release_report


__all__ = ["ReleaseReport", "ResourceAllocationError", "ScopeClosedError"]
