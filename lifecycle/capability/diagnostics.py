from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LifecycleDiagnostics:
    """Preserve operation failure separately from cleanup/finalization diagnostics."""

    acquire_error: BaseException | None = None
    cleanup_errors: tuple[BaseException, ...] = ()
    finalization_errors: tuple[BaseException, ...] = ()

    def __post_init__(self) -> None:
        if self.acquire_error is not None and not isinstance(
            self.acquire_error, BaseException
        ):
            raise TypeError("acquire_error must be a BaseException or None")
        if not isinstance(self.cleanup_errors, tuple) or not all(
            isinstance(error, BaseException) for error in self.cleanup_errors
        ):
            raise TypeError("cleanup_errors must be a tuple of BaseException values")
        if not isinstance(self.finalization_errors, tuple) or not all(
            isinstance(error, BaseException) for error in self.finalization_errors
        ):
            raise TypeError("finalization_errors must be a tuple of BaseException values")

    def add_cleanup(self, *errors: BaseException) -> "LifecycleDiagnostics":
        return LifecycleDiagnostics(
            self.acquire_error,
            self.cleanup_errors + tuple(errors),
            self.finalization_errors,
        )

    def add_finalization(self, error: BaseException) -> "LifecycleDiagnostics":
        return LifecycleDiagnostics(
            self.acquire_error,
            self.cleanup_errors,
            self.finalization_errors + (error,),
        )


__all__ = ["LifecycleDiagnostics"]
