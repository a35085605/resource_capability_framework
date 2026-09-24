from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Generic, TypeVar

from lifecycle.capability.result import (
    AcquireResult,
    LifecycleDiagnostics,
    LifecycleOutcome,
    LifecycleResult,
    RecoveryResult,
    ReleaseResult,
)
from lifecycle.capability.session import (
    CapabilitySessionFactory,
    CleanupReport,
    PreparationFailed,
    PreparedSession,
    SessionOwner,
)
from lifecycle.capability.snapshot import CleanupOrigin, LifecyclePhase, LifecycleSnapshot
from lifecycle.capability.state import (
    Acquiring,
    Active,
    CleanupPending,
    FinalizationPending,
    Idle,
    LifecycleState,
    Recovering,
    Releasing,
)
from lifecycle.resource.result import ResourceCleanupStatus


GenerationT = TypeVar("GenerationT")
RequestT = TypeVar("RequestT")
CapabilityT = TypeVar("CapabilityT")


def _snapshot_from_state(
    state: LifecycleState[GenerationT, RequestT, CapabilityT],
) -> LifecycleSnapshot[GenerationT, RequestT, CapabilityT]:
    if isinstance(state, Idle):
        return LifecycleSnapshot(state.generation)
    if isinstance(state, Active):
        return LifecycleSnapshot(
            state.generation,
            state.request,
            state.capability,
            phase=LifecyclePhase.ACTIVE,
        )
    if isinstance(state, Acquiring):
        return LifecycleSnapshot(
            state.generation,
            state.request,
            phase=LifecyclePhase.ACQUIRING,
        )
    if isinstance(state, Releasing):
        return LifecycleSnapshot(
            state.generation,
            state.request,
            phase=LifecyclePhase.RELEASING,
        )
    if isinstance(state, CleanupPending):
        return LifecycleSnapshot(
            state.generation,
            state.request,
            phase=LifecyclePhase.CLEANUP_PENDING,
            origin=state.origin,
            cleanup_status=state.cleanup_status,
            diagnostics=state.diagnostics,
        )
    if isinstance(state, FinalizationPending):
        return LifecycleSnapshot(
            state.generation,
            state.request,
            phase=LifecyclePhase.FINALIZATION_PENDING,
            origin=state.origin,
            diagnostics=state.diagnostics,
        )
    if isinstance(state, Recovering):
        return LifecycleSnapshot(
            state.generation,
            state.request,
            phase=LifecyclePhase.RECOVERING,
            origin=state.origin,
            cleanup_status=state.cleanup_status,
            diagnostics=state.diagnostics,
        )
    raise RuntimeError("unsupported lifecycle state")


class _UnknownSessionOwner:
    """Conservatively block recovery after a custom factory violates its contract."""

    __slots__ = ("_error",)

    def __init__(self, error: BaseException) -> None:
        self._error = error

    @property
    def has_ownership(self) -> bool:
        return True

    def cleanup(self) -> CleanupReport:
        return CleanupReport(ResourceCleanupStatus.BLOCKED, (self._error,))


class CapabilityLifecycleCoordinator(Generic[GenerationT, RequestT, CapabilityT]):
    """Coordinate capability publication, session ownership, cleanup and fencing.

    Generation identifies one lifecycle fencing round. A capability is published only
    after session preparation succeeds. Failed preparation is rolled back once inside
    ``acquire``; only unresolved cleanup or generation finalization remains pending.
    Physical ownership never escapes the SessionOwner.
    """

    def __init__(
        self,
        issue_generation: Callable[[], GenerationT],
        session_factory: CapabilitySessionFactory[RequestT, CapabilityT],
    ) -> None:
        if not callable(issue_generation):
            raise TypeError("issue_generation must be callable")
        if not callable(getattr(session_factory, "prepare", None)):
            raise TypeError("session_factory must provide prepare()")
        self._issue_generation = issue_generation
        self._session_factory = session_factory
        self._lock = Lock()
        generation = issue_generation()
        if generation is None:
            raise TypeError("issue_generation cannot return None")
        self._state: LifecycleState[GenerationT, RequestT, CapabilityT] = Idle(generation)

    def read(self) -> LifecycleSnapshot[GenerationT, RequestT, CapabilityT]:
        with self._lock:
            return _snapshot_from_state(self._state)

    def acquire(
        self,
        expected_generation: GenerationT,
        request: RequestT,
    ) -> AcquireResult[GenerationT, RequestT, CapabilityT]:
        if expected_generation is None:
            raise TypeError("expected_generation cannot be None")
        if request is None:
            raise TypeError("request cannot be None")

        with self._lock:
            state = self._state
            if expected_generation != state.generation:
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            if not isinstance(state, Idle):
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            generation = state.generation
            self._state = Acquiring(generation, request)

        try:
            prepared = self._session_factory.prepare(request)
        except BaseException as exc:
            owner: SessionOwner = _UnknownSessionOwner(exc)
            diagnostics = LifecycleDiagnostics(acquire_error=exc)
            self._commit_cleanup_pending(
                generation,
                request,
                owner,
                CleanupOrigin.ACQUIRE,
                ResourceCleanupStatus.BLOCKED,
                diagnostics,
            )
            if not isinstance(exc, Exception):
                raise
            return self._operation_result(
                LifecycleOutcome.ACQUIRE_FAILED,
                CleanupOrigin.ACQUIRE,
                diagnostics,
            )

        if isinstance(prepared, PreparedSession):
            with self._lock:
                state = Active(generation, request, prepared.capability, prepared.owner)
                self._state = state
                snapshot = _snapshot_from_state(state)
            return LifecycleResult(
                snapshot,
                LifecycleOutcome.ACQUIRE_SUCCEEDED,
                LifecycleDiagnostics(),
                CleanupOrigin.ACQUIRE,
            )

        if not isinstance(prepared, PreparationFailed):
            error = TypeError("CapabilitySessionFactory.prepare() returned an invalid result")
            owner = _UnknownSessionOwner(error)
            diagnostics = LifecycleDiagnostics(acquire_error=error)
            self._commit_cleanup_pending(
                generation,
                request,
                owner,
                CleanupOrigin.ACQUIRE,
                ResourceCleanupStatus.BLOCKED,
                diagnostics,
            )
            return self._operation_result(
                LifecycleOutcome.ACQUIRE_FAILED,
                CleanupOrigin.ACQUIRE,
                diagnostics,
            )

        diagnostics = LifecycleDiagnostics(
            acquire_error=prepared.error,
            cleanup_errors=prepared.cleanup_errors,
        )

        # Control-flow interruption is not normalized into an ordinary failed acquire.
        # Commit recoverable ownership first, then propagate it. The primary operation
        # error remains separate when cleanup itself was what got interrupted.
        interruption = prepared.interruption
        if interruption is None and not isinstance(prepared.error, Exception):
            interruption = prepared.error
        if interruption is not None:
            if prepared.owner.has_ownership:
                self._commit_cleanup_pending(
                    generation,
                    request,
                    prepared.owner,
                    CleanupOrigin.ACQUIRE,
                    ResourceCleanupStatus.RETRYABLE,
                    diagnostics,
                )
            else:
                self._commit_finalization_pending(
                    generation,
                    request,
                    CleanupOrigin.ACQUIRE,
                    diagnostics,
                )
            raise interruption

        return self._finish_failed_acquire(
            generation,
            request,
            prepared.owner,
            diagnostics,
        )

    def release(
        self,
        expected_generation: GenerationT,
        request: RequestT,
    ) -> ReleaseResult[GenerationT, RequestT, CapabilityT]:
        if expected_generation is None:
            raise TypeError("expected_generation cannot be None")
        if request is None:
            raise TypeError("request cannot be None")

        with self._lock:
            state = self._state
            if expected_generation != state.generation:
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            if not isinstance(state, Active):
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            if state.request != request:
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            generation = state.generation
            owner = state.owner
            self._state = Releasing(generation, request, owner)

        diagnostics = LifecycleDiagnostics()
        report = self._attempt_cleanup_or_interrupt(
            generation,
            request,
            owner,
            CleanupOrigin.RELEASE,
            diagnostics,
        )
        diagnostics = diagnostics.add_cleanup(*report.errors)

        if report.status is not ResourceCleanupStatus.COMPLETE:
            self._commit_cleanup_pending(
                generation,
                request,
                owner,
                CleanupOrigin.RELEASE,
                report.status,
                diagnostics,
            )
            return self._operation_result(
                LifecycleOutcome.RELEASE_INCOMPLETE,
                CleanupOrigin.RELEASE,
                diagnostics,
            )

        return self._finalize_operation(
            generation,
            request,
            CleanupOrigin.RELEASE,
            diagnostics,
            success_outcome=LifecycleOutcome.RELEASE_COMPLETED,
            incomplete_outcome=LifecycleOutcome.RELEASE_INCOMPLETE,
        )

    def recover(
        self,
        expected_generation: GenerationT,
        request: RequestT,
    ) -> RecoveryResult[GenerationT, RequestT, CapabilityT]:
        if expected_generation is None:
            raise TypeError("expected_generation cannot be None")
        if request is None:
            raise TypeError("request cannot be None")

        with self._lock:
            state = self._state
            if expected_generation != state.generation:
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            if not isinstance(state, (CleanupPending, FinalizationPending)):
                return LifecycleResult.not_executed(_snapshot_from_state(state))
            if state.request != request:
                return LifecycleResult.not_executed(_snapshot_from_state(state))

            generation = state.generation
            origin = state.origin
            diagnostics = state.diagnostics
            if isinstance(state, CleanupPending):
                if state.cleanup_status is ResourceCleanupStatus.BLOCKED:
                    snapshot = _snapshot_from_state(state)
                    return LifecycleResult(
                        snapshot,
                        LifecycleOutcome.RECOVERY_INCOMPLETE,
                        diagnostics,
                        origin,
                    )
                owner = state.owner
                previous_status = state.cleanup_status
            else:
                owner = None
                previous_status = None
            self._state = Recovering(
                generation,
                request,
                origin,
                diagnostics,
                owner,
                previous_status,
            )

        if owner is not None:
            report = self._attempt_cleanup_or_interrupt(
                generation,
                request,
                owner,
                origin,
                diagnostics,
            )
            diagnostics = diagnostics.add_cleanup(*report.errors)
            if report.status is not ResourceCleanupStatus.COMPLETE:
                self._commit_cleanup_pending(
                    generation,
                    request,
                    owner,
                    origin,
                    report.status,
                    diagnostics,
                )
                return self._operation_result(
                    LifecycleOutcome.RECOVERY_INCOMPLETE,
                    origin,
                    diagnostics,
                )

        return self._finalize_operation(
            generation,
            request,
            origin,
            diagnostics,
            success_outcome=LifecycleOutcome.RECOVERY_COMPLETED,
            incomplete_outcome=LifecycleOutcome.RECOVERY_INCOMPLETE,
        )

    def _finish_failed_acquire(
        self,
        generation: GenerationT,
        request: RequestT,
        owner: SessionOwner,
        diagnostics: LifecycleDiagnostics,
    ) -> AcquireResult[GenerationT, RequestT, CapabilityT]:
        if owner.has_ownership:
            report = self._attempt_cleanup_or_interrupt(
                generation,
                request,
                owner,
                CleanupOrigin.ACQUIRE,
                diagnostics,
            )
            diagnostics = diagnostics.add_cleanup(*report.errors)
            if report.status is not ResourceCleanupStatus.COMPLETE:
                self._commit_cleanup_pending(
                    generation,
                    request,
                    owner,
                    CleanupOrigin.ACQUIRE,
                    report.status,
                    diagnostics,
                )
                return self._operation_result(
                    LifecycleOutcome.ACQUIRE_FAILED,
                    CleanupOrigin.ACQUIRE,
                    diagnostics,
                )

        return self._finalize_operation(
            generation,
            request,
            CleanupOrigin.ACQUIRE,
            diagnostics,
            success_outcome=LifecycleOutcome.ACQUIRE_FAILED,
            incomplete_outcome=LifecycleOutcome.ACQUIRE_FAILED,
        )

    def _attempt_cleanup_or_interrupt(
        self,
        generation: GenerationT,
        request: RequestT,
        owner: SessionOwner,
        origin: CleanupOrigin,
        diagnostics: LifecycleDiagnostics,
    ) -> CleanupReport:
        try:
            report = owner.cleanup()
            if not isinstance(report, CleanupReport):
                raise TypeError("SessionOwner.cleanup() must return CleanupReport")
            has_ownership = owner.has_ownership
            if report.status is ResourceCleanupStatus.COMPLETE and has_ownership:
                return CleanupReport(
                    ResourceCleanupStatus.BLOCKED,
                    report.errors
                    + (
                        RuntimeError(
                            "SessionOwner reported COMPLETE while ownership remains"
                        ),
                    ),
                )
            if report.status is not ResourceCleanupStatus.COMPLETE and not has_ownership:
                return CleanupReport(
                    ResourceCleanupStatus.COMPLETE,
                    report.errors
                    + (
                        RuntimeError(
                            "SessionOwner reported incomplete cleanup after ownership was discharged"
                        ),
                    ),
                )
            return report
        except BaseException as exc:
            updated = diagnostics.add_cleanup(exc)
            self._commit_cleanup_pending(
                generation,
                request,
                owner,
                origin,
                ResourceCleanupStatus.BLOCKED,
                updated,
            )
            if not isinstance(exc, Exception):
                raise
            return CleanupReport(ResourceCleanupStatus.BLOCKED, (exc,))

    def _finalize_operation(
        self,
        generation: GenerationT,
        request: RequestT,
        origin: CleanupOrigin,
        diagnostics: LifecycleDiagnostics,
        *,
        success_outcome: LifecycleOutcome,
        incomplete_outcome: LifecycleOutcome,
    ) -> LifecycleResult[GenerationT, RequestT, CapabilityT]:
        try:
            next_generation = self._issue_next_generation(generation)
        except BaseException as exc:
            diagnostics = diagnostics.add_finalization(exc)
            self._commit_finalization_pending(
                generation,
                request,
                origin,
                diagnostics,
            )
            if not isinstance(exc, Exception):
                raise
            return self._operation_result(incomplete_outcome, origin, diagnostics)

        with self._lock:
            state = Idle(next_generation)
            self._state = state
            snapshot = _snapshot_from_state(state)
        return LifecycleResult(snapshot, success_outcome, diagnostics, origin)

    def _issue_next_generation(self, previous_generation: GenerationT) -> GenerationT:
        next_generation = self._issue_generation()
        if next_generation is None:
            raise TypeError("issue_generation cannot return None")
        if next_generation == previous_generation:
            raise RuntimeError("issue_generation must return a fresh generation")
        return next_generation

    def _commit_cleanup_pending(
        self,
        generation: GenerationT,
        request: RequestT,
        owner: SessionOwner,
        origin: CleanupOrigin,
        cleanup_status: ResourceCleanupStatus,
        diagnostics: LifecycleDiagnostics,
    ) -> None:
        if not owner.has_ownership:
            raise RuntimeError(
                "CleanupPending must retain an owner with cleanup responsibility"
            )
        with self._lock:
            self._state = CleanupPending(
                generation,
                request,
                owner,
                origin,
                cleanup_status,
                diagnostics,
            )

    def _commit_finalization_pending(
        self,
        generation: GenerationT,
        request: RequestT,
        origin: CleanupOrigin,
        diagnostics: LifecycleDiagnostics,
    ) -> None:
        with self._lock:
            self._state = FinalizationPending(
                generation,
                request,
                origin,
                diagnostics,
            )

    def _operation_result(
        self,
        outcome: LifecycleOutcome,
        origin: CleanupOrigin,
        diagnostics: LifecycleDiagnostics,
    ) -> LifecycleResult[GenerationT, RequestT, CapabilityT]:
        with self._lock:
            snapshot = _snapshot_from_state(self._state)
        return LifecycleResult(snapshot, outcome, diagnostics, origin)


__all__ = ["CapabilityLifecycleCoordinator"]
