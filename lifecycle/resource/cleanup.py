from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from lifecycle.resource.result import ResourceCleanupResult, ResourceCleanupStatus


ResourceT = TypeVar("ResourceT")


def cleanup_reverse(
    resources: tuple[ResourceT, ...],
    cleanup_one: Callable[[ResourceT], ResourceCleanupResult[ResourceT]],
) -> ResourceCleanupResult[ResourceT]:
    """Clean resources in reverse acquisition order without crossing failed dependencies.

    Cleanup stops at the first resource whose ownership cannot be conclusively
    discharged. Resources acquired before that item are left untouched because they may
    be dependencies of the unresolved item. Errors from every cleanup operation that did
    run are retained. COMPLETE-with-errors is allowed and does not stop cleanup.

    ``cleanup_one`` receives exactly one resource. Its incomplete result must retain that
    same resource and no other resource.
    """

    if not isinstance(resources, tuple):
        raise TypeError("resources must be a PhysicalResources tuple")
    if not callable(cleanup_one):
        raise TypeError("cleanup_one must be callable")

    errors: list[BaseException] = []
    for index in range(len(resources) - 1, -1, -1):
        resource = resources[index]
        try:
            outcome = cleanup_one(resource)
        except Exception as exc:
            # An adapter that throws instead of reporting ownership leaves us unable to
            # prove whether the resource is safe to retry. Preserve it and stop.
            return ResourceCleanupResult.blocked(
                resources[: index + 1],
                errors=(*errors, exc),
            )

        if not isinstance(outcome, ResourceCleanupResult):
            return ResourceCleanupResult.blocked(
                resources[: index + 1],
                errors=(
                    *errors,
                    TypeError("cleanup_one must return ResourceCleanupResult"),
                ),
            )
        errors.extend(outcome.errors)

        if outcome.status is ResourceCleanupStatus.COMPLETE:
            continue

        if outcome.remaining_resources != (resource,):
            return ResourceCleanupResult.blocked(
                resources[: index + 1],
                errors=(
                    *errors,
                    TypeError(
                        "single-resource cleanup must retain exactly the supplied resource"
                    ),
                ),
            )

        remaining = resources[:index] + outcome.remaining_resources
        if outcome.status is ResourceCleanupStatus.RETRYABLE:
            return ResourceCleanupResult.retryable(
                remaining,
                errors=tuple(errors),
            )
        return ResourceCleanupResult.blocked(
            remaining,
            errors=tuple(errors),
        )

    return ResourceCleanupResult.complete(errors=tuple(errors))


__all__ = ["cleanup_reverse"]
