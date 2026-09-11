from __future__ import annotations

from enum import Enum


class ResourcePolicy(Enum):
    """Whether an existing Resource blocks the same Access from acquiring it again."""

    BLOCKING = "blocking"
    NON_BLOCKING = "non_blocking"


__all__ = ["ResourcePolicy"]
