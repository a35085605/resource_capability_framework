from __future__ import annotations

from enum import Enum


class ResourcePolicy(Enum):
    """Whether an existing matching ResourceKey blocks a later matching requirement."""

    BLOCKING = "blocking"
    NON_BLOCKING = "non_blocking"


__all__ = ["ResourcePolicy"]
