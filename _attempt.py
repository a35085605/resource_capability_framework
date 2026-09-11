from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True, eq=False)
class AttemptId:
    """Opaque process-local identity for one Resource acquisition lifecycle."""


__all__ = ["AttemptId"]
