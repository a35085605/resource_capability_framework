from __future__ import annotations

from dataclasses import dataclass
from functools import total_ordering
from numbers import Integral


def _normalize_required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string, got {type(value).__name__}")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty")
    return normalized


@total_ordering
@dataclass(frozen=True, slots=True, eq=False)
class TcpAddress:
    """Validated TCP host and port endpoint."""

    host: str
    port: int

    def __post_init__(self) -> None:
        host = _normalize_required_text(self.host, field_name="TCP address host")
        if isinstance(self.port, bool) or not isinstance(self.port, Integral):
            raise TypeError("TCP address port must be an integer")
        port = int(self.port)
        if not 1 <= port <= 65535:
            raise ValueError("TCP address port must be between 1 and 65535")
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "port", port)

    def _comparison_key(self) -> tuple[str, int]:
        return self.host, self.port

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TcpAddress):
            return NotImplemented
        return self._comparison_key() == other._comparison_key()

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, TcpAddress):
            return NotImplemented
        return self._comparison_key() < other._comparison_key()

    def __hash__(self) -> int:
        return hash(self._comparison_key())


__all__ = ["TcpAddress"]
