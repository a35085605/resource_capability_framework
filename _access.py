from __future__ import annotations

from typing import Hashable, Protocol, TypeVar


AccessT = TypeVar("AccessT", contravariant=True)
AccessKeyT = TypeVar("AccessKeyT", bound=Hashable, covariant=True)


class AccessIdentity(Protocol[AccessT, AccessKeyT]):
    """Project an Access to the logical identity used by Managed coordination."""

    def key(self, access: AccessT) -> AccessKeyT: ...


__all__ = ["AccessIdentity"]
