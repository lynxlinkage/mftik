"""Qualified type names: ``public::HelloStrategy``, ``alpha::HelloStrategy``.

Bundled strategies stay unprefixed (``NoopStrategy``). This node's trees are
``public`` or ``private``; a pulled copy carries the remote's name so two
copies of the same class can coexist.
"""

from __future__ import annotations

PUBLIC_ORIGIN = "public"
PRIVATE_ORIGIN = "private"
OWN_ORIGINS = frozenset({PUBLIC_ORIGIN, PRIVATE_ORIGIN})
#: Remotes must not collide with this node's origins, or with the old ``local``.
RESERVED_REMOTE_NAMES = frozenset({*OWN_ORIGINS, "local"})
SEP = "::"


def qualify(origin: str, type_name: str) -> str:
    return f"{origin}{SEP}{type_name}"


def split_qualified(value: str) -> tuple[str, str] | None:
    origin, sep, rest = value.partition(SEP)
    if not sep or not origin or not rest or SEP in rest:
        return None
    return origin, rest
