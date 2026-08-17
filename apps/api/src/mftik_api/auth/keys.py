"""Bearer credentials: minting them, and recognising one on the wire.

A token is `mft_ak_` or `mft_rk_` followed by 256 bits of randomness. The
prefix on the wire says which kind it claims to be before anything is looked
up, and the first characters of the random part are stored — unique, indexed
— so verification is one exact lookup rather than a scan over every hash.

The secret is returned exactly once, by ``mint``. After that this module can
only confirm a token, never produce one.
"""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from mftik_db.models.auth import AuthKey, KeyKind
from mftik_db.repositories import AuthKeyRepository
from sqlalchemy.ext.asyncio import AsyncSession

from mftik_api.auth.principal import SCOPE_API, SCOPE_REGISTRY_READ

#: Wire prefix per kind. Distinct so a leaked token is identifiable on sight —
#: in a log, in a shell history — as this platform's, and as which kind.
WIRE_PREFIX: dict[str, str] = {
    KeyKind.API.value: "mft_ak_",
    KeyKind.REGISTRY.value: "mft_rk_",
}

#: Characters of the random part kept in the clear. Long enough that a
#: collision is not a practical concern, short enough to be useless alone.
PREFIX_LEN = 8

#: What each kind may do. An API key stands in for the Owner on the domain
#: routes and can read the registry like the Owner can; a registry key is not
#: a person at all and can only read what this node publishes. Neither can
#: reach the routes that change identities or mint more keys — that needs a
#: browser session, which is the point of issuing scoped credentials at all.
SCOPES: dict[str, tuple[str, ...]] = {
    KeyKind.API.value: (SCOPE_API, SCOPE_REGISTRY_READ),
    KeyKind.REGISTRY.value: (SCOPE_REGISTRY_READ,),
}

#: How stale ``last_used_at`` may get before a request pays to write it. Same
#: bargain as a session's idle window: a busy CI key must not cost a row
#: update per call to record a field nobody reads to the minute.
TOUCH_INTERVAL = timedelta(minutes=1)


@dataclass(frozen=True, slots=True)
class MintedKey:
    """The one moment the secret exists outside the caller's hands."""

    token: str
    prefix: str
    key_hash: str
    scopes: tuple[str, ...]


def digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def mint(kind: str) -> MintedKey:
    wire = WIRE_PREFIX[kind]
    body = secrets.token_urlsafe(32)
    token = f"{wire}{body}"
    return MintedKey(
        token=token,
        prefix=body[:PREFIX_LEN],
        key_hash=digest(token),
        scopes=SCOPES[kind],
    )


def parse(token: str) -> tuple[str, str] | None:
    """``(kind, prefix)`` for something shaped like one of our tokens."""
    for kind, wire in WIRE_PREFIX.items():
        if not token.startswith(wire):
            continue
        body = token[len(wire) :]
        if len(body) <= PREFIX_LEN:
            return None
        return kind, body[:PREFIX_LEN]
    return None


async def resolve(db: AsyncSession, token: str) -> AuthKey | None:
    """The live key this token names, recording that it was used.

    The wire prefix is checked against the stored ``kind`` rather than
    trusted: it is attacker-supplied, and a token that says `mft_rk_` while
    naming a row minted as an API key is not a key, it is someone probing.
    """
    parsed = parse(token)
    if parsed is None:
        return None
    kind, prefix = parsed

    repo = AuthKeyRepository(db)
    row = await repo.get_live_by_prefix(prefix)
    if row is None or row.kind != kind:
        return None
    # Constant time: the prefix narrowed it to one row, and this is the part
    # an attacker would otherwise learn a byte at a time.
    if not secrets.compare_digest(row.key_hash, digest(token)):
        return None

    now = datetime.now(UTC)
    last = row.last_used_at
    if last is None or now - _as_utc(last) >= TOUCH_INTERVAL:
        await repo.touch(row.id, now)
    return row


def _as_utc(value: datetime) -> datetime:
    """sqlite returns naive datetimes where Postgres returns aware ones."""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def display(row: AuthKey) -> str:
    """What a list can show: the wire prefix, the stored prefix, and no more."""
    return f"{WIRE_PREFIX.get(row.kind, '')}{row.prefix}…"
