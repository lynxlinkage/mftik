"""Browser sessions: an opaque cookie, and the row that makes it mean something.

The cookie is a random token. The database stores its SHA-256, so a dump of
``auth_sessions`` is not a set of live logins. Nothing ever needs the token
back — verification is "hash what arrived and look that up".
"""

from __future__ import annotations

import hashlib
import os
import secrets
from datetime import UTC, datetime, timedelta

from fastapi import Response
from mftik_db.models.auth import AuthSession
from mftik_db.repositories import AuthSessionRepository
from sqlalchemy.ext.asyncio import AsyncSession

COOKIE_NAME = "mftik_session"

#: Sliding: a session dies this long after the last request that used it.
IDLE_TTL = timedelta(hours=12)

#: Hard ceiling from login, never extended. A seat left logged in forever is
#: the one an attacker inherits with the laptop.
ABSOLUTE_TTL = timedelta(days=7)

#: How stale ``last_seen_at`` may get before a request pays to write it.
#: Sliding the window on *every* request would put a row update in front of
#: every read the UI makes; a minute of imprecision on a twelve-hour window
#: costs nothing.
TOUCH_INTERVAL = timedelta(minutes=1)


def cookie_secure() -> bool:
    """Whether to mark the session cookie ``Secure``.

    On by default and off only where it has to be. Local development is
    ``http://localhost:5173`` end to end: Chrome and Firefox treat localhost
    as a secure context and keep Secure cookies there, but Safari drops them,
    and the symptom is a login that silently does nothing.
    """
    return os.getenv("MFTIK_COOKIE_SECURE", "1").strip().lower() not in {
        "0",
        "false",
        "no",
    }


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _as_utc(value: datetime) -> datetime:
    """Read a timestamp the database returned as UTC.

    sqlite has no timezone type and hands back naive datetimes for the same
    column Postgres returns aware. Everything is written in UTC, so attaching
    it is a correction, not an assumption. Same reasoning as ``board.py``.
    """
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


async def issue(
    db: AsyncSession,
    *,
    user_id: int,
    via: str,
    user_agent: str | None = None,
    ip: str | None = None,
) -> str:
    """Create a session and return the cookie token. Only the hash is stored."""
    token = secrets.token_urlsafe(32)
    now = datetime.now(UTC)
    await AuthSessionRepository(db).add(
        AuthSession(
            id=_digest(token),
            user_id=user_id,
            via=via,
            created_at=now,
            last_seen_at=now,
            expires_at=now + IDLE_TTL,
            user_agent=user_agent[:256] if user_agent else None,
            ip=ip,
        )
    )
    return token


async def resolve(db: AsyncSession, token: str) -> AuthSession | None:
    """The live session this token names, sliding its window on the way.

    Returns None for a token that never existed, one that idled out, and one
    past its absolute lifetime — all of which are "log in again", and none of
    which the caller should be able to tell apart.
    """
    now = datetime.now(UTC)
    repo = AuthSessionRepository(db)
    row = await repo.get_live(_digest(token), now)
    if row is None:
        return None

    if now - _as_utc(row.created_at) >= ABSOLUTE_TTL:
        await repo.delete(row.id)
        return None

    if now - _as_utc(row.last_seen_at) >= TOUCH_INTERVAL:
        await repo.touch(row.id, now=now, expires_at=now + IDLE_TTL)
    return row


async def revoke(db: AsyncSession, token: str) -> None:
    await AuthSessionRepository(db).delete(_digest(token))


def set_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=int(ABSOLUTE_TTL.total_seconds()),
        httponly=True,
        secure=cookie_secure(),
        # Lax, not Strict: a Strict cookie is withheld on the redirect back
        # from an OAuth provider, which is the flow the next steps add.
        samesite="lax",
        path="/",
    )


def clear_cookie(response: Response) -> None:
    response.delete_cookie(
        COOKIE_NAME,
        httponly=True,
        secure=cookie_secure(),
        samesite="lax",
        path="/",
    )
