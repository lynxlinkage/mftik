from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from mft_db.models.auth import AuthIdentity, AuthKey, AuthOAuthState, AuthSession
from mft_db.repositories.base import BaseRepository


def _as_utc(value: datetime) -> datetime:
    """sqlite has no timezone type and hands these back naive.

    Everything is written in UTC, so attaching it is a correction rather than
    an assumption — and comparing a naive one against an aware ``now`` would
    raise instead of just being wrong, which is at least loud.
    """
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


class AuthSessionRepository(BaseRepository[AuthSession]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, AuthSession)

    async def get_live(self, session_id: str, now: datetime) -> AuthSession | None:
        """A session that exists and has not idled out.

        Expiry is a predicate rather than a sweep: a row past its deadline is
        not a session, whether or not anything has got round to deleting it.
        """
        result = await self.session.execute(
            select(AuthSession).where(
                AuthSession.id == session_id,
                AuthSession.expires_at > now,
            )
        )
        return result.scalar_one_or_none()

    async def touch(
        self, session_id: str, *, now: datetime, expires_at: datetime
    ) -> None:
        row = await self.session.get(AuthSession, session_id)
        if row is None:
            return
        row.last_seen_at = now
        row.expires_at = expires_at

    async def delete(self, session_id: str) -> None:
        await self.session.execute(
            delete(AuthSession).where(AuthSession.id == session_id)
        )

    async def delete_for_user(self, user_id: int) -> None:
        """Every seat this Owner holds. Logout-everywhere, and later single-seat."""
        await self.session.execute(
            delete(AuthSession).where(AuthSession.user_id == user_id)
        )

    async def delete_expired(self, now: datetime) -> int:
        result = await self.session.execute(
            delete(AuthSession).where(AuthSession.expires_at <= now)
        )
        return result.rowcount or 0


class AuthKeyRepository(BaseRepository[AuthKey]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, AuthKey)

    async def get_live_by_prefix(self, prefix: str) -> AuthKey | None:
        """A key that exists and has not been revoked.

        Revocation is a predicate here for the same reason expiry is for
        sessions: a revoked row is not a credential, and nothing should have
        to remember to filter it out at the call site.
        """
        result = await self.session.execute(
            select(AuthKey).where(
                AuthKey.prefix == prefix,
                AuthKey.revoked_at.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def list_for_user(self, user_id: int) -> list[AuthKey]:
        """Every key, revoked ones included — a revoked key is history worth
        showing, and hiding it makes a key that stopped working look like one
        that never existed."""
        result = await self.session.execute(
            select(AuthKey)
            .where(AuthKey.user_id == user_id)
            .order_by(AuthKey.created_at.desc(), AuthKey.id.desc())
        )
        return list(result.scalars())

    async def touch(self, key_id: int, now: datetime) -> None:
        row = await self.session.get(AuthKey, key_id)
        if row is not None:
            row.last_used_at = now

    async def revoke(self, key_id: int, user_id: int, now: datetime) -> AuthKey | None:
        row = await self.session.get(AuthKey, key_id)
        if row is None or row.user_id != user_id:
            return None
        if row.revoked_at is None:
            row.revoked_at = now
        return row


class AuthIdentityRepository(BaseRepository[AuthIdentity]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, AuthIdentity)

    async def get(self, provider: str, subject: str) -> AuthIdentity | None:
        result = await self.session.execute(
            select(AuthIdentity).where(
                AuthIdentity.provider == provider,
                AuthIdentity.subject == subject,
            )
        )
        return result.scalar_one_or_none()

    async def list_for_user(self, user_id: int) -> list[AuthIdentity]:
        result = await self.session.execute(
            select(AuthIdentity)
            .where(AuthIdentity.user_id == user_id)
            .order_by(AuthIdentity.provider, AuthIdentity.id)
        )
        return list(result.scalars())

    async def unlink(self, identity_id: int, user_id: int) -> AuthIdentity | None:
        row = await self.session.get(AuthIdentity, identity_id)
        if row is None or row.user_id != user_id:
            return None
        await self.session.delete(row)
        return row


class AuthOAuthStateRepository(BaseRepository[AuthOAuthState]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, AuthOAuthState)

    async def consume(self, state: str, now: datetime) -> AuthOAuthState | None:
        """Take the record and destroy it in the same breath.

        Single use is the point: a callback replayed with a state that already
        worked must not work again, and the cheapest way to guarantee that is
        for the second lookup to find nothing.
        """
        row = await self.session.get(AuthOAuthState, state)
        if row is None:
            return None
        await self.session.delete(row)
        return None if _as_utc(row.expires_at) <= now else row

    async def delete_expired(self, now: datetime) -> int:
        result = await self.session.execute(
            delete(AuthOAuthState).where(AuthOAuthState.expires_at <= now)
        )
        return result.rowcount or 0
