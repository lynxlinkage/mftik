"""TD persistence helpers over mftik_db.td_sessions."""

from __future__ import annotations

from dataclasses import dataclass

from mftik_db.models.api import Api
from mftik_db.repositories import ApiRepository, TdSessionRepository
from mftik_db.session import session_scope


async def count_live_for_api(api_id: int) -> int:
    async with session_scope() as db:
        repo = TdSessionRepository(db)
        return await repo.count_live_for_api(api_id)


async def get_api(api_id: int) -> Api | None:
    """Load the venue credential row backing ``api_id``."""
    async with session_scope() as db:
        return await ApiRepository(db).get(api_id)


@dataclass(frozen=True)
class AccountCredential:
    """The fields an account worker needs, copied out of the session."""

    api_id: int
    venue: str
    api_key: str
    api_secret: str
    passphrase: str | None
    cancel_on_disconnect: bool


@dataclass(frozen=True)
class InstanceBinding:
    """One ``apis`` row bound to an instance, without the secret."""

    api_id: int
    venue: str
    cancel_on_disconnect: bool


async def account_credential(api_id: int) -> AccountCredential | None:
    """The credential for ``api_id``, or ``None`` when the row is gone.

    Copied before the session closes. A detached ORM row would expire
    on the next attribute read.
    """
    async with session_scope() as db:
        row = await ApiRepository(db).get(api_id)
        if row is None:
            return None
        return AccountCredential(
            api_id=row.id,
            venue=row.venue,
            api_key=row.api_key,
            api_secret=row.api_secret,
            passphrase=row.passphrase,
            cancel_on_disconnect=bool(row.cancel_on_disconnect),
        )


async def bindings_for_instance(instance: str) -> tuple[InstanceBinding, ...]:
    """Every account bound to ``instance``, secrets left in the database."""
    async with session_scope() as db:
        rows = await ApiRepository(db).list_by_instance(instance)
        return tuple(
            InstanceBinding(
                api_id=row.id,
                venue=row.venue,
                cancel_on_disconnect=bool(row.cancel_on_disconnect),
            )
            for row in rows
        )
