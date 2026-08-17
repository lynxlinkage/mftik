"""TD persistence helpers over mftik_db.td_sessions."""

from __future__ import annotations

from collections.abc import Sequence

from mftik_db.models.api import Api
from mftik_db.models.session import TdSessionRow
from mftik_db.repositories import ApiRepository, TdSessionRepository
from mftik_db.session import session_scope


async def persist_live_session(
    *,
    session_id: str,
    created_by: int,
    api_id: int,
) -> TdSessionRow:
    async with session_scope() as db:
        repo = TdSessionRepository(db)
        return await repo.attach_live(
            session_id=session_id,
            created_by=created_by,
            api_id=api_id,
        )


async def mark_session_done(*, session_id: str, api_id: int) -> TdSessionRow | None:
    async with session_scope() as db:
        repo = TdSessionRepository(db)
        return await repo.mark_done(session_id=session_id, api_id=api_id)


async def list_sessions(
    *,
    status: str | None = "live",
    created_by: int | None = None,
    limit: int = 100,
) -> Sequence[TdSessionRow]:
    async with session_scope() as db:
        repo = TdSessionRepository(db)
        return list(
            await repo.list_sessions(
                status=status, created_by=created_by, limit=limit
            )
        )


async def count_live_for_api(api_id: int) -> int:
    async with session_scope() as db:
        repo = TdSessionRepository(db)
        return await repo.count_live_for_api(api_id)


async def get_api(api_id: int) -> Api | None:
    """Load the venue credential row backing ``api_id``."""
    async with session_scope() as db:
        return await ApiRepository(db).get(api_id)
