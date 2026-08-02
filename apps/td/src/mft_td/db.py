"""TD persistence helpers over mft_db.td_sessions."""

from __future__ import annotations

from collections.abc import Sequence

from mft_db.models.api import Api
from mft_db.models.session import TdSessionRow
from mft_db.repositories import ApiRepository, TdSessionRepository
from mft_db.session import session_scope


async def persist_live_session(
    *,
    session_id: str,
    created_by: int,
    api_id: int,
) -> TdSessionRow:
    async with session_scope() as db:
        repo = TdSessionRepository(db)
        existing = await repo.get_live(session_id=session_id, api_id=api_id)
        if existing is not None:
            return existing
        return await repo.create_live(
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
) -> Sequence[TdSessionRow]:
    async with session_scope() as db:
        repo = TdSessionRepository(db)
        return list(
            await repo.list_sessions(status=status, created_by=created_by)
        )


async def count_live_for_api(api_id: int) -> int:
    async with session_scope() as db:
        repo = TdSessionRepository(db)
        return await repo.count_live_for_api(api_id)


async def get_api(api_id: int) -> Api | None:
    """Load the venue credential row backing ``api_id``."""
    async with session_scope() as db:
        return await ApiRepository(db).get(api_id)
