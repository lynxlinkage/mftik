"""MD persistence helpers over mft_db.md_sessions."""

from __future__ import annotations

from collections.abc import Sequence

from mft_db.models.session import MdSessionRow
from mft_db.repositories import MdSessionRepository
from mft_db.session import session_scope


async def persist_live_session(
    *,
    session_id: str,
    created_by: int,
    venues: list[str] | None = None,
) -> list[MdSessionRow]:
    """Upsert live attach rows for each venue bound to ``session_id``."""
    venue_list = list(venues or [])
    if not venue_list:
        return []
    out: list[MdSessionRow] = []
    async with session_scope() as db:
        repo = MdSessionRepository(db)
        for venue in venue_list:
            out.append(
                await repo.attach_live(
                    venue=venue,
                    session_id=session_id,
                    created_by=created_by,
                )
            )
    return out


async def mark_session_done(*, session_id: str) -> list[MdSessionRow]:
    async with session_scope() as db:
        repo = MdSessionRepository(db)
        return await repo.mark_done_session(session_id)


async def list_sessions(
    *,
    status: str | None = "live",
    created_by: int | None = None,
) -> Sequence[MdSessionRow]:
    async with session_scope() as db:
        repo = MdSessionRepository(db)
        return list(
            await repo.list_sessions(status=status, created_by=created_by)
        )
