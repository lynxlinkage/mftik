"""STS persistence helpers over mft_db.sts_sessions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mft_db.models.session import SessionStatus, StsSessionRow
from mft_db.repositories import StsSessionRepository
from mft_db.session import session_scope


async def persist_live_session(
    *,
    session_id: str,
    created_by: int,
    strategy: str | None = None,
    td_api_ids: list[int] | None = None,
    md_ids: list[str] | None = None,
    st_paras: dict[str, Any] | None = None,
    cid_slot: int | None = None,
) -> StsSessionRow:
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        existing = await repo.get_by_session_id(session_id)
        if existing is not None:
            return existing
        return await repo.create_live(
            session_id=session_id,
            created_by=created_by,
            strategy=strategy,
            td_api_ids=td_api_ids,
            md_ids=md_ids,
            st_paras=st_paras,
            cid_slot=cid_slot,
        )


async def mark_session_live(session_id: str) -> StsSessionRow | None:
    """Put a session back to ``live`` — used when rebuilding one."""
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        return await repo.mark_live(session_id)


async def remember_fact(
    session_id: str, key: str, value: str
) -> StsSessionRow | None:
    """Persist one fact a strategy cannot re-derive after a restart."""
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        return await repo.remember(session_id, key, value)


async def mark_session_finished(
    session_id: str,
    *,
    status: str = SessionStatus.DONE.value,
    reason: str | None = None,
) -> StsSessionRow | None:
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        return await repo.mark_finished(session_id, status=status, reason=reason)


async def mark_session_done(session_id: str) -> StsSessionRow | None:
    """Natural end. Failures go through :func:`mark_session_finished`."""
    return await mark_session_finished(session_id)


async def list_sessions(
    *,
    status: str | None = "live",
    created_by: int | None = None,
) -> Sequence[StsSessionRow]:
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        return list(
            await repo.list_sessions(status=status, created_by=created_by)
        )
