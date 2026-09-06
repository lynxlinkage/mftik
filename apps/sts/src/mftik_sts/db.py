"""STS persistence helpers over mftik_db.sts_sessions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mftik_db.models.session import SessionStatus, StsSessionRow
from mftik_db.repositories import ApiRepository, StsSessionRepository
from mftik_db.session import session_scope


async def persist_live_session(
    *,
    session_id: str,
    created_by: int,
    strategy: str | None = None,
    type: str | None = None,
    yaml_text: str | None = None,
    td: dict[str, Any] | None = None,
    md_ids: list[str] | None = None,
    st_paras: dict[str, Any] | None = None,
    cid_slot: int | None = None,
    restart: str = "always",
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
            type=type,
            yaml_text=yaml_text,
            td=td,
            md_ids=md_ids,
            st_paras=st_paras,
            cid_slot=cid_slot,
            restart=restart,
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
    limit: int = 100,
) -> Sequence[StsSessionRow]:
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        return list(
            await repo.list_sessions(
                status=status, created_by=created_by, limit=limit
            )
        )


async def bump_rebuild_count(session_id: str) -> int:
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        return await repo.bump_rebuild_count(session_id)


async def reset_rebuild_count(session_id: str) -> StsSessionRow | None:
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        return await repo.reset_rebuild_count(session_id)


async def td_instance(api_id: int) -> str | None:
    """Which TD instance may use this credential.

    STS resolves it here rather than reading a name recorded in the session
    document. A credential can be moved between instances, and a copy taken at
    deploy time would send a rebuild — days later, after a restart — to the
    instance that used to be allowed to use it. ``apis.instance_id`` is the
    only thing that knows.
    """
    async with session_scope() as db:
        return await ApiRepository(db).instance_name(api_id)
