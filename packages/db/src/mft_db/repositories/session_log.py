from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from mft_db.models.session_log import SessionLog
from mft_db.repositories.base import BaseRepository


class SessionLogRepository(BaseRepository[SessionLog]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, SessionLog)

    async def bulk_insert_ignore(
        self,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        """Insert rows; skip conflicts on ``envelope_id``. Returns attempted count."""
        if not rows:
            return 0
        bind = self.session.get_bind()
        dialect = bind.dialect.name if bind is not None else "postgresql"
        insert = sqlite_insert if dialect == "sqlite" else pg_insert
        stmt = (
            insert(SessionLog)
            .values(list(rows))
            .on_conflict_do_nothing(index_elements=["envelope_id"])
        )
        await self.session.execute(stmt)
        await self.session.flush()
        return len(rows)

    async def list_before(
        self,
        domain: str,
        stream_id: str,
        *,
        before_ts: float | None = None,
        before_id: int | None = None,
        limit: int = 100,
    ) -> Sequence[SessionLog]:
        """Return up to ``limit`` rows newer-first, optionally older than a cursor."""
        q = select(SessionLog).where(
            SessionLog.domain == domain,
            SessionLog.stream_id == stream_id,
        )
        if before_ts is not None:
            if before_id is not None:
                q = q.where(
                    (SessionLog.ts < before_ts)
                    | ((SessionLog.ts == before_ts) & (SessionLog.id < before_id))
                )
            else:
                q = q.where(SessionLog.ts < before_ts)
        q = q.order_by(SessionLog.ts.desc(), SessionLog.id.desc()).limit(limit)
        result = await self.session.execute(q)
        return result.scalars().all()
