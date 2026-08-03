"""Historical session log listing (Postgres)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from mft_db.repositories import SessionLogRepository
from mft_db.session import session_scope

from mft_api.schemas import SessionLogListResponse, SessionLogOut

router = APIRouter(tags=["logs"])

_VALID_DOMAINS = frozenset({"sts", "td", "md"})


@router.get("/logs/{domain}/{stream_id}", response_model=SessionLogListResponse)
async def list_session_logs(
    domain: str,
    stream_id: str,
    before_ts: float | None = Query(default=None),
    before_id: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> SessionLogListResponse:
    if domain not in _VALID_DOMAINS:
        raise HTTPException(
            status_code=400,
            detail=f"domain must be one of {sorted(_VALID_DOMAINS)}",
        )
    if not stream_id:
        raise HTTPException(status_code=400, detail="stream_id is required")

    # Fetch one extra to compute has_more without a separate COUNT.
    fetch_limit = limit + 1
    async with session_scope() as db:
        repo = SessionLogRepository(db)
        rows = await repo.list_before(
            domain,
            stream_id,
            before_ts=before_ts,
            before_id=before_id,
            limit=fetch_limit,
        )
    has_more = len(rows) > limit
    page = rows[:limit]
    return SessionLogListResponse(
        logs=[
            SessionLogOut(
                id=row.envelope_id,
                db_id=row.id,
                ts=row.ts,
                source=row.source,
                level=row.level,
                message=row.message,
            )
            for row in page
        ],
        has_more=has_more,
    )
