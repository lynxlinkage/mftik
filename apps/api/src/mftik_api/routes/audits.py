"""Audit log listing."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query
from mftik_db.repositories import AuditRepository
from mftik_db.session import session_scope

from mftik_api.paging import ListOffset
from mftik_api.schemas import AuditListResponse, AuditOut

router = APIRouter(tags=["audits"])


@router.get("/audits", response_model=AuditListResponse)
async def list_audits(
    offset: ListOffset = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> AuditListResponse:
    """Newest first. ``offset`` / ``limit`` page a numbered browse."""
    async with session_scope() as db:
        repo = AuditRepository(db)
        total = await repo.count()
        rows = list(await repo.list_recent(limit=limit, offset=offset))
    return AuditListResponse(
        audits=[
            AuditOut(
                id=row.id,
                user_id=row.user_id,
                operation=row.operation,
                result=row.result,
                created_at=row.created_at.timestamp() if row.created_at else None,
                via=row.via,
                key_kind=row.key_kind,
                key_id=row.key_id,
            )
            for row in rows
        ],
        total=total,
        has_more=offset + len(rows) < total,
    )
