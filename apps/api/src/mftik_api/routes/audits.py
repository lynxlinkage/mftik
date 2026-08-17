"""Audit log listing."""

from __future__ import annotations

from fastapi import APIRouter
from mftik_db.repositories import AuditRepository
from mftik_db.session import session_scope

from mftik_api.schemas import AuditListResponse, AuditOut

router = APIRouter(tags=["audits"])


@router.get("/audits", response_model=AuditListResponse)
async def list_audits(limit: int = 100) -> AuditListResponse:
    limit = max(1, min(limit, 500))
    async with session_scope() as db:
        repo = AuditRepository(db)
        rows = await repo.list_recent(limit=limit)
    return AuditListResponse(
        audits=[
            AuditOut(
                id=row.id,
                user_id=row.user_id,
                operation=row.operation,
                result=row.result,
                created_at=row.created_at.timestamp() if row.created_at else None,
            )
            for row in rows
        ]
    )
