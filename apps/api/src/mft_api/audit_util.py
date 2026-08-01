"""Best-effort audit logging for API mutations."""

from __future__ import annotations

import logging

from mft_db.repositories import AuditRepository
from mft_db.session import session_scope

logger = logging.getLogger(__name__)


async def record_audit(*, user_id: int, operation: str, result: str) -> None:
    try:
        async with session_scope() as db:
            repo = AuditRepository(db)
            await repo.record(user_id=user_id, operation=operation, result=result)
    except Exception:
        logger.exception("failed to record audit op=%s", operation)
