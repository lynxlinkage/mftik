"""Best-effort audit logging for API mutations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mftik_db.repositories import AuditRepository
from mftik_db.session import session_scope

if TYPE_CHECKING:
    from mftik_api.auth.principal import Principal

logger = logging.getLogger(__name__)


async def record_audit(
    *,
    user_id: int,
    operation: str,
    result: str,
    principal: Principal | None = None,
    via: str | None = None,
) -> None:
    actor_via = via if via is not None else (principal.via if principal else None)
    key_id = principal.key_id if principal is not None else None
    key_kind = principal.key_kind if principal is not None else None
    try:
        async with session_scope() as db:
            repo = AuditRepository(db)
            await repo.record(
                user_id=user_id,
                operation=operation,
                result=result,
                via=actor_via,
                key_id=key_id,
                key_kind=key_kind,
            )
    except Exception:
        logger.exception("failed to record audit op=%s", operation)
