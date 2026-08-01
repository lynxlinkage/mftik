"""TD session attach / list RPC handlers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mft.broker import IncomingRequest
from mft.protocol import (
    TD_ERROR,
    TD_SESSION_ATTACH,
    TD_SESSION_LIST,
    ListSessionsRequest,
    ListSessionsResult,
    ListSessionsResultEnvelope,
    RpcError,
    RpcErrorEnvelope,
    TdAttachRequest,
    TdAttachResultEnvelope,
)

if TYPE_CHECKING:
    from mft_td.session import SessionManager

logger = logging.getLogger(__name__)


async def handle_session_attach(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    if sessions is None:
        await _error(req, "unavailable", "session manager not configured")
        return

    try:
        payload = TdAttachRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return

    try:
        result = await sessions.attach(payload)
    except TimeoutError as exc:
        await _error(req, "timeout", str(exc))
        return
    except Exception as exc:
        logger.exception("td.session.attach failed")
        await _error(req, "attach_failed", str(exc))
        return

    await req.reply(
        TdAttachResultEnvelope.wrap(
            result,
            type=TD_SESSION_ATTACH,
            source="td",
            session_id=result.session_id,
        )
    )


# Alias for older dispatcher registration name
handle_session_create = handle_session_attach


async def handle_session_list(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    if sessions is None:
        await _error(req, "unavailable", "session manager not configured")
        return

    try:
        payload = ListSessionsRequest.model_validate(req.envelope.payload or {})
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return

    items = await sessions.list_sessions(payload)
    await req.reply(
        ListSessionsResultEnvelope.wrap(
            ListSessionsResult(sessions=items),
            type=TD_SESSION_LIST,
            source="td",
            session_id=req.envelope.session_id,
        )
    )


async def _error(req: IncomingRequest, code: str, message: str) -> None:
    await req.reply(
        RpcErrorEnvelope.wrap(
            RpcError(code=code, message=message),
            type=TD_ERROR,
            source="td",
            session_id=req.envelope.session_id,
        )
    )
