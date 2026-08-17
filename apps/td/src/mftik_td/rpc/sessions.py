"""TD session attach / list RPC handlers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import (
    TD_ERROR,
    TD_SESSION_ATTACH,
    TD_SESSION_DETACH,
    TD_SESSION_LIST,
    ListSessionsRequest,
    ListSessionsResult,
    ListSessionsResultEnvelope,
    RpcError,
    RpcErrorEnvelope,
    TdAttachRequest,
    TdAttachResultEnvelope,
    TdDetachRequest,
    TdDetachResult,
    TdDetachResultEnvelope,
)

if TYPE_CHECKING:
    from mftik_td.session import SessionManager

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


async def handle_session_detach(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    """Close one attach and answer with what the api_id has left.

    Served here rather than on the session stream because this is the one
    request whose delivery cannot be checked afterwards: everything that
    would notice a missed detach — the lease, the watchdog, the ACKs — lives
    in the loop the detach is meant to end. Answering from the process-level
    RPC subject means one always-on server task handles it, and the caller
    finds out either way.

    ``detach`` is idempotent: an attach that is already gone still closes its
    row, so a retry after a lost reply is a no-op rather than an error.
    """
    if sessions is None:
        await _error(req, "unavailable", "session manager not configured")
        return

    try:
        payload = TdDetachRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return

    try:
        await sessions.detach(
            session_id=payload.session_id,
            api_id=payload.api_id,
            reason=payload.reason,
        )
    except Exception as exc:
        logger.exception("td.session.detach failed")
        await _error(req, "detach_failed", str(exc))
        return

    await req.reply(
        TdDetachResultEnvelope.wrap(
            TdDetachResult(
                session_id=payload.session_id,
                api_id=payload.api_id,
                refcount=sessions.refcount(payload.api_id),
            ),
            type=TD_SESSION_DETACH,
            source="td",
            session_id=payload.session_id,
        )
    )


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
