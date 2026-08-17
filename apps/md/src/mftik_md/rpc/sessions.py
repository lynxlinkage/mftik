"""MD session attach / detach / list RPC handlers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import (
    MD_ERROR,
    MD_SESSION_ATTACH,
    MD_SESSION_DETACH,
    MD_SESSION_LIST,
    ListSessionsRequest,
    ListSessionsResult,
    ListSessionsResultEnvelope,
    MdAttachRequest,
    MdAttachResultEnvelope,
    MdDetachRequest,
    MdDetachResult,
    MdDetachResultEnvelope,
    RpcError,
    RpcErrorEnvelope,
)

if TYPE_CHECKING:
    from mftik_md.session import SessionManager

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
        payload = MdAttachRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return

    try:
        result = await sessions.attach(payload)
    except TimeoutError as exc:
        await _error(req, "timeout", str(exc))
        return
    except Exception as exc:
        logger.exception("md.session.attach failed")
        await _error(req, "attach_failed", str(exc))
        return

    await req.reply(
        MdAttachResultEnvelope.wrap(
            result,
            type=MD_SESSION_ATTACH,
            source="md",
            session_id=result.session_id,
        )
    )


async def handle_session_detach(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    """Close this session's attach and answer.

    Served here rather than on the session stream for the reason given in
    TD's handler: the lease loop is both the only reader of that stream and
    the thing a detach ends, so a detach sent there is lost exactly when the
    loop is already gone — the case that leaves a feed pumping for a session
    nobody is leasing.
    """
    if sessions is None:
        await _error(req, "unavailable", "session manager not configured")
        return

    try:
        payload = MdDetachRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return

    try:
        await sessions.detach(
            session_id=payload.session_id, reason=payload.reason
        )
    except Exception as exc:
        logger.exception("md.session.detach failed")
        await _error(req, "detach_failed", str(exc))
        return

    await req.reply(
        MdDetachResultEnvelope.wrap(
            MdDetachResult(session_id=payload.session_id),
            type=MD_SESSION_DETACH,
            source="md",
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
            type=MD_SESSION_LIST,
            source="md",
            session_id=req.envelope.session_id,
        )
    )


async def _error(req: IncomingRequest, code: str, message: str) -> None:
    await req.reply(
        RpcErrorEnvelope.wrap(
            RpcError(code=code, message=message),
            type=MD_ERROR,
            source="md",
            session_id=req.envelope.session_id,
        )
    )
