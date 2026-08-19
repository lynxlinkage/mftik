"""STS session create / list / control RPC handlers."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import (
    STS_ERROR,
    STS_SESSION_CREATE,
    STS_SESSION_FAIL,
    STS_SESSION_LIST,
    STS_SESSION_STOP,
    ListSessionsRequest,
    ListSessionsResult,
    ListSessionsResultEnvelope,
    RpcError,
    RpcErrorEnvelope,
    StsCreateSessionRequest,
    StsCreateSessionResultEnvelope,
    StsSessionControlRequest,
    StsSessionControlResult,
    StsSessionControlResultEnvelope,
)

if TYPE_CHECKING:
    from mftik_sts.session import SessionManager

logger = logging.getLogger(__name__)

ControlFn = Callable[[str], Awaitable[StsSessionControlResult]]


async def handle_session_create(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    if sessions is None:
        await _error(req, "unavailable", "session manager not configured")
        return
    try:
        payload = StsCreateSessionRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    try:
        result = await sessions.create_session(payload)
    except KeyError as exc:
        await _error(req, "unknown_strategy", str(exc))
        return
    except Exception as exc:
        logger.exception("sts.session.create failed")
        await _error(req, "create_failed", str(exc))
        return
    await req.reply(
        StsCreateSessionResultEnvelope.wrap(
            result,
            type=STS_SESSION_CREATE,
            source="sts",
            session_id=result.session_id,
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
        raw = req.envelope.payload or {"domain": "sts"}
        if "domain" not in raw:
            raw = {**raw, "domain": "sts"}
        payload = ListSessionsRequest.model_validate(raw)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    items = await sessions.list_sessions(payload)
    await req.reply(
        ListSessionsResultEnvelope.wrap(
            ListSessionsResult(sessions=items),
            type=STS_SESSION_LIST,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


async def handle_session_stop(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    await _control(req, sessions=sessions, action="stop", reply_type=STS_SESSION_STOP)


async def handle_session_fail(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    if sessions is None:
        await _error(req, "unavailable", "session manager not configured")
        return
    try:
        payload = StsSessionControlRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    try:
        result = await sessions.fail_session(
            payload.session_id,
            reason=payload.reason or "failed",
        )
    except KeyError as exc:
        await _error(req, "not_found", str(exc))
        return
    except Exception as exc:
        logger.exception("sts.session.fail failed")
        await _error(req, "fail_failed", str(exc))
        return
    await req.reply(
        StsSessionControlResultEnvelope.wrap(
            result,
            type=STS_SESSION_FAIL,
            source="sts",
            session_id=result.session_id,
        )
    )


async def _control(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None,
    action: str,
    reply_type: str,
) -> None:
    if sessions is None:
        await _error(req, "unavailable", "session manager not configured")
        return
    try:
        payload = StsSessionControlRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return

    fn: ControlFn | None = {
        "stop": sessions.stop_session,
    }.get(action)
    if fn is None:
        await _error(req, "unknown_action", action)
        return

    try:
        result = await fn(payload.session_id)
    except KeyError as exc:
        await _error(req, "not_found", str(exc))
        return
    except Exception as exc:
        logger.exception("sts.session.%s failed", action)
        await _error(req, f"{action}_failed", str(exc))
        return

    await req.reply(
        StsSessionControlResultEnvelope.wrap(
            result,
            type=reply_type,
            source="sts",
            session_id=result.session_id,
        )
    )


async def _error(req: IncomingRequest, code: str, message: str) -> None:
    await req.reply(
        RpcErrorEnvelope.wrap(
            RpcError(code=code, message=message),
            type=STS_ERROR,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )
