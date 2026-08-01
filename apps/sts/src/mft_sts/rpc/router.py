"""Dispatch API→STS control-plane requests by Envelope.type."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from mft.broker import IncomingRequest
from mft.protocol import (
    STS_ERROR,
    STS_HEALTH,
    STS_SESSION_CREATE,
    STS_SESSION_LIST,
    STS_SESSION_PAUSE,
    STS_SESSION_RESUME,
    STS_SESSION_STOP,
    RpcError,
    RpcErrorEnvelope,
)

from mft_sts.rpc.health import handle_health
from mft_sts.rpc.sessions import (
    handle_session_create,
    handle_session_list,
    handle_session_pause,
    handle_session_resume,
    handle_session_stop,
)

if TYPE_CHECKING:
    from mft_sts.session import SessionManager

logger = logging.getLogger(__name__)

Handler = Callable[..., Awaitable[None]]

_HANDLERS: dict[str, Handler] = {
    STS_HEALTH: handle_health,
    STS_SESSION_CREATE: handle_session_create,
    STS_SESSION_LIST: handle_session_list,
    STS_SESSION_PAUSE: handle_session_pause,
    STS_SESSION_RESUME: handle_session_resume,
    STS_SESSION_STOP: handle_session_stop,
}


async def dispatch(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    handler = _HANDLERS.get(req.envelope.type)
    if handler is None:
        logger.warning(
            "unknown sts rpc type=%s id=%s",
            req.envelope.type,
            req.envelope.id,
        )
        await req.reply(
            RpcErrorEnvelope.wrap(
                RpcError(
                    code="unknown_type",
                    message=f"unknown type: {req.envelope.type}",
                ),
                type=STS_ERROR,
                source="sts",
                session_id=req.envelope.session_id,
            )
        )
        return
    await handler(req, sessions=sessions)
