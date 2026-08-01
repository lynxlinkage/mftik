"""Dispatch API→TD control-plane requests by Envelope.type."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from mft.broker import IncomingRequest
from mft.protocol import (
    TD_ERROR,
    TD_HEALTH,
    TD_SESSION_ATTACH,
    TD_SESSION_CREATE,
    TD_SESSION_LIST,
    RpcError,
    RpcErrorEnvelope,
)

from mft_td.rpc.health import handle_health
from mft_td.rpc.sessions import handle_session_attach, handle_session_list

if TYPE_CHECKING:
    from mft_td.session import SessionManager

logger = logging.getLogger(__name__)

Handler = Callable[..., Awaitable[None]]

_HANDLERS: dict[str, Handler] = {
    TD_HEALTH: handle_health,
    TD_SESSION_ATTACH: handle_session_attach,
    TD_SESSION_CREATE: handle_session_attach,
    TD_SESSION_LIST: handle_session_list,
}


async def dispatch(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    """Route a request to its handler, or reply with ``td.error``."""
    handler = _HANDLERS.get(req.envelope.type)
    if handler is None:
        logger.warning(
            "unknown td rpc type=%s id=%s",
            req.envelope.type,
            req.envelope.id,
        )
        await req.reply(
            RpcErrorEnvelope.wrap(
                RpcError(
                    code="unknown_type",
                    message=f"unknown type: {req.envelope.type}",
                ),
                type=TD_ERROR,
                source="td",
                session_id=req.envelope.session_id,
            )
        )
        return
    await handler(req, sessions=sessions)
