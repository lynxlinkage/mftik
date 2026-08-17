"""Dispatch API→MD control-plane requests by Envelope.type."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import (
    MD_ERROR,
    MD_HEALTH,
    MD_SESSION_ATTACH,
    MD_SESSION_DETACH,
    MD_SESSION_LIST,
    RpcError,
    RpcErrorEnvelope,
)

from mftik_md.rpc.health import handle_health
from mftik_md.rpc.sessions import (
    handle_session_attach,
    handle_session_detach,
    handle_session_list,
)

if TYPE_CHECKING:
    from mftik_md.session import SessionManager

logger = logging.getLogger(__name__)

Handler = Callable[..., Awaitable[None]]

_HANDLERS: dict[str, Handler] = {
    MD_HEALTH: handle_health,
    MD_SESSION_ATTACH: handle_session_attach,
    MD_SESSION_DETACH: handle_session_detach,
    MD_SESSION_LIST: handle_session_list,
}


async def dispatch(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    handler = _HANDLERS.get(req.envelope.type)
    if handler is None:
        logger.warning(
            "unknown md rpc type=%s id=%s",
            req.envelope.type,
            req.envelope.id,
        )
        await req.reply(
            RpcErrorEnvelope.wrap(
                RpcError(
                    code="unknown_type",
                    message=f"unknown type: {req.envelope.type}",
                ),
                type=MD_ERROR,
                source="md",
                session_id=req.envelope.session_id,
            )
        )
        return
    await handler(req, sessions=sessions)
