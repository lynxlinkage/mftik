"""Dispatch API→TD control-plane requests by Envelope.type."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from mftik.broker import IncomingRequest
from mftik.protocol import (
    TD_ERROR,
    TD_HEALTH,
    RpcError,
    RpcErrorEnvelope,
)

from mftik_td.rpc.health import handle_health

logger = logging.getLogger(__name__)

Handler = Callable[..., Awaitable[None]]

_HANDLERS: dict[str, Handler] = {
    TD_HEALTH: handle_health,
}


async def dispatch(req: IncomingRequest) -> None:
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
    await handler(req)
