"""Dispatch API→MD control-plane requests by Envelope.type."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import (
    MD_ERROR,
    MD_HEALTH,
    MD_TAPE_TAIL,
    RpcError,
    RpcErrorEnvelope,
)

from mftik_md.rpc.health import handle_health
from mftik_md.rpc.tape import handle_tape_tail

if TYPE_CHECKING:
    from mftik_md.tape_store import TapeStore

logger = logging.getLogger(__name__)

Handler = Callable[..., Awaitable[None]]

_HANDLERS: dict[str, Handler] = {
    MD_HEALTH: handle_health,
    MD_TAPE_TAIL: handle_tape_tail,
}


async def dispatch(
    req: IncomingRequest,
    *,
    store: TapeStore | None = None,
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
    await handler(req, store=store)
