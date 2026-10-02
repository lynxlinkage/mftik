"""Dispatch API→TD control-plane requests by Envelope.type.

A handler's whole input is the decoded envelope and its whole output is
the reply (H1). :func:`mftik.broker.handler.serve` is the loop. The
order path is not on this subject: ``td.order.{api_id}`` is the account
worker's (§7.1).
"""

from __future__ import annotations

import logging

from mftik.broker.handler import Reply
from mftik.protocol import (
    TD_ERROR,
    TD_HEALTH,
    RpcError,
    RpcErrorEnvelope,
    UntypedEnvelope,
)

from mftik_td.controller import INTENT_TYPES, intent_book, intent_handler
from mftik_td.rpc.health import handle_health

logger = logging.getLogger(__name__)


async def dispatch(message: UntypedEnvelope) -> Reply | None:
    """Route one control-plane message, or answer ``td.error``."""
    if message.type in INTENT_TYPES:
        return await intent_handler(intent_book())(message)
    if message.type == TD_HEALTH:
        return await handle_health(message)
    logger.warning("unknown td rpc type=%s id=%s", message.type, message.id)
    return RpcErrorEnvelope.wrap(
        RpcError(code="unknown_type", message=f"unknown type: {message.type}"),
        type=TD_ERROR,
        source="td",
        session_id=message.session_id,
    )
