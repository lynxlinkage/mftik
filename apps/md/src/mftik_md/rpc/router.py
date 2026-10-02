"""Dispatch API→MD control-plane requests by Envelope.type.

A handler's whole input is the decoded envelope and its whole output is
the reply (H1). :func:`mftik.broker.handler.serve` is the loop.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mftik.broker.handler import Handler, Reply
from mftik.protocol import (
    MD_ERROR,
    MD_HEALTH,
    MD_TAPE_TAIL,
    RpcError,
    RpcErrorEnvelope,
    UntypedEnvelope,
)

from mftik_md.intents import INTENT_TYPES, MdIntentBook, md_intent_handler
from mftik_md.rpc.health import handle_health
from mftik_md.rpc.tape import handle_tape_tail

if TYPE_CHECKING:
    from mftik_md.tape_store import TapeStore

logger = logging.getLogger(__name__)


def control_handler(store: TapeStore | None, intents: MdIntentBook) -> Handler:
    """Health, tape, and intent put/delete for one MD process.

    ``intents`` is shared by every subject this process serves. The
    pooled ``md`` subject and ``md.{instance}`` therefore hold the same
    owners.
    """
    intents_handle = md_intent_handler(intents)

    async def handle(message: UntypedEnvelope) -> Reply | None:
        kind = message.type
        if kind == MD_HEALTH:
            return await handle_health(message)
        if kind == MD_TAPE_TAIL:
            return await handle_tape_tail(message, store=store)
        if kind in INTENT_TYPES:
            return await intents_handle(message)
        logger.warning("unknown md rpc type=%s id=%s", kind, message.id)
        return RpcErrorEnvelope.wrap(
            RpcError(code="unknown_type", message=f"unknown type: {kind}"),
            type=MD_ERROR,
            source="md",
            session_id=message.session_id,
        )

    return handle
