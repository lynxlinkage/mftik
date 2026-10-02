"""Dispatch API→STS control-plane requests by Envelope.type.

Start, end, list and health are handlers: one decoded message in, one
reply out (H1). Artifacts, registry, env, event-log, fail and force-stop
still take :class:`~mftik.broker.IncomingRequest` and reply themselves.
IF-16 and B5-11 own those. ``sts.ctl.{session_id}`` is not registered.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.broker.handler import Handler as MessageHandler
from mftik.broker.handler import Reply
from mftik.protocol import (
    STS_ARTIFACT_ABORT,
    STS_ARTIFACT_BEGIN,
    STS_ARTIFACT_CHUNK,
    STS_ARTIFACT_COMMIT,
    STS_ARTIFACT_DELETE,
    STS_ARTIFACT_LIST,
    STS_ARTIFACT_READ,
    STS_ENV_SYNC,
    STS_ERROR,
    STS_EVENTLOG_INFO,
    STS_EVENTLOG_READ,
    STS_HEALTH,
    STS_REGISTRY_GENERATION,
    STS_REGISTRY_LOADED,
    STS_REGISTRY_RELOAD,
    STS_REGISTRY_SYNC,
    STS_SESSION_END,
    STS_SESSION_FAIL,
    STS_SESSION_FORCE_STOP,
    STS_SESSION_LIST,
    STS_SESSION_START,
    RpcError,
    RpcErrorEnvelope,
    UntypedEnvelope,
)

from mftik_sts.rpc.artifacts import (
    handle_artifact_abort,
    handle_artifact_begin,
    handle_artifact_chunk,
    handle_artifact_commit,
    handle_artifact_delete,
    handle_artifact_list,
    handle_artifact_read,
)
from mftik_sts.rpc.env import handle_env_sync
from mftik_sts.rpc.eventlog import handle_eventlog_info, handle_eventlog_read
from mftik_sts.rpc.health import handle_health
from mftik_sts.rpc.registry import (
    handle_registry_generation,
    handle_registry_loaded,
    handle_registry_reload,
    handle_registry_sync,
)
from mftik_sts.rpc.sessions import (
    handle_session_fail,
    handle_session_force_stop,
)

if TYPE_CHECKING:
    from mftik.broker import Broker

    from mftik_sts.controller import StsOrchestrator

logger = logging.getLogger(__name__)

Handler = Callable[..., Awaitable[None]]

#: Start, end and list. Served by the controller, not by :data:`_HANDLERS`.
CONTROLLER_TYPES = frozenset(
    {STS_SESSION_START, STS_SESSION_END, STS_SESSION_LIST}
)

_HANDLERS: dict[str, Handler] = {
    STS_ARTIFACT_LIST: handle_artifact_list,
    STS_ARTIFACT_READ: handle_artifact_read,
    STS_ARTIFACT_BEGIN: handle_artifact_begin,
    STS_ARTIFACT_CHUNK: handle_artifact_chunk,
    STS_ARTIFACT_COMMIT: handle_artifact_commit,
    STS_ARTIFACT_ABORT: handle_artifact_abort,
    STS_ARTIFACT_DELETE: handle_artifact_delete,
    STS_EVENTLOG_INFO: handle_eventlog_info,
    STS_EVENTLOG_READ: handle_eventlog_read,
    STS_ENV_SYNC: handle_env_sync,
    STS_REGISTRY_GENERATION: handle_registry_generation,
    STS_REGISTRY_LOADED: handle_registry_loaded,
    STS_REGISTRY_RELOAD: handle_registry_reload,
    STS_REGISTRY_SYNC: handle_registry_sync,
    STS_SESSION_FAIL: handle_session_fail,
    STS_SESSION_FORCE_STOP: handle_session_force_stop,
}


def _accepted_session_id(reply: Reply | None) -> str | None:
    """The session a ``starting`` accept named, or ``None``."""
    if reply is None or reply.type != STS_SESSION_START:
        return None
    payload = reply.payload
    status = getattr(payload, "status", None)
    session_id = getattr(payload, "session_id", None)
    if status != "starting" or not isinstance(session_id, str) or not session_id:
        return None
    return session_id


def _not_ready(message: UntypedEnvelope) -> Reply:
    return RpcErrorEnvelope.wrap(
        RpcError(code="not_ready", message="STS controller is not bound"),
        type=STS_ERROR,
        source="sts",
        session_id=message.session_id,
    )


def _unknown(message: UntypedEnvelope) -> Reply:
    logger.warning("unknown sts rpc type=%s id=%s", message.type, message.id)
    return RpcErrorEnvelope.wrap(
        RpcError(code="unknown_type", message=f"unknown type: {message.type}"),
        type=STS_ERROR,
        source="sts",
        session_id=message.session_id,
    )


def control_handler(
    broker: Broker, orchestrator: StsOrchestrator | None
) -> MessageHandler:
    """Health, start, end, list, and the handlers that still reply themselves.

    Health works when ``orchestrator`` is ``None``, so a probe during boot
    does not need a supervisor. A legacy handler is handed an
    :class:`~mftik.broker.IncomingRequest` and this function returns
    ``None``, so :func:`mftik.broker.handler.serve` does not reply twice.
    """
    from mftik_sts.controller import end_handler, list_handler, start_handler

    start = None if orchestrator is None else start_handler(orchestrator)
    end = None if orchestrator is None else end_handler(orchestrator)
    listing = None if orchestrator is None else list_handler(orchestrator)
    instance = None if orchestrator is None else orchestrator.supervisor.instance

    async def handle(message: UntypedEnvelope) -> Reply | None:
        kind = message.type
        if kind == STS_HEALTH:
            return await handle_health(message)
        if kind in CONTROLLER_TYPES and orchestrator is None:
            return _not_ready(message)
        if kind == STS_SESSION_START and start is not None and orchestrator is not None:
            reply = await start(message)
            session_id = _accepted_session_id(reply)
            if session_id is not None:
                # Spawn before the reply. A capacity refusal is this
                # reply; the process is not left accepted. ``on_start``
                # has not run. Until spawn is entered the session is on
                # the report via extra_workers (B4-07).
                refusal = await orchestrator.finish_start(session_id)
                if refusal is not None:
                    return RpcErrorEnvelope.wrap(
                        RpcError(code=refusal.code, message=refusal.message),
                        type=STS_ERROR,
                        source="sts",
                        session_id=message.session_id,
                    )
            return reply
        if kind == STS_SESSION_END and end is not None:
            return await end(message)
        if kind == STS_SESSION_LIST and listing is not None:
            return await listing(message)
        legacy = _HANDLERS.get(kind)
        if legacy is None:
            return _unknown(message)
        await legacy(IncomingRequest(broker, message), instance=instance)
        return None

    return handle


async def dispatch(
    req: IncomingRequest,
    *,
    instance: str | None = None,
) -> None:
    """Route one request. ``instance`` is which STS this process is.

    It used to arrive as the session manager, which the handlers read an
    instance name off. RM-04 deleted that manager, and the name is the only
    thing any remaining handler wanted from it.
    """
    if req.envelope.type == STS_HEALTH:
        await req.reply(await handle_health(req.envelope))
        return
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
    await handler(req, instance=instance)
