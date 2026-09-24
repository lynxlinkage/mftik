"""Dispatch API→STS control-plane requests by Envelope.type."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
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
    STS_REGISTRY_RELOAD,
    STS_SESSION_CREATE,
    STS_SESSION_FAIL,
    STS_SESSION_LIST,
    STS_SESSION_STOP,
    RpcError,
    RpcErrorEnvelope,
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
    handle_registry_reload,
)
from mftik_sts.rpc.sessions import (
    handle_session_create,
    handle_session_fail,
    handle_session_list,
    handle_session_stop,
)

if TYPE_CHECKING:
    from mftik_sts.session import SessionManager

logger = logging.getLogger(__name__)

Handler = Callable[..., Awaitable[None]]

_HANDLERS: dict[str, Handler] = {
    STS_HEALTH: handle_health,
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
    STS_REGISTRY_RELOAD: handle_registry_reload,
    STS_SESSION_CREATE: handle_session_create,
    STS_SESSION_LIST: handle_session_list,
    STS_SESSION_FAIL: handle_session_fail,
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
