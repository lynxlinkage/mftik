"""Bring one STS registry disk in line with the API store.

A push that an STS missed is not on its disk, and a restart that only
re-scans that disk will never see it. The process asks for a catch-up
when it starts. Declaring or re-enabling an STS asks as well, in case
the process is already up.
"""

from __future__ import annotations

import asyncio
import logging

from mftik.broker import Broker, IncomingRequest
from mftik.instance import validate_instance_name
from mftik.protocol import (
    API_REGISTRY_CATCHUP,
    ApiRegistryCatchupRequest,
    ApiRegistryCatchupResult,
    ApiRegistryCatchupResultEnvelope,
)
from mftik_db.models.session import SessionDomain
from mftik_db.repositories import InstanceRepository
from mftik_db.session import session_scope

from mftik_api.sts_fanout import reconcile_instance

logger = logging.getLogger(__name__)

#: The NATS subject is the envelope type. One API serves it.
CATCHUP_SUBJECT = API_REGISTRY_CATCHUP

_pending: set[asyncio.Task[None]] = set()


def schedule_reconcile(broker: Broker, name: str) -> None:
    """Fire a reconcile. The HTTP handler must not wait on a down STS."""
    task = asyncio.create_task(
        _quietly(broker, name), name=f"registry-reconcile-{name}"
    )
    _pending.add(task)
    task.add_done_callback(_pending.discard)


async def _quietly(broker: Broker, name: str) -> None:
    try:
        result = await reconcile_instance(broker, name)
    except Exception:
        logger.exception("registry reconcile failed for %s", name)
        return
    if result.error:
        logger.warning("registry reconcile for %s: %s", name, result.error)
        return
    logger.info(
        "registry reconcile for %s: %d key(s)", name, len(result.loaded)
    )


async def handle_catchup(broker: Broker, req: IncomingRequest) -> None:
    """Answer one STS that just started, by pushing the current store."""
    body = ApiRegistryCatchupRequest.model_validate(req.envelope.payload)
    try:
        name = validate_instance_name(body.instance)
    except ValueError as exc:
        await _reply(req, ok=False, error=str(exc))
        return
    async with session_scope() as db:
        row = await InstanceRepository(db).get_by_name(name)
    if (
        row is None
        or row.domain != SessionDomain.STS.value
        or not row.enabled
    ):
        await _reply(req, ok=False, error=f"{name} is not an enabled STS")
        return
    result = await reconcile_instance(broker, name)
    await _reply(req, ok=result.error is None, error=result.error)


async def _reply(
    req: IncomingRequest, *, ok: bool, error: str | None
) -> None:
    await req.reply(
        ApiRegistryCatchupResultEnvelope.wrap(
            ApiRegistryCatchupResult(ok=ok, error=error),
            type=API_REGISTRY_CATCHUP,
            source="api",
        )
    )


async def serve_registry_catchup(broker: Broker, stop: asyncio.Event) -> None:
    """Serve catch-up requests until ``stop``."""

    async def handle(req: IncomingRequest) -> None:
        await handle_catchup(broker, req)

    while not stop.is_set():
        try:
            await broker.serve_handler(CATCHUP_SUBJECT, handle, stop=stop)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("registry catch-up serve failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=1)
            except TimeoutError:
                continue
