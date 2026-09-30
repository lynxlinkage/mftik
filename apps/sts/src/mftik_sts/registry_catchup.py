"""Ask the API to make this disk match its registry store.

A restart used to only re-scan local files. A push or delete this process
missed is not among them, so boot asks the API to send the difference,
including deletes for trees the API no longer has. Retries until that
succeeds or the process is stopping: the API may not be up yet, and the
instance row may not be declared yet.
"""

from __future__ import annotations

import asyncio
import logging

from mftik.broker import Broker
from mftik.protocol import (
    API_REGISTRY_CATCHUP,
    ApiRegistryCatchupRequest,
    ApiRegistryCatchupRequestEnvelope,
    ApiRegistryCatchupResult,
)

logger = logging.getLogger(__name__)


async def catch_up_until_matched(
    broker: Broker, instance: str, stop: asyncio.Event
) -> None:
    """Request a reconcile until the API reports this disk matches."""
    delay = 0.2
    while not stop.is_set():
        try:
            reply = await broker.request(
                API_REGISTRY_CATCHUP,
                ApiRegistryCatchupRequestEnvelope.wrap(
                    ApiRegistryCatchupRequest(instance=instance),
                    type=API_REGISTRY_CATCHUP,
                    source="sts",
                ),
                timeout=60,
            )
        except Exception as exc:
            logger.warning(
                "registry catch-up: API did not answer (%s); retrying", exc
            )
        else:
            result = ApiRegistryCatchupResult.model_validate(reply.payload)
            if result.ok:
                logger.info(
                    "registry catch-up: %s matches the API store", instance
                )
                return
            logger.warning("registry catch-up: %s", result.error)
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
            return
        except TimeoutError:
            delay = min(delay * 2, 30)
