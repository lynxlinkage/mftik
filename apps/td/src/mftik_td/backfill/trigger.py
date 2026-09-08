"""Asking for a backfill — from a schedule or a detach.

The schedule is the guarantee. A detach is latency: it exists so the record
settles soon after somebody wants to read it, not so it settles at all.

That ranking is what lets the hook be fire-and-forget. It asks rather than
waits out the walk — TD acks as soon as it accepts — and a request that
fails is logged and dropped, because the schedule will ask again anyway.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

from mftik.broker import Broker, RequestTimeoutError
from mftik.protocol import TD_BACKFILL, Envelope, TdBackfill, Topics

logger = logging.getLogger(__name__)

#: How long a teardown may spend asking. Short enough that an unreachable
#: broker cannot hold a container past its stop timeout.
REQUEST_TIMEOUT_S = 3.0


async def request_backfill(
    broker: Broker,
    api_id: int,
    *,
    instance: str,
    reason: str,
    tickers: Sequence[str] = (),
    timeout: float = REQUEST_TIMEOUT_S,
) -> bool:
    """Ask for a backfill of ``api_id``. Never raises; True if TD accepted.

    Best-effort by design. Every caller of this has something more important
    to be doing — stopping a session, moving on to the next account — and
    none of them is the reason the record eventually settles.
    """
    envelope = Envelope[TdBackfill].wrap(
        TdBackfill(api_id=api_id, tickers=list(tickers), reason=reason),
        type=TD_BACKFILL,
        source="td",
    )
    try:
        await asyncio.wait_for(
            broker.request(
                Topics.td_backfill(instance), envelope, timeout=timeout
            ),
            timeout=timeout,
        )
    except RequestTimeoutError:
        logger.warning(
            "TD backfill request timed out api_id=%s reason=%s", api_id, reason
        )
        return False
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.warning(
            "TD backfill request failed api_id=%s reason=%s",
            api_id,
            reason,
            exc_info=True,
        )
        return False
    logger.debug("TD backfill requested api_id=%s reason=%s", api_id, reason)
    return True


# Kept so existing imports keep working.
POST_TIMEOUT_S = REQUEST_TIMEOUT_S

__all__ = ["POST_TIMEOUT_S", "REQUEST_TIMEOUT_S", "request_backfill"]
