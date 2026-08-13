"""Asking for a backfill — from a schedule, a detach, or a process going down.

Three callers, one envelope, one subject. They differ only in what they are
worth, and that difference is the whole design:

* **The schedule is the guarantee.** If it stops running, settlement lines stop
  moving and the dashboard is left calling everything provisional — which is
  correct, and is why a stalled cron is worth an alert.
* **A detach and a shutdown are latency.** They exist so the record settles
  soon after the moment somebody wants to read it, not so it settles at all.

That ranking is what lets the two hooks be fire-and-forget. They post rather
than request — nobody waits on a walk that takes minutes, least of all a
teardown path measured in seconds — and a post that fails is logged and
dropped, because the schedule will ask again anyway. Only the schedule's own
failure is worth more than a log line.

A posted request left in the list because nothing is serving it yet is not
lost: the next TD to come up takes it. A shutdown asking for a backfill it
cannot itself run is the case that relies on this, and it is the right
behaviour rather than a leak — the account was just traded, so its record is
exactly the one worth repairing when something comes back.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

from mft.broker import Broker
from mft.protocol import TD_BACKFILL, Envelope, TdBackfill, Topics

logger = logging.getLogger(__name__)

#: How long a teardown may spend asking. Generous for an ``RPUSH`` and short
#: enough that an unreachable Redis cannot hold a container past its stop
#: timeout and turn a clean exit into a kill.
POST_TIMEOUT_S = 3.0


async def request_backfill(
    broker: Broker,
    api_id: int,
    *,
    reason: str,
    tickers: Sequence[str] = (),
    timeout: float = POST_TIMEOUT_S,
) -> bool:
    """Ask for a backfill of ``api_id``. Never raises; True if it was posted.

    Best-effort by design. Every caller of this has something more important
    to be doing — stopping a session, shutting a process down, moving on to
    the next account — and none of them is the reason the record eventually
    settles.
    """
    envelope = Envelope[TdBackfill].wrap(
        TdBackfill(api_id=api_id, tickers=list(tickers), reason=reason),
        type=TD_BACKFILL,
        source="td",
    )
    try:
        await asyncio.wait_for(
            broker.post(Topics.td_backfill(), envelope), timeout=timeout
        )
    except TimeoutError:
        logger.warning(
            "TD backfill request timed out api_id=%s reason=%s", api_id, reason
        )
        return False
    except asyncio.CancelledError:
        # Propagated, never swallowed: this being best-effort is about Redis
        # being unwell, not about ignoring a caller that asked us to stop.
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


__all__ = ["POST_TIMEOUT_S", "request_backfill"]
