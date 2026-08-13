"""The schedule that makes the settlement line a guarantee rather than a hope.

Two other things ask for backfills — a strategy detaching, a TD shutting down —
and both are latency: they settle the record soon after somebody wants to read
it. Neither is *why* it settles. A run may be skipped, a process may die before
it asks, a detach may never happen because nothing ever detached cleanly. This
loop is the one that has to keep running, and a stalled one is worth noticing:
the visible symptom is a dashboard that calls everything provisional forever,
which is correct but useless.

It lives in the API for the same reason the log persister does — the process is
always up, and it holds no venue credentials. It only *asks*: the walk itself
runs in TD, which is where the credentials and the connectors are. What travels
between them is one envelope on an unkeyed subject, so an account nobody is
trading is swept exactly like one somebody is.
"""

from __future__ import annotations

import asyncio
import logging
import os

from mft.broker import Broker
from mft.protocol import TD_BACKFILL, Envelope, TdBackfill, Topics
from mft_db.repositories import OrderRepository
from mft_db.session import session_scope

logger = logging.getLogger("mft_api.backfill_cron")

#: How often to sweep. Well under the window in which somebody would go looking
#: for a session's final number, and far enough above a walk's own duration
#: that a slow account is never being asked about twice at once.
DEFAULT_INTERVAL_S = 900.0

#: Between accounts. The sweep is not in a hurry and the venues are shared with
#: whatever is trading right now.
ACCOUNT_PAUSE_S = 0.5


def _interval() -> float:
    return max(30.0, float(os.getenv("BACKFILL_INTERVAL", str(DEFAULT_INTERVAL_S))))


async def accounts_to_sweep() -> list[int]:
    async with session_scope() as db:
        return await OrderRepository(db).api_ids_with_history()


async def sweep(broker: Broker, *, reason: str = "cron") -> int:
    """Ask for a backfill of every account with history. Returns how many.

    Posts rather than requests: a walk is minutes of venue round trips and
    there is nothing here that would do anything with the answer. TD replies to
    nobody, and the result of the run is the cursor it moved.
    """
    api_ids = await accounts_to_sweep()
    for api_id in api_ids:
        await broker.post(
            Topics.td_backfill(),
            Envelope[TdBackfill].wrap(
                TdBackfill(api_id=api_id, reason=reason),
                type=TD_BACKFILL,
                source="api",
            ),
        )
        if ACCOUNT_PAUSE_S:
            await asyncio.sleep(ACCOUNT_PAUSE_S)
    return len(api_ids)


async def run_backfill_cron(
    stop: asyncio.Event, *, interval: float | None = None
) -> None:
    """Sweep on an interval until ``stop``.

    A failed sweep is logged and the loop goes on: the next tick asks again,
    and the cursors an unasked account keeps are the truthful ones — behind,
    and saying so.
    """
    every = interval if interval is not None else _interval()
    broker = Broker()
    await broker.connect()
    logger.info("backfill cron started (every %.0fs)", every)
    try:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=every)
            except TimeoutError:
                pass
            if stop.is_set():
                break
            try:
                asked = await sweep(broker)
                logger.info("backfill cron asked for %d account(s)", asked)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("backfill cron sweep failed")
    finally:
        await broker.close()
        logger.info("backfill cron stopped")


__all__ = [
    "ACCOUNT_PAUSE_S",
    "DEFAULT_INTERVAL_S",
    "accounts_to_sweep",
    "run_backfill_cron",
    "sweep",
]
