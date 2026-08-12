"""TD process bootstrap — RPC, venue session factory, heartbeat."""

from __future__ import annotations

import asyncio
import logging
import signal

from mft import configure_logging
from mft.broker import Broker
from mft.protocol import Topics

from mft_td import db as td_db
from mft_td.rpc import dispatch
from mft_td.session import SessionManager, VenueSessionFactory

SOURCE = "td"
logger = logging.getLogger(SOURCE)


async def run_rpc(
    broker: Broker,
    sessions: SessionManager,
    stop: asyncio.Event,
) -> None:
    """Serve API→TD request-reply on ``Topics.TD`` until ``stop``."""
    logger.info("TD RPC listening on subject=%s", Topics.TD)
    async for req in broker.serve(Topics.TD, stop=stop):
        try:
            await dispatch(req, sessions=sessions)
        except Exception:
            logger.exception(
                "TD RPC handler failed type=%s id=%s",
                req.envelope.type,
                req.envelope.id,
            )


#: How often to look for attaches whose strategy is gone. Well under the
#: window someone would spend wondering why an api_id still reports a session
#: nobody is running, and far enough above the liveness TTL that a key is
#: never checked mid-refresh.
REAP_INTERVAL_SECONDS = 60.0


async def reap_loop(
    sessions: SessionManager,
    stop: asyncio.Event,
    *,
    interval: float = REAP_INTERVAL_SECONDS,
) -> None:
    """Scan for orphaned attaches on boot, then on a slow interval.

    On boot because rows outlive the process that wrote them, and whatever
    replaces it is the first thing in a position to notice; on an interval
    because a lease loop can stop without the process doing so, and nobody
    should have to restart TD to find out.
    """
    while not stop.is_set():
        try:
            closed = await sessions.reap_orphans()
            if closed:
                logger.warning("TD reaped %d orphaned attach(es)", len(closed))
        except Exception:
            logger.exception("TD orphan reaper failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue


async def amain() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with Broker() as broker:
        # Venue comes from the apis row: paper goes to the paper-engine
        # container, Gate connects to the venue directly.
        factory = VenueSessionFactory(broker, load_api=td_db.get_api)
        sessions = SessionManager(
            factory,
            broker,
            persist_live=td_db.persist_live_session,
            mark_done=td_db.mark_session_done,
            list_db_sessions=td_db.list_sessions,
        )
        logger.info("TD started (venue session factory)")
        rpc_task = asyncio.create_task(
            run_rpc(broker, sessions, stop), name="td-rpc"
        )
        hb_task = asyncio.create_task(
            broker.heartbeat_loop(
                SOURCE,
                interval=5.0,
                stop=stop,
                on_tick=lambda: logger.debug("heartbeat"),
            ),
            name="td-heartbeat",
        )
        reaper_task = asyncio.create_task(
            reap_loop(sessions, stop), name="td-reaper"
        )
        try:
            await stop.wait()
        finally:
            stop.set()
            for task in (rpc_task, hb_task, reaper_task):
                task.cancel()
            await asyncio.gather(
                rpc_task, hb_task, reaper_task, return_exceptions=True
            )
            await sessions.close_all()
    logger.info("TD stopped")


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(amain())
