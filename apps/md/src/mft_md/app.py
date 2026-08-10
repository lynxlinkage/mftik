"""MD process bootstrap — RPC, paper public factory, heartbeat."""

from __future__ import annotations

import asyncio
import logging
import signal

from mft import configure_logging
from mft.broker import Broker
from mft.exchange import venues
from mft.protocol import Topics
from mft.symbols import SymbolClient

from mft_md import db as md_db
from mft_md.fetch import FetchSession, VenueReaderFactory
from mft_md.rpc import dispatch
from mft_md.session import SessionManager, VenuePublicFactory

SOURCE = "md"
logger = logging.getLogger(SOURCE)


async def run_rpc(
    broker: Broker,
    sessions: SessionManager,
    stop: asyncio.Event,
) -> None:
    """Serve API→MD request-reply on ``Topics.MD`` until ``stop``."""
    logger.info("MD RPC listening on subject=%s", Topics.MD)
    async for req in broker.serve(Topics.MD, stop=stop):
        try:
            await dispatch(req, sessions=sessions)
        except Exception:
            logger.exception(
                "MD RPC handler failed type=%s id=%s",
                req.envelope.type,
                req.envelope.id,
            )


#: How often to look for rows whose MD process died. Well under the window
#: someone would spend wondering why a session claims a feed that is not
#: running, and far enough above the liveness TTL that a key is never
#: checked mid-refresh.
REAP_INTERVAL_SECONDS = 60.0


async def reap_loop(
    sessions: SessionManager,
    stop: asyncio.Event,
    *,
    interval: float = REAP_INTERVAL_SECONDS,
) -> None:
    """Scan for orphaned attach rows on boot, then on a slow interval.

    On boot because a crash is most often noticed by whatever replaces the
    process; on an interval because a crash with no restart still leaves
    rows claiming a feed is up, and nobody should have to restart MD to find
    out.
    """
    while not stop.is_set():
        try:
            reaped = await sessions.reap_orphans()
            if reaped:
                logger.warning(
                    "MD reaped %d orphaned session(s)", len(reaped)
                )
        except Exception:
            logger.exception("MD orphan reaper failed")
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
        factory = VenuePublicFactory(broker)
        sessions = SessionManager(
            factory,
            broker,
            persist_live=md_db.persist_live_session,
            mark_done=md_db.mark_session_done,
            list_db_sessions=md_db.list_sessions,
        )
        # Up for as long as the process is, and attached to nothing. A read
        # is owned by nobody, so the fetch plane needs no lease and no
        # subscription to answer — which is the whole point of it being
        # separate from the feed sessions above.
        fetch = FetchSession(broker, VenueReaderFactory(SymbolClient(broker)))
        await fetch.start()
        logger.info("MD started (venue public factory: %s)", venues.names())
        rpc_task = asyncio.create_task(
            run_rpc(broker, sessions, stop), name="md-rpc"
        )
        hb_task = asyncio.create_task(
            broker.heartbeat_loop(
                SOURCE,
                interval=5.0,
                stop=stop,
                on_tick=lambda: logger.debug("heartbeat"),
            ),
            name="md-heartbeat",
        )
        reaper_task = asyncio.create_task(
            reap_loop(sessions, stop), name="md-reaper"
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
            await fetch.stop()
            await sessions.close_all()
    logger.info("MD stopped")


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(amain())
