"""MD process bootstrap — RPC, paper public factory, heartbeat."""

from __future__ import annotations

import asyncio
import logging
import signal

from mft import configure_logging
from mft.broker import Broker
from mft.exchange import venues
from mft.protocol import Topics

from mft_md import db as md_db
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
        try:
            await stop.wait()
        finally:
            stop.set()
            for task in (rpc_task, hb_task):
                task.cancel()
            await asyncio.gather(rpc_task, hb_task, return_exceptions=True)
            await sessions.close_all()
    logger.info("MD stopped")


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(amain())
