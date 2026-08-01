"""TD process bootstrap — RPC, paper session factory, heartbeat."""

from __future__ import annotations

import asyncio
import logging
import signal

from mft import configure_logging
from mft.broker import Broker
from mft.protocol import Topics

from mft_td import db as td_db
from mft_td.rpc import dispatch
from mft_td.session import PaperSessionFactory, SessionManager

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


async def amain() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with Broker() as broker:
        # Paper venue lives in the paper-engine container; TD is a remote client.
        factory = PaperSessionFactory(broker)
        sessions = SessionManager(
            factory,
            broker,
            persist_live=td_db.persist_live_session,
            mark_done=td_db.mark_session_done,
            list_db_sessions=td_db.list_sessions,
        )
        logger.info("TD started (remote paper session factory)")
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
        try:
            await stop.wait()
        finally:
            stop.set()
            for task in (rpc_task, hb_task):
                task.cancel()
            await asyncio.gather(rpc_task, hb_task, return_exceptions=True)
            await sessions.close_all()
    logger.info("TD stopped")


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(amain())
