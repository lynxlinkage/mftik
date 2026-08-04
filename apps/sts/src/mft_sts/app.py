"""STS process bootstrap — RPC, independent sessions, heartbeat."""

from __future__ import annotations

import asyncio
import logging
import signal

from mft import configure_logging
from mft.broker import Broker
from mft.protocol import Topics

from mft_sts import db as sts_db
from mft_sts.rpc import dispatch
from mft_sts.session import SessionManager

SOURCE = "sts"
logger = logging.getLogger(SOURCE)


async def run_rpc(
    broker: Broker,
    sessions: SessionManager,
    stop: asyncio.Event,
) -> None:
    logger.info("STS RPC listening on subject=%s", Topics.STS)
    async for req in broker.serve(Topics.STS, stop=stop):
        try:
            await dispatch(req, sessions=sessions)
        except Exception:
            logger.exception(
                "STS RPC handler failed type=%s id=%s",
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
        sessions = SessionManager(
            broker,
            persist_live=sts_db.persist_live_session,
            mark_done=sts_db.mark_session_finished,
            list_db_sessions=sts_db.list_sessions,
        )
        logger.info("STS started")
        rpc_task = asyncio.create_task(
            run_rpc(broker, sessions, stop), name="sts-rpc"
        )
        hb_task = asyncio.create_task(
            broker.heartbeat_loop(
                SOURCE,
                interval=5.0,
                stop=stop,
                on_tick=lambda: logger.debug("heartbeat"),
            ),
            name="sts-sys-heartbeat",
        )
        try:
            await stop.wait()
        finally:
            stop.set()
            for task in (rpc_task, hb_task):
                task.cancel()
            await asyncio.gather(rpc_task, hb_task, return_exceptions=True)
            await sessions.close_all()
    logger.info("STS stopped")


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(amain())
