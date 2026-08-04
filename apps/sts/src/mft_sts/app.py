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


#: How often to look for sessions whose process died. Well under the window
#: someone would spend wondering why a strategy is not doing anything, and
#: far enough above the liveness TTL that a key is never checked mid-refresh.
REAP_INTERVAL_SECONDS = 60.0


async def reap_loop(
    sessions: SessionManager,
    stop: asyncio.Event,
    *,
    interval: float = REAP_INTERVAL_SECONDS,
) -> None:
    """Scan for orphaned sessions on boot, then on a slow interval.

    On boot because a crash is most often noticed by whatever replaces the
    process; on an interval because a crash with no restart still leaves rows
    claiming to be running, and nobody should have to restart STS to find out.
    """
    while not stop.is_set():
        try:
            reaped = await sessions.reap_orphans()
            if reaped:
                logger.warning("STS reaped %d orphaned session(s)", len(reaped))
        except Exception:
            logger.exception("STS orphan reaper failed")
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
        reaper_task = asyncio.create_task(
            reap_loop(sessions, stop), name="sts-reaper"
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
    logger.info("STS stopped")


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(amain())
