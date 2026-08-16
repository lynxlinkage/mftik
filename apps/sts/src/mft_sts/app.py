"""STS process bootstrap — RPC, independent sessions, heartbeat."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import Any

from mft import configure_logging
from mft.broker import Broker
from mft.protocol import Topics

from mft_sts import db as sts_db
from mft_sts.impl import load_local_registry
from mft_sts.rpc import dispatch
from mft_sts.session import SessionManager

SOURCE = "sts"
logger = logging.getLogger(SOURCE)

#: Mirrors the manager's own default; kept here so the env override has
#: something to fall back to without importing a private name.
_DEFAULT_REBUILD_MAX_AGE_S = 1800.0


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


def _rebuild_enabled() -> bool:
    """Whether to restore interrupted sessions on boot.

    Off by default. Restoring a session puts a strategy back in front of a
    live account, and until at least one strategy has implemented
    ``on_rebuild`` that is a strategy which does not know it was ever away.
    Opt in per deployment with ``STS_REBUILD_ON_BOOT=1``.
    """
    return os.getenv("STS_REBUILD_ON_BOOT", "0").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _rebuild_max_age_s() -> float:
    """How stale an interrupted session may be and still be restored.

    Sized for a restart, where the gap is seconds to minutes. Widen it with
    ``STS_REBUILD_MAX_AGE_S`` if a deploy routinely takes longer than the
    default; do not widen it to cover sessions nobody meant to resume.
    """
    raw = os.getenv("STS_REBUILD_MAX_AGE_S", "").strip()
    if not raw:
        return _DEFAULT_REBUILD_MAX_AGE_S
    try:
        return float(raw)
    except ValueError:
        logger.warning(
            "ignoring STS_REBUILD_MAX_AGE_S=%r — not a number, using %.0fs",
            raw,
            _DEFAULT_REBUILD_MAX_AGE_S,
        )
        return _DEFAULT_REBUILD_MAX_AGE_S


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


async def _rebuild_on_boot(sessions: SessionManager) -> None:
    try:
        rebuilt = await sessions.rebuild_interrupted()
    except Exception:
        logger.exception("STS rebuild on boot failed")
        return
    if rebuilt:
        logger.warning("STS rebuilt %d interrupted session(s)", len(rebuilt))
    else:
        logger.info("STS found no interrupted sessions to rebuild")


async def amain() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with Broker() as broker:
        loaded = load_local_registry()
        if loaded:
            logger.info(
                "STS loaded %d registry strategy(ies): %s",
                len(loaded),
                ", ".join(loaded),
            )
        sessions = SessionManager(
            broker,
            persist_live=sts_db.persist_live_session,
            mark_done=sts_db.mark_session_finished,
            list_db_sessions=sts_db.list_sessions,
            remember_fact=sts_db.remember_fact,
            mark_live=sts_db.mark_session_live,
            bump_rebuild_count=sts_db.bump_rebuild_count,
            rebuild_max_age_s=_rebuild_max_age_s(),
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
        if _rebuild_enabled():
            # A task, not awaited: rebuilding waits on TD and MD, which may
            # not be up yet, and RPC service must not be held up behind it.
            rebuild_task: asyncio.Task[Any] | None = asyncio.create_task(
                _rebuild_on_boot(sessions), name="sts-rebuild"
            )
        else:
            rebuild_task = None
            logger.info(
                "STS rebuild on boot disabled (set STS_REBUILD_ON_BOOT=1)"
            )
        logger.info(
            "STS rebuild window is %.0fs", _rebuild_max_age_s()
        )
        try:
            await stop.wait()
        finally:
            stop.set()
            tasks = [rpc_task, hb_task, reaper_task]
            if rebuild_task is not None:
                tasks.append(rebuild_task)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await sessions.close_all()
    logger.info("STS stopped")


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(amain())
