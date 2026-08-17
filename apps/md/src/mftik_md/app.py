"""MD process bootstrap — RPC, paper public factory, heartbeat."""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from mftik import configure_logging
from mftik.broker import Broker
from mftik.exchange import venues
from mftik.protocol import Topics
from mftik.symbols import SymbolClient

from mftik_md import db as md_db
from mftik_md.fetch import FetchSession, VenueReaderFactory
from mftik_md.publish_track import ROLE_MIRROR, ROLE_PRIMARY, PublishTracker
from mftik_md.rpc import dispatch
from mftik_md.session import SessionManager, VenuePublicFactory
from mftik_md.tape import (
    DEFAULT_MAXLEN,
    DEFAULT_RETENTION_S,
    DEFAULT_TOPICS,
    TapeRecorder,
)

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


#: How often the retention window is applied. Far below it, so a tape is never
#: much longer than it claims, and far above the cost of one XTRIM per feed.
TRIM_INTERVAL_SECONDS = 60.0


def _build_recorder(broker: Broker) -> TapeRecorder | None:
    """Configure tape recording from the environment.

    On by default: recording is what makes a warm-up possible at all, and a
    strategy that needs one cannot add it after the fact — the history it wants
    is the history nobody was keeping. ``MD_TAPE_TOPICS=`` (empty) turns it off
    for a deployment that would rather not spend the memory.
    """
    raw = os.getenv("MD_TAPE_TOPICS")
    if raw is None:
        topics = list(DEFAULT_TOPICS)
    else:
        topics = [part.strip() for part in raw.split(",") if part.strip()]
    if not topics:
        logger.info("MD tape recording disabled (MD_TAPE_TOPICS is empty)")
        return None

    def _number(name: str, fallback: float) -> float:
        text = os.getenv(name, "").strip()
        if not text:
            return fallback
        try:
            return float(text)
        except ValueError:
            logger.warning(
                "ignoring %s=%r — not a number, using %s", name, text, fallback
            )
            return fallback

    retention_s = _number("MD_TAPE_RETENTION_S", DEFAULT_RETENTION_S)
    maxlen = int(_number("MD_TAPE_MAXLEN", DEFAULT_MAXLEN))
    logger.info(
        "MD tape recording topics=%s retention=%.0fs maxlen=%d",
        topics,
        retention_s,
        maxlen,
    )
    return TapeRecorder(
        broker, topics=topics, maxlen=maxlen, retention_s=retention_s
    )


async def trim_loop(
    sessions: SessionManager,
    stop: asyncio.Event,
    *,
    interval: float = TRIM_INTERVAL_SECONDS,
) -> None:
    """Hold every recording feed to its retention window."""
    while not stop.is_set():
        try:
            await sessions.trim_tapes()
        except Exception:
            logger.exception("MD tape trim failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue


def _mirror_enabled() -> bool:
    return os.getenv("MD_MIRROR", "").strip().lower() in {"1", "true", "yes"}


async def amain() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    mirror = _mirror_enabled()
    async with Broker() as broker:
        factory = VenuePublicFactory(broker)
        tracker = PublishTracker(
            broker, role=ROLE_MIRROR if mirror else ROLE_PRIMARY
        )
        sessions = SessionManager(
            factory,
            broker,
            persist_live=None if mirror else md_db.persist_live_session,
            mark_done=None if mirror else md_db.mark_session_done,
            list_db_sessions=None if mirror else md_db.list_sessions,
            recorder=_build_recorder(broker),
            tracker=tracker,
            stamp_coverage=not mirror,
        )
        if mirror:
            live = await md_db.list_live_sts_feeds()
            await sessions.pin_live_sessions(live)
        else:
            await tracker.reset()
        # Up for as long as the process is, and attached to nothing. A read
        # is owned by nobody, so the fetch plane needs no lease and no
        # subscription to answer — which is the whole point of it being
        # separate from the feed sessions above.
        fetch = FetchSession(broker, VenueReaderFactory(SymbolClient(broker)))
        await fetch.start()
        logger.info(
            "MD started (venue public factory: %s%s)",
            venues.names(),
            ", mirror" if mirror else "",
        )
        tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(
                broker.heartbeat_loop(
                    SOURCE,
                    interval=5.0,
                    stop=stop,
                    on_tick=lambda: logger.debug("heartbeat"),
                ),
                name="md-heartbeat",
            ),
            asyncio.create_task(
                trim_loop(sessions, stop), name="md-tape-trim"
            ),
        ]
        # The sidecar must not take attach RPC: a rebuild landing here would
        # die when the updater stops this container.
        if not mirror:
            tasks.append(
                asyncio.create_task(
                    run_rpc(broker, sessions, stop), name="md-rpc"
                )
            )
            tasks.append(
                asyncio.create_task(
                    reap_loop(sessions, stop), name="md-reaper"
                )
            )
        try:
            await stop.wait()
        finally:
            stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await fetch.stop()
            await sessions.close_all()
    logger.info("MD stopped")


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(amain())
