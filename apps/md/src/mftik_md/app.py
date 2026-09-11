"""MD process bootstrap — RPC, paper public factory, heartbeat."""

from __future__ import annotations

import asyncio
import logging
import os
import signal

import uvloop
from mftik import (
    InstanceAlreadyServing,
    configure_logging,
    control_subjects,
    instance_name,
    instance_role,
    refuse_if_serving,
    run_until_stopped,
    serve_health,
)
from mftik.broker import Broker
from mftik.exchange import venues
from mftik.symbols import SymbolClient

from mftik_md import db as md_db
from mftik_md.fetch import FetchSession, VenueReaderFactory
from mftik_md.rpc import dispatch
from mftik_md.session import SessionManager, VenuePublicFactory
from mftik_md.tape import (
    DEFAULT_MAXLEN,
    DEFAULT_RETENTION_S,
    DEFAULT_TOPICS,
    TapeRecorder,
)
from mftik_md.tape_store import TapeStore

SOURCE = "md"
#: Which MD this process is. ``MFTIK_INSTANCE``, defaulting to the
#: plane name — so an unconfigured deployment is the instance called
#: ``md``, which migration 0031 declares. Nothing routes on it yet.
INSTANCE = instance_name(SOURCE)
#: How much of the plane this process answers for. ``MFTIK_ROLE``,
#: defaulting to ``active`` — it serves its own subject and the shared
#: pool, which is what every deployment did before instances existed.
#: Raises at import on a value this plane cannot hold, so a bad setting
#: is a boot failure with a sentence rather than a process that comes up
#: answering nothing.
ROLE = instance_role(SOURCE)
logger = logging.getLogger(SOURCE)

#: How long a serve loop waits before rebuilding itself after an exception it
#: did not expect. ``Broker.serve`` already survives what it knows how to
#: survive, so this only paces the failures nothing has a name for yet.
RPC_RESTART_DELAY_SECONDS = 1.0



async def run_rpc(
    broker: Broker,
    sessions: SessionManager,
    stop: asyncio.Event,
    *,
    subject: str,
) -> None:
    """Serve MD request-reply on ``subject`` until ``stop``.

    One task per subject the role grants, rather than one loop over several:
    each is the same loop with a different name, and a failure in one is not a
    reason to stop answering on the other.
    """
    logger.info("MD RPC listening on subject=%s", subject)
    while not stop.is_set():
        try:
            async for req in broker.serve(subject, stop=stop):
                try:
                    await dispatch(req, sessions=sessions)
                except Exception:
                    logger.exception(
                        "MD RPC handler failed type=%s id=%s",
                        req.envelope.type,
                        req.envelope.id,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Reaching here means something ``serve`` does not already handle,
            # and the answer is still to serve. This coroutine returning is how
            # MD ends up running sessions that nobody can list, pause or stop
            # — the process alive, the subject silent, and no line anywhere
            # saying so.
            logger.exception(
                "MD RPC serve loop failed subject=%s — restarting", subject
            )
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=RPC_RESTART_DELAY_SECONDS
                )
            except TimeoutError:
                continue


#: How often to look for rows whose MD process died. Well under the window
#: someone would spend wondering why a session claims a feed that is not
#: running, and far enough above two reap scans that a row between persist
#: and ``_links`` is not closed on the first look.
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
#: much longer than it claims, and far above the cost of one trim per feed.
TRIM_INTERVAL_SECONDS = 60.0


def _build_recorder() -> TapeRecorder | None:
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

    url = os.getenv("REDIS_URL", "").strip()
    if not url:
        logger.warning(
            "MD tape recording disabled (REDIS_URL is unset) — live "
            "fan-out is unaffected; warm-up reads will be empty"
        )
        return None

    retention_s = _number("MD_TAPE_RETENTION_S", DEFAULT_RETENTION_S)
    maxlen = int(_number("MD_TAPE_MAXLEN", DEFAULT_MAXLEN))
    logger.info(
        "MD tape recording topics=%s retention=%.0fs maxlen=%d redis=%s",
        topics,
        retention_s,
        maxlen,
        url,
    )
    return TapeRecorder(
        TapeStore.from_url(url),
        topics=topics,
        maxlen=maxlen,
        retention_s=retention_s,
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


async def amain() -> bool:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with Broker() as broker:
        try:
            await refuse_if_serving(broker, domain=SOURCE, instance=INSTANCE)
        except InstanceAlreadyServing as exc:
            logger.error("%s", exc)
            return False
        factory = VenuePublicFactory(broker)
        sessions = SessionManager(
            factory,
            broker,
            persist_live=md_db.persist_live_session,
            mark_done=md_db.mark_session_done,
            list_db_sessions=md_db.list_sessions,
            recorder=_build_recorder(),
            instance=INSTANCE,
        )
        if sessions.tape_store is not None:
            await sessions.tape_store.ping()
        # Up for as long as the process is, and attached to nothing. A read
        # is owned by nobody, so the fetch plane needs no lease and no
        # subscription to answer — which is the whole point of it being
        # separate from the feed sessions above.
        fetch = FetchSession(broker, VenueReaderFactory(SymbolClient(broker)))
        await fetch.start()
        logger.info(
            "MD started instance=%s (venue public factory: %s)",
            INSTANCE,
            venues.names(),
        )
        subjects = control_subjects(SOURCE, INSTANCE, ROLE)
        if not subjects:
            logger.warning(
                "MD is %s and serves no control subject — it holds what it "
                "has and takes nothing new",
                ROLE.value,
            )
        rpc_tasks = [
            asyncio.create_task(
                run_rpc(broker, sessions, stop, subject=subject),
                name=f"md-rpc-{subject}",
            )
            for subject in subjects
        ]
        hb_task = asyncio.create_task(
            broker.heartbeat_loop(
                SOURCE,
                interval=5.0,
                stop=stop,
                on_tick=lambda: logger.debug("heartbeat"),
            ),
            name="md-heartbeat",
        )
        # A standby MD has nothing to reap and a peer that does — see
        # ``Role.runs_reaper``.
        reaper_task = (
            asyncio.create_task(reap_loop(sessions, stop), name="md-reaper")
            if ROLE.runs_reaper
            else None
        )
        trim_task = asyncio.create_task(
            trim_loop(sessions, stop), name="md-tape-trim"
        )
        health_task = asyncio.create_task(
            serve_health(
                broker,
                domain=SOURCE,
                instance=INSTANCE,
                stop=stop,
                # Which venues this MD can reach. A deploy naming a feed on a
                # venue it cannot serve should fail at deploy rather than at
                # subscribe, and this is where that answer comes from.
                describe=lambda: {"venues": sorted(venues.names())},
            ),
            name="md-health",
        )
        try:
            clean = await run_until_stopped(
                stop,
                *rpc_tasks,
                hb_task,
                *( [reaper_task] if reaper_task is not None else [] ),
                trim_task,
                health_task,
                logger=logger,
            )
        finally:
            stop.set()
            tasks = [
                *rpc_tasks,
                hb_task,
                trim_task,
                health_task,
                *( [reaper_task] if reaper_task is not None else [] ),
            ]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await fetch.stop()
            await sessions.close_all()
    logger.info("MD stopped")
    return clean


def main() -> None:
    configure_logging(SOURCE)
    # Non-zero when a long-lived task ended on its own: the restart
    # policy is what puts the process back, and an exit code is what
    # tells anyone reading ``docker ps`` that MD did not just stop.
    #
    # ``uvloop.run`` rather than ``asyncio.run`` — docs/EventLoop.md has the
    # measurements. It builds the loop for this one call and leaves the global
    # policy alone, so the loop this process runs is stated here rather than
    # inherited from whatever an import happened to install.
    if not uvloop.run(amain()):
        raise SystemExit(1)
