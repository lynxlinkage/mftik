"""STS process bootstrap — RPC, independent sessions, heartbeat."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import Any

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
from mftik.protocol import STS_SESSION_CREATE, STS_SESSION_FORCE_STOP, Topics
from mftik.strategy.artifacts import get_store
from mftik_db.schema import SchemaTooOld, require_sts_schema

from mftik_sts import db as sts_db
from mftik_sts.registry_catchup import catch_up_until_matched
from mftik_sts.rpc import dispatch
from mftik_sts.runtime_env import extras_names, refresh
from mftik_sts.session import SessionManager
from mftik_sts.spawn import SubprocessSpawner

SOURCE = "sts"
#: Which STS this process is. ``MFTIK_INSTANCE``, defaulting to the
#: plane name — so an unconfigured deployment is the instance called
#: ``sts``, which migration 0031 declares. Nothing routes on it yet.
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


async def _dispatch_request(req: Any, sessions: SessionManager) -> None:
    try:
        await dispatch(req, sessions=sessions)
    except Exception:
        logger.exception(
            "STS RPC handler failed type=%s id=%s",
            req.envelope.type,
            req.envelope.id,
        )


async def run_rpc(
    broker: Broker,
    sessions: SessionManager,
    stop: asyncio.Event,
    *,
    subject: str,
) -> None:
    """Serve STS request-reply on ``subject`` until ``stop``.

    One task per subject the role grants, rather than one loop over several:
    each is the same loop with a different name, and a failure in one is not a
    reason to stop answering on the other.
    """
    logger.info("STS RPC listening on subject=%s", subject)
    while not stop.is_set():
        try:
            async for req in broker.serve(subject, stop=stop):
                # Create waits on the worker's result line. Force-stop waits
                # out the kill and the row write. Awaiting either here would
                # hold every other RPC on this subject — list, artifacts,
                # another session's stop — for that whole time.
                # The API's own timeout is unchanged. If a create timeout
                # already fired and the worker later reports success, the
                # session stays live and the deploy has not attached it.
                # This process does not kill that worker and does not mark
                # the row failed.
                if req.envelope.type == STS_SESSION_CREATE:
                    task = asyncio.create_task(
                        _dispatch_request(req, sessions),
                        name="sts-rpc-create",
                    )
                    sessions.track_create(task)
                    continue
                if req.envelope.type == STS_SESSION_FORCE_STOP:
                    task = asyncio.create_task(
                        _dispatch_request(req, sessions),
                        name="sts-rpc-force-stop",
                    )
                    sessions.track_escalation(task)
                    continue
                await _dispatch_request(req, sessions)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Reaching here means something ``serve`` does not already handle,
            # and the answer is still to serve. This coroutine returning is how
            # STS ends up running sessions that nobody can list, pause or stop
            # — the process alive, the subject silent, and no line anywhere
            # saying so.
            logger.exception(
                "STS RPC serve loop failed subject=%s — restarting", subject
            )
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=RPC_RESTART_DELAY_SECONDS
                )
            except TimeoutError:
                continue


#: How often to look for sessions this instance owns and does not hold.
#: Well under the window someone would spend wondering why a strategy is
#: not doing anything, and far enough above two reap scans that a row
#: between persist and ``_sessions`` is not closed on the first look.
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
            # An upload nobody committed — the API died, the laptop closed —
            # leaves a part file. Hidden from listings, and not an object.
            swept = await asyncio.to_thread(get_store().sweep_parts)
            if swept:
                logger.info("STS swept %d idle artifact upload(s)", swept)
        except Exception:
            logger.exception("STS artifact sweep failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue


#: How long boot waits for the database to say it has run the migrations this
#: build needs, before giving up and exiting.
#:
#: Bounded, and then fatal. The compose stack starts STS beside Postgres and
#: the one-shot migration step without ordering them, so "not listening yet",
#: "no tables yet" and "one revision short" are all states a cold start sees
#: for its first seconds — each of which becomes the right answer on its own,
#: given a moment. None of them is a reason to serve: a session created
#: against the old schema is not written again when the migration lands.
#:
#: Wide enough for Postgres to pass its healthcheck (up to ~50s in that
#: stack) and for a cold database to run the whole migration history behind
#: it. ``STS_SCHEMA_WAIT_S`` widens it for a node where that takes longer.
SCHEMA_WAIT_S = 180.0
SCHEMA_WAIT_ENV = "STS_SCHEMA_WAIT_S"
_SCHEMA_RETRY_S = 1.0
_SCHEMA_RETRY_MAX_S = 15.0


def _schema_wait_s() -> float:
    raw = os.getenv(SCHEMA_WAIT_ENV, "").strip()
    if not raw:
        return SCHEMA_WAIT_S
    try:
        return float(raw)
    except ValueError:
        logger.warning(
            "ignoring %s=%r — not a number, using %.0fs",
            SCHEMA_WAIT_ENV,
            raw,
            SCHEMA_WAIT_S,
        )
        return SCHEMA_WAIT_S


async def schema_is_current(budget_s: float | None = None) -> bool:
    """Wait for a database this build may serve. False means do not start.

    The deploy this build belongs to has an order, and this is the step that
    catches it being run out of it: a session row written before
    ``0034_strategy_type_key`` keeps its strategy's short name in a column
    this build does not read, so every one of them reads as a row naming no
    strategy. Refusing to start is the only answer that leaves those rows for
    the migration to fix.

    Every wait is logged with what is wrong, so an operator who ran the steps
    in the wrong order reads the reason in the first second rather than at
    the end of the window. Returning False rather than raising: the caller
    turns it into an exit code, and a traceback would say less than the line
    already logged.
    """
    budget = _schema_wait_s() if budget_s is None else budget_s
    loop = asyncio.get_running_loop()
    deadline = loop.time() + budget
    delay = _SCHEMA_RETRY_S
    while True:
        try:
            await require_sts_schema()
            return True
        except SchemaTooOld as exc:
            problem = str(exc)
        except Exception as exc:
            problem = f"the database could not be read: {exc}"
        left = deadline - loop.time()
        if left <= 0:
            logger.error(
                "STS will not start: %s (waited %.0fs)", problem, budget
            )
            return False
        logger.warning(
            "STS is waiting for the database: %s (%.0fs left)", problem, left
        )
        await asyncio.sleep(min(delay, left))
        delay = min(delay * 2, _SCHEMA_RETRY_MAX_S)


async def amain() -> bool:
    if not await schema_is_current():
        return False
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
        loaded, stamp = refresh()
        if loaded:
            logger.info(
                "STS loaded %d registry strategy(ies): %s",
                len(loaded),
                ", ".join(loaded),
            )
        if stamp.generation:
            logger.info(
                "STS env generation=%s extras=%s",
                stamp.generation,
                ", ".join(sorted(extras_names())) or "(none)",
            )
        sessions = SessionManager(
            broker,
            persist_live=sts_db.persist_live_session,
            mark_done=sts_db.mark_session_finished,
            list_db_sessions=sts_db.list_sessions,
            load_session=sts_db.load_session,
            mark_live=sts_db.mark_session_live,
            td_instance=sts_db.td_instance,
            derive_sts=sts_db.derived_sts,
            instance=INSTANCE,
            spawner=SubprocessSpawner(),
        )
        logger.info("STS started instance=%s", INSTANCE)
        subjects = control_subjects(SOURCE, INSTANCE, ROLE)
        if not subjects:
            logger.warning(
                "STS is %s and serves no control subject — it holds what it "
                "has and takes nothing new",
                ROLE.value,
            )
        rpc_tasks = [
            asyncio.create_task(
                run_rpc(broker, sessions, stop, subject=subject),
                name=f"sts-rpc-{subject}",
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
            name="sts-sys-heartbeat",
        )
        reaper_task = asyncio.create_task(
            reap_loop(sessions, stop), name="sts-reaper"
        )
        health_task = asyncio.create_task(
            serve_health(
                broker, domain=SOURCE, instance=INSTANCE, stop=stop
            ),
            name="sts-health",
        )
        # Not one of the tasks run_until_stopped watches: this is meant
        # to finish, once the API has pushed the store. A process that
        # does not serve its own subject cannot receive that push.
        catchup_task: asyncio.Task[Any] | None = None
        if Topics.sts(INSTANCE) in subjects:
            catchup_task = asyncio.create_task(
                catch_up_until_matched(broker, INSTANCE, stop),
                name="sts-registry-catchup",
            )
        try:
            clean = await run_until_stopped(
                stop,
                *rpc_tasks,
                hb_task,
                reaper_task,
                health_task,
                logger=logger,
            )
        finally:
            stop.set()
            tasks = [*rpc_tasks, hb_task, reaper_task, health_task]
            if catchup_task is not None:
                tasks.append(catchup_task)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await sessions.close_all()
    logger.info("STS stopped")
    return clean


def main() -> None:
    configure_logging(SOURCE)
    # Non-zero when a long-lived task ended on its own: the restart
    # policy is what puts the process back, and an exit code is what
    # tells anyone reading ``docker ps`` that STS did not just stop.
    #
    # ``uvloop.run`` rather than ``asyncio.run`` — docs/EventLoop.md has the
    # measurements. This loop serves the instance. Each live session is a
    # worker process with a loop of its own. It builds that loop for this
    # call alone and leaves the global policy untouched.
    if not uvloop.run(amain()):
        raise SystemExit(1)
