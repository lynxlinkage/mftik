"""STS process bootstrap — RPC, registry, heartbeat, session supervisor.

B4-02 runs one :class:`~mftik.procman.Supervisor` for this instance.
``start`` applies reattach before the control subject is served.
``SIGTERM`` closes with ``detach``, so the session workers keep running
(§4.6). Reports are :func:`mftik.procman.publish_reports` with no phase
filter. ``extra_workers`` lists a session that has been accepted and
not yet spawned, so the next report still names it (B4-07). A
``restarting`` session with no process is B5-06 and is not added.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from pathlib import Path
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
from mftik.protocol import Topics
from mftik.strategy.artifacts import get_store
from mftik_db.schema import SchemaTooOld, require_sts_schema

from mftik_sts.registry_catchup import catch_up_until_matched
from mftik_sts.runtime_env import extras_names, refresh

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
#: did not expect. :func:`mftik.broker.handler.serve` already survives what
#: it knows how to survive, so this only paces the failures nothing has a
#: name for yet. ``run_rpc`` passes it through so a test can set it to zero.
RPC_RESTART_DELAY_SECONDS = 1.0

#: The process's orchestrator. ``run_rpc`` reads it. Health still answers
#: when it is ``None`` (a probe, or a test that never booted a supervisor).
_orchestrator: Any = None


def bind_orchestrator(orchestrator: Any) -> None:
    """Install the orchestrator ``run_rpc`` serves. ``None`` clears it."""
    global _orchestrator
    _orchestrator = orchestrator


def _supervisor_work_dir(plane: str, instance: str) -> Path:
    """``${WORK_DIR}/<plane>/<instance>``, or the cwd when ``WORK_DIR`` is unset."""
    root = Path(os.environ.get("WORK_DIR") or os.getcwd())
    return root / plane / instance


def _open_supervisor() -> Any:
    """This process's supervisor.

    ``pin_path`` is :func:`mftik.procman.pinned_releases_path`: the S-2
    file when Strategon named a release, and ``None`` while it did not
    (B3-07). The path is not this instance's work directory.
    """
    from mftik.procman import Supervisor, pinned_releases_path

    return Supervisor(
        _supervisor_work_dir(SOURCE, INSTANCE),
        plane="sts",
        instance=INSTANCE,
        pin_path=pinned_releases_path(),
    )


async def run_rpc(
    broker: Broker,
    stop: asyncio.Event,
    *,
    subject: str,
    instance: str | None = None,
) -> None:
    """Serve STS request-reply on ``subject`` until ``stop``.

    One task per subject the role grants, rather than one loop over several:
    each is the same loop with a different name, and a failure in one is not a
    reason to stop answering on the other. ``instance`` is unused: the bound
    orchestrator already knows which STS this process is. Health answers
    even when that orchestrator is not bound.
    """
    del instance
    # Imported here so a health probe, which imports this package, does not
    # pay for the supervisor on the way to ``handle_health``.
    from mftik.broker.handler import serve as serve_subject

    from mftik_sts.rpc.router import control_handler

    logger.info("STS RPC listening on subject=%s", subject)
    await serve_subject(
        broker,
        subject,
        control_handler(broker, _orchestrator),
        stop=stop,
        restart_delay=RPC_RESTART_DELAY_SECONDS,
    )


#: How often to clear abandoned artifact uploads. Slow on purpose: a part
#: file costs disk and nothing else, and the scan reads a directory.
SWEEP_INTERVAL_SECONDS = 60.0


async def sweep_loop(
    stop: asyncio.Event,
    *,
    interval: float = SWEEP_INTERVAL_SECONDS,
) -> None:
    """Clear artifact uploads nobody committed, on boot and on an interval.

    This used to share a loop with the orphan reaper, which RM-04 deleted
    along with the session manager it scanned. The artifact store is this
    plane's own disk and still needs sweeping.
    """
    while not stop.is_set():
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
    catches it being run out of it. A session row written before
    ``0034_strategy_type_key`` keeps its strategy's short name in a column
    this build does not read, so every one of them reads as a row naming no
    strategy. A database that has not reached ``MIN_STS_REVISION`` is also
    missing Spec/Status columns this build selects. Refusing to start is the
    only answer that leaves those rows for the migration to fix.

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
        logger.info("STS started instance=%s", INSTANCE)
        from mftik.clock import SystemClock
        from mftik.procman import CloseMode, publish_reports
        from mftik_db.session import session_scope

        from mftik_sts.controller import StsOrchestrator
        from mftik_sts.controller.status import DbStatusStore

        supervisor = _open_supervisor()
        orchestrator = StsOrchestrator(
            supervisor,
            clock=SystemClock(),
            store=DbStatusStore(session_scope),
        )
        bind_orchestrator(orchestrator)
        try:
            await orchestrator.boot()
        except Exception:
            logger.exception("STS supervisor failed to start")
            bind_orchestrator(None)
            await supervisor.close(CloseMode.DETACH)
            return False
        subjects = control_subjects(SOURCE, INSTANCE, ROLE)
        if not subjects:
            logger.warning(
                "STS is %s and serves no control subject — it holds what it "
                "has and takes nothing new",
                ROLE.value,
            )
        rpc_tasks = [
            asyncio.create_task(
                run_rpc(broker, stop, subject=subject, instance=INSTANCE),
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
        sweep_task = asyncio.create_task(
            sweep_loop(stop), name="sts-artifact-sweep"
        )
        health_task = asyncio.create_task(
            serve_health(
                broker, domain=SOURCE, instance=INSTANCE, stop=stop
            ),
            name="sts-health",
        )

        async def _publish(subject: str, envelope: Any) -> None:
            await broker.publish(subject, envelope)

        report_task = asyncio.create_task(
            publish_reports(
                supervisor,
                plane="sts",
                instance=INSTANCE,
                publish=_publish,
                clock=SystemClock(),
                extra_workers=orchestrator.extra_workers,
            ),
            name="sts-procman-report",
        )
        watch_task = asyncio.create_task(
            orchestrator.watch(stop), name="sts-session-watch"
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
                sweep_task,
                health_task,
                report_task,
                watch_task,
                logger=logger,
            )
        finally:
            # Stop accepting, then detach. Workers keep running (§4.6).
            stop.set()
            tasks = [
                *rpc_tasks,
                hb_task,
                sweep_task,
                health_task,
                report_task,
                watch_task,
            ]
            if catchup_task is not None:
                tasks.append(catchup_task)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await supervisor.close(CloseMode.DETACH)
            bind_orchestrator(None)
    logger.info("STS stopped")
    return clean


def main() -> None:
    configure_logging(SOURCE)
    # Non-zero when a long-lived task ended on its own: the restart
    # policy is what puts the process back, and an exit code is what
    # tells anyone reading ``docker ps`` that STS did not just stop.
    #
    # ``uvloop.run`` rather than ``asyncio.run`` — docs/EventLoop.md has the
    # measurements. This loop serves the instance. It builds that loop for
    # this call alone and leaves the global policy untouched.
    if not uvloop.run(amain()):
        raise SystemExit(1)
