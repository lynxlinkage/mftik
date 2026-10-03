"""MD process bootstrap — RPC, paper public factory, heartbeat."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from pathlib import Path

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
from mftik.broker.handler import serve
from mftik.clock import SystemClock
from mftik.exchange import venues
from mftik.intent_gc import watch_sts_reports
from mftik.procman import (
    ProcmanError,
    Supervisor,
    admission_budget_from_environ,
    current_release,
    pinned_releases_path,
    publish_reports,
)
from mftik.protocol import ProcmanReportEnvelope

from mftik_md.fetch_ctl import FetchController, fetch_close_mode
from mftik_md.intents import MdIntentBook
from mftik_md.rpc import control_handler
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


def _work_dir() -> Path:
    """``${WORK_DIR:-cwd}/md/<instance>``.

    Strategon starts a payload with its work dir as cwd. The per-plane
    subdirectory keeps two planes started from one directory from sharing
    ``supervisor.json``.
    """
    root = Path(os.environ.get("WORK_DIR") or os.getcwd())
    return root / SOURCE / INSTANCE


def _code_ref() -> str:
    """The release that spawned this process's workers (§4.5).

    :func:`mftik.procman.current_release`: the Strategon tag when that
    variable is set, otherwise the installed distribution version.
    """
    return current_release()


async def run_rpc(
    broker: Broker,
    store: TapeStore | None,
    stop: asyncio.Event,
    *,
    subject: str,
    intents: MdIntentBook | None = None,
) -> None:
    """Serve MD request-reply on ``subject`` until ``stop``.

    One task per subject the role grants, rather than one loop over several:
    each is the same loop with a different name, and a failure in one is not a
    reason to stop answering on the other. ``intents`` is the process's held
    set. Omitted, this subject gets an empty book of its own — health probes
    never read it. The running process passes one book into every subject.
    """
    book = intents if intents is not None else MdIntentBook()
    logger.info("MD RPC listening on subject=%s", subject)
    await serve(broker, subject, control_handler(store, book), stop=stop)


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
        # Built for its store, which is what ``md.tape.tail`` reads. Nothing
        # appends to it: the fan-out that recorded every print went with the
        # session mechanism, and B8 is what gives the tape a writer again.
        recorder = _build_recorder()
        store = recorder.store if recorder is not None else None
        if store is not None:
            await store.ping()
        # The fetch worker is a separate process. This process supervises
        # it and detaches on the way out, so a roll does not interrupt
        # ``md.fetch``. Reads are not served here.
        clock = SystemClock()
        supervisor = Supervisor(
            _work_dir(),
            plane="md",
            instance=INSTANCE,
            clock=clock,
            budget=admission_budget_from_environ("md"),
            pin_path=pinned_releases_path(),
        )
        report_task: asyncio.Task[None] | None = None
        watch_task: asyncio.Task[None] | None = None
        rpc_tasks: list[asyncio.Task[None]] = []
        gc_task: asyncio.Task[None] | None = None
        hb_task: asyncio.Task[None] | None = None
        health_task: asyncio.Task[None] | None = None
        try:
            observations = await supervisor.start()
            fetch = FetchController(
                supervisor, clock=clock, code_ref=_code_ref()
            )
            try:
                await fetch.reconcile(observations)
            except ProcmanError as exc:
                # A refusal (including capacity_exceeded) is retried by
                # the watch. No memory budget is configured.
                logger.error("MD fetch worker was not started: %s", exc)

            async def _publish_report(
                subject: str, envelope: ProcmanReportEnvelope
            ) -> None:
                await broker.publish(subject, envelope)

            report_task = asyncio.create_task(
                publish_reports(
                    supervisor,
                    plane="md",
                    instance=INSTANCE,
                    publish=_publish_report,
                    clock=clock,
                ),
                name="md-procman-report",
            )
            watch_task = asyncio.create_task(
                fetch.watch(stop), name="md-fetch-watch"
            )
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
            # One held set for every subject, including the pooled ``md``.
            # A lower ``procman.report`` generation is a new STS publisher and
            # resets that instance before the sample; see
            # :func:`mftik.intent_gc.watch_sts_reports`.
            intents = MdIntentBook()
            rpc_tasks = [
                asyncio.create_task(
                    run_rpc(
                        broker, store, stop, subject=subject, intents=intents
                    ),
                    name=f"md-rpc-{subject}",
                )
                for subject in subjects
            ]
            gc_task = asyncio.create_task(
                watch_sts_reports(
                    broker,
                    held=intents.owners,
                    release=intents.release_owners,
                    stop=stop,
                    states=intents.gc_states,
                ),
                name="md-intent-gc",
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
            health_task = asyncio.create_task(
                serve_health(
                    broker,
                    domain=SOURCE,
                    instance=INSTANCE,
                    stop=stop,
                    # Which venues this MD can reach. A deploy naming a feed
                    # on a venue it cannot serve should fail at deploy rather
                    # than at subscribe, and this is where that answer comes
                    # from.
                    describe=lambda: {"venues": sorted(venues.names())},
                ),
                name="md-health",
            )
            try:
                clean = await run_until_stopped(
                    stop,
                    *rpc_tasks,
                    gc_task,
                    hb_task,
                    health_task,
                    report_task,
                    watch_task,
                    logger=logger,
                )
            finally:
                stop.set()
                tasks = [
                    task
                    for task in (
                        *rpc_tasks,
                        gc_task,
                        hb_task,
                        health_task,
                        report_task,
                        watch_task,
                    )
                    if task is not None
                ]
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            # SIGTERM is a roll. The fetch worker keeps serving.
            await supervisor.close(fetch_close_mode())
            if recorder is not None:
                await recorder.aclose()
    logger.info("MD stopped")
    return clean


def main() -> None:
    configure_logging(SOURCE)
    # Non-zero when a long-lived task ended on its own: the restart
    # policy is what puts the process back, and an exit code is what
    # tells anyone reading ``docker ps`` that MD did not just stop.
    #
    # ``uvloop.run`` rather than ``asyncio.run`` — docs/archive/EventLoop.md has the
    # measurements. It builds the loop for this one call and leaves the global
    # policy alone, so the loop this process runs is stated here rather than
    # inherited from whatever an import happened to install.
    if not uvloop.run(amain()):
        raise SystemExit(1)
