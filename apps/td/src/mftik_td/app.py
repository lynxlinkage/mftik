"""TD process bootstrap — RPC, account workers, backfill, heartbeat."""

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
from mftik.intent_gc import watch_sts_reports
from mftik.procman import (
    CloseMode,
    Supervisor,
    admission_budget_from_environ,
    current_release,
    pinned_releases_path,
    publish_reports,
)
from mftik.protocol import UntypedEnvelope
from mftik.symbols import SymbolClient

from mftik_td import db as td_db
from mftik_td.backfill import (
    BackfillExecutor,
    BackfillSession,
    HistoryReaderFactory,
)
from mftik_td.controller import TdOrchestrator, intent_book
from mftik_td.controller.defaults import ACCOUNT_RECONCILE_PERIOD_S
from mftik_td.rpc import dispatch
from mftik_td.supervise import (
    account_restart_intensity,
    account_views,
    apply_reconcile,
    load_accounts,
    serve_account_drain,
)

SOURCE = "td"
#: Which TD this process is. ``MFTIK_INSTANCE``, defaulting to the
#: plane name — so an unconfigured deployment is the instance called
#: ``td``, which migration 0031 declares. Nothing routes on it yet.
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
    """``${WORK_DIR}/td/<instance>``, or ``./td/<instance>`` when unset."""
    root = os.environ.get("WORK_DIR") or os.getcwd()
    return Path(root) / SOURCE / INSTANCE


def _code_ref() -> str:
    """The release this process is. B3-07's :func:`current_release`."""
    return current_release()


async def run_rpc(
    broker: Broker,
    stop: asyncio.Event,
    *,
    subject: str,
    drain=None,
) -> None:
    """Serve TD request-reply on ``subject`` until ``stop``.

    One task per subject the role grants, rather than one loop over several:
    each is the same loop with a different name, and a failure in one is not a
    reason to stop answering on the other. ``drain`` is the account
    drain-replace callback. Without it that type is refused.
    """
    logger.info("TD RPC listening on subject=%s", subject)

    async def handle(message: UntypedEnvelope):
        # The broker and the instance are this process's. A delete that
        # leaves an account idle asks for a detach backfill through them.
        return await dispatch(
            message, broker=broker, instance=INSTANCE, drain=drain
        )

    await serve(broker, subject, handle, stop=stop)


async def _held_set_ready(seeded: bool) -> bool:
    """Whether the in-memory book has been rebuilt from ``td_intents``.

    False until :func:`mftik_td.db.seed_intent_book` succeeds. A failed
    read stays false and the next pass tries again. The trading bit is
    not pushed from that empty book (P5): workers that outlived this
    process keep the bit they already have.
    """
    if seeded:
        return True
    return await td_db.seed_intent_book(intent_book(), instance=INSTANCE)


async def _reconcile_once(
    supervisor: Supervisor,
    orchestrator: TdOrchestrator,
    observations,
    broker: Broker,
    *,
    publish: bool,
) -> None:
    """Read bindings, name actions, apply spawn, stop, release and the trading bit.

    ``publish`` false names no ``td.account.trading`` push. The boot
    pass and every later pass leave it false until the held set has
    been seeded.
    """
    accounts, flags = await load_accounts(INSTANCE)
    views = await account_views(supervisor, observations, accounts)
    actions = orchestrator.reconcile(
        accounts, intent_book().rows(), views, publish=publish
    )
    await apply_reconcile(
        supervisor,
        actions,
        accounts,
        code_ref=orchestrator.code_ref,
        cancel_on_disconnect=flags,
        broker=broker,
        held=orchestrator.draining,
        gate=orchestrator.gate,
        respect_held=True,
    )


async def _reconcile_loop(
    supervisor: Supervisor,
    orchestrator: TdOrchestrator,
    observations,
    broker: Broker,
    stop: asyncio.Event,
    seeded: bool,
) -> None:
    """Later passes. The boot pass already ran, before the subjects opened.

    ``seeded`` is that boot pass's answer. A seed that already succeeded
    is not read again: a later read would put back an owner the report
    GC had released.
    """
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=ACCOUNT_RECONCILE_PERIOD_S)
            return
        except TimeoutError:
            pass
        try:
            seeded = await _held_set_ready(seeded)
            await _reconcile_once(
                supervisor, orchestrator, observations, broker, publish=seeded
            )
        except Exception:
            logger.exception("TD reconcile pass failed")


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
        # One symbol client for the process: its cache is what keeps symbol
        # resolution off the wire, and the backfill resolves the same tickers
        # the order path does.
        symbols = SymbolClient(broker)
        backfill = BackfillSession(
            broker,
            BackfillExecutor(
                broker=broker,
                factory=HistoryReaderFactory(symbols),
                load_api=td_db.get_api,
            ),
            instance=INSTANCE,
        )
        await backfill.start()
        supervisor = Supervisor(
            _work_dir(),
            plane="td",
            instance=INSTANCE,
            budget=admission_budget_from_environ("td"),
            # The pin file lives under the Strategon work dir, not under
            # ``td/<instance>``. ``None`` while that variable is unset.
            pin_path=pinned_releases_path(),
        )
        booted = False
        clean = False
        try:
            observations = await supervisor.start()
            booted = True
            orchestrator = TdOrchestrator(
                supervisor,
                intensity=account_restart_intensity(),
                code_ref=_code_ref(),
            )
            # One held set for every subject this process serves. A lower
            # ``procman.report`` generation is a new STS publisher and resets
            # that instance before the sample; see
            # :func:`mftik.intent_gc.watch_sts_reports`.
            intents = intent_book()
            # The book is empty after a process start. Seed it from
            # unreleased ``td_intents`` before any trading push. A failed
            # read leaves ``publish`` false; the next pass retries.
            # Workers that are still up keep their last bit (P5).
            seeded = False
            try:
                seeded = await _held_set_ready(seeded)
                await _reconcile_once(
                    supervisor,
                    orchestrator,
                    observations,
                    broker,
                    publish=seeded,
                )
            except Exception:
                logger.exception("TD reconcile pass failed")
            logger.info("TD started instance=%s", INSTANCE)
            subjects = control_subjects(SOURCE, INSTANCE, ROLE)
            if not subjects:
                logger.warning(
                    "TD is %s and serves no control subject — it holds what it "
                    "has and takes nothing new",
                    ROLE.value,
                )

            async def _drain(message: UntypedEnvelope):
                return await serve_account_drain(
                    message,
                    supervisor=supervisor,
                    orchestrator=orchestrator,
                    broker=broker,
                    instance=INSTANCE,
                )

            rpc_tasks = [
                asyncio.create_task(
                    run_rpc(broker, stop, subject=subject, drain=_drain),
                    name=f"td-rpc-{subject}",
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
                name="td-intent-gc",
            )
            reconcile_task = asyncio.create_task(
                _reconcile_loop(
                    supervisor,
                    orchestrator,
                    observations,
                    broker,
                    stop,
                    seeded,
                ),
                name="td-reconcile",
            )
            report_task = asyncio.create_task(
                publish_reports(
                    supervisor,
                    plane=SOURCE,
                    instance=INSTANCE,
                    publish=lambda subject, envelope: broker.publish(subject, envelope),
                    clock=SystemClock(),
                ),
                name="td-procman-report",
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
            health_task = asyncio.create_task(
                serve_health(
                    broker,
                    domain=SOURCE,
                    instance=INSTANCE,
                    stop=stop,
                ),
                name="td-health",
            )
            tasks = (
                *rpc_tasks,
                gc_task,
                reconcile_task,
                report_task,
                hb_task,
                health_task,
            )
            try:
                clean = await run_until_stopped(stop, *tasks, logger=logger)
            finally:
                stop.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            if booted:
                await supervisor.close(CloseMode.DETACH)
            # The cron is the guarantee. Asking after we have stopped serving
            # the subject would always fail, and a successor looking at the
            # cursors is strictly better than being told.
            await backfill.stop()
    logger.info("TD stopped")
    return clean


def main() -> None:
    configure_logging(SOURCE)
    # Non-zero when a long-lived task ended on its own: the restart
    # policy is what puts the process back, and an exit code is what
    # tells anyone reading ``docker ps`` that TD did not just stop.
    #
    # ``uvloop.run`` rather than ``asyncio.run`` — docs/EventLoop.md has the
    # measurements. It builds the loop for this one call and leaves the global
    # policy alone, so the loop this process runs is stated here rather than
    # inherited from whatever an import happened to install.
    if not uvloop.run(amain()):
        raise SystemExit(1)
