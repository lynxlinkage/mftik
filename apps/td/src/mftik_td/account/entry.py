"""Process entry for one paper account worker.

``python -m mftik_td.account`` builds one :class:`AccountWorker` from
the argv and the ``apis`` row, heartbeats to the shim, and serves
``td.order.{api_id}`` and ``td.account.{api_id}``. The TD process does
not import this module. The order path does not go through the
controller (§7.1).

Paper only. Any other venue logs and exits before ``ready``, which is
B6-02's trading layer. The paper trading layer stays up for the life
of the process: there is no wire type for ``PUSH_TRADING`` yet
(DEFAULT 1). Shutdown is the only :meth:`TradingLayer.deactivate`.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

import uvloop
from mftik import configure_logging
from mftik.broker import Broker
from mftik.broker.handler import serve
from mftik.clock import SystemClock
from mftik.exchange.paper.remote import PaperRemotePrivateClient
from mftik.exchange.venues import UnknownVenueError, require
from mftik.protocol import STS_ORDER_SUBMIT, TD_OMS_VIEW, Envelope, Topics
from mftik.symbols import SymbolClient

from mftik_td.account.handlers import account_subject_handler
from mftik_td.account.heartbeat import beat_until, write_heartbeat
from mftik_td.account.session import Session
from mftik_td.account.worker import AccountWorker
from mftik_td.backfill.executor import BackfillExecutor
from mftik_td.backfill.reader import HistoryReaderFactory
from mftik_td.db import account_credential, get_api

logger = logging.getLogger("mftik_td.account")

#: How long to wait for this process's own subjects to answer before
#: the shim is told the worker is ready.
#:
#: ``serve`` flushes the subscription and then blocks on the next
#: request; it does not report that the flush finished. The probe is an
#: empty payload of a real type, which the handler answers as
#: ``invalid_payload`` and does not book. A live subscription answers
#: at once; this is only the failure bound. The plan does not name it.
_LISTEN_TIMEOUT_S = 2.0


def main(argv: list[str] | None = None) -> None:
    """Run one account worker. A non-zero exit is a failed start."""
    configure_logging("td")
    code = uvloop.run(run(argv))
    if code:
        raise SystemExit(code)


async def run(argv: list[str] | None = None) -> int:
    """Serve one paper account until SIGINT or SIGTERM. Returns an exit code."""
    args = _parse(argv)
    stop = asyncio.Event()
    _install_signals(stop)
    ready = False

    def is_ready() -> bool:
        return ready

    beat = asyncio.create_task(beat_until(stop, is_ready), name="td-account-beat")
    write_heartbeat(False)
    worker: AccountWorker | None = None
    try:
        row = await account_credential(args.api_id)
        if row is None:
            logger.error("account worker api_id=%s has no apis row", args.api_id)
            return 1
        try:
            venue = require(row.venue).name
        except UnknownVenueError:
            logger.error(
                "account worker api_id=%s venue=%r is not a venue",
                args.api_id,
                row.venue,
            )
            return 1
        if venue != "Paper":
            logger.error(
                "account worker api_id=%s venue=%s is not paper; "
                "real venues are B6-02",
                args.api_id,
                venue,
            )
            return 1
        if row.cancel_on_disconnect != args.cancel_on_disconnect:
            logger.info(
                "account worker api_id=%s cancel_on_disconnect argv=%s row=%s; "
                "using the row",
                args.api_id,
                args.cancel_on_disconnect,
                row.cancel_on_disconnect,
            )
        async with Broker() as broker:
            private = PaperRemotePrivateClient(
                broker,
                api_key=row.api_key,
                api_secret=row.api_secret,
                passphrase=row.passphrase,
            )
            symbols = SymbolClient(broker)
            session = Session(
                api_id=row.api_id,
                broker=broker,
                private=private,
                symbols=symbols,
            )
            worker = AccountWorker(
                row.api_id,
                venue=venue,
                incarnation=args.incarnation,
                cancel_on_disconnect=row.cancel_on_disconnect,
                private=private,
                session=session,
                clock=SystemClock(),
                backfill=BackfillExecutor(
                    broker=broker,
                    factory=HistoryReaderFactory(symbols),
                    load_api=get_api,
                ),
            )
            try:
                await worker.resident.start()
                await worker.trading.activate()
            except Exception:
                logger.exception(
                    "account worker api_id=%s failed to start", args.api_id
                )
                await _shutdown(worker)
                return 1
            order_task = asyncio.create_task(
                serve(
                    broker,
                    Topics.td_order(worker.api_id),
                    worker.orders,
                    stop=stop,
                ),
                name=f"td-order-{worker.api_id}",
            )
            account_task = asyncio.create_task(
                serve(
                    broker,
                    Topics.td_account(worker.api_id),
                    account_subject_handler(worker),
                    stop=stop,
                ),
                name=f"td-account-{worker.api_id}",
            )
            try:
                await _listen(
                    broker, Topics.td_order(worker.api_id), STS_ORDER_SUBMIT
                )
                await _listen(
                    broker, Topics.td_account(worker.api_id), TD_OMS_VIEW
                )
            except Exception:
                logger.exception(
                    "account worker api_id=%s subjects did not answer",
                    args.api_id,
                )
                for task in (order_task, account_task):
                    task.cancel()
                await asyncio.gather(
                    order_task, account_task, return_exceptions=True
                )
                await _shutdown(worker)
                return 1
            ready = True
            write_heartbeat(True)
            logger.info(
                "account worker ready api_id=%s incarnation=%s",
                worker.api_id,
                worker.incarnation,
            )
            await stop.wait()
            for task in (order_task, account_task):
                task.cancel()
            await asyncio.gather(order_task, account_task, return_exceptions=True)
            await _shutdown(worker)
        return 0
    finally:
        stop.set()
        beat.cancel()
        await asyncio.gather(beat, return_exceptions=True)


async def _listen(broker: Broker, subject: str, type_name: str) -> None:
    """Return once ``subject`` has a responder in this process.

    An empty payload does not validate, so the handler answers
    ``invalid_payload`` and does not touch the book.
    """
    await broker.request(
        subject,
        Envelope[dict[str, object]].wrap(
            {},
            type=type_name,
            source="td",
        ),
        timeout=_LISTEN_TIMEOUT_S,
    )


async def _shutdown(worker: AccountWorker) -> None:
    """Close the trading book, then the resident connector."""
    try:
        if worker.trading.active:
            await worker.trading.deactivate()
        else:
            session = worker.trading.session
            if session is not None and session.started and not session.destroyed:
                await session.destroy()
        if worker.resident.started:
            await worker.resident.close()
    except Exception:
        logger.exception("account worker api_id=%s shutdown failed", worker.api_id)


def _parse(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="mftik_td.account")
    parser.add_argument("--api-id", type=int, required=True)
    parser.add_argument("--incarnation", type=int, required=True)
    parser.add_argument(
        "--cancel-on-disconnect",
        required=True,
        choices=("true", "false"),
    )
    args = parser.parse_args(argv)
    if args.api_id <= 0:
        parser.error("--api-id must be a positive int")
    if args.incarnation < 0:
        parser.error("--incarnation must be an int >= 0")
    args.cancel_on_disconnect = args.cancel_on_disconnect == "true"
    return args


def _install_signals(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            return
