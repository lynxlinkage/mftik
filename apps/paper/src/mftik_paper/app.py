"""Paper engine process — owns PaperExchange, serves RPC, publishes streams."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import Any

from mftik import configure_logging
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.exchange.models import Balance, Fill, Order, OrderBook
from mftik.protocol import (
    PAPER_BALANCE,
    PAPER_FILL,
    PAPER_ORDER,
    PAPER_ORDER_BOOK,
    Topics,
    UntypedEnvelope,
)

from mftik_paper.accounts import LIQUIDITY_ORDERS, SEEDED_ACCOUNTS
from mftik_paper.rpc import dispatch

SOURCE = "paper"
logger = logging.getLogger(SOURCE)

#: How often the book is republished with nothing having moved it. Two seconds
#: is short enough that a strategy attaching to a quiet venue sees a book
#: before it wonders whether the feed works, and long enough that an idle
#: stack is not writing to Redis for no reason.
BOOK_INTERVAL_ENV = "PAPER_BOOK_INTERVAL_S"
DEFAULT_BOOK_INTERVAL_S = 2.0


def _book_interval() -> float:
    raw = os.getenv(BOOK_INTERVAL_ENV, "").strip()
    if not raw:
        return DEFAULT_BOOK_INTERVAL_S
    try:
        value = float(raw)
    except ValueError:
        logger.warning("%s=%r is not a number; using default", BOOK_INTERVAL_ENV, raw)
        return DEFAULT_BOOK_INTERVAL_S
    if value <= 0:
        logger.warning("%s must be positive; using default", BOOK_INTERVAL_ENV)
        return DEFAULT_BOOK_INTERVAL_S
    return value


class RedisEventBridge:
    """Fan engine private events out on ``paper.{api_key}.*`` topics."""

    def __init__(self, broker: Broker) -> None:
        self._broker = broker

    def on_order(self, account: str, order: Order) -> None:
        self._schedule(
            Topics.paper_orders(account),
            PAPER_ORDER,
            order.model_dump(mode="json"),
        )

    def on_fill(self, account: str, fill: Fill) -> None:
        self._schedule(
            Topics.paper_fills(account),
            PAPER_FILL,
            fill.model_dump(mode="json"),
        )

    def on_balance(self, account: str, balance: Balance) -> None:
        self._schedule(
            Topics.paper_balances(account),
            PAPER_BALANCE,
            balance.model_dump(mode="json"),
        )

    def _schedule(self, topic: str, type_: str, payload: dict) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._publish(topic, type_, payload))

    async def _publish(self, topic: str, type_: str, payload: dict) -> None:
        try:
            await self._broker.publish(
                topic,
                UntypedEnvelope.wrap(payload, type=type_, source="paper"),
            )
        except Exception:
            logger.exception("paper stream publish failed topic=%s", topic)


async def _publish_book(
    broker: Broker, topic: str, book: OrderBook, symbol: str
) -> None:
    try:
        await broker.publish(
            topic,
            UntypedEnvelope.wrap(
                book.model_dump(mode="json"),
                type=PAPER_ORDER_BOOK,
                source=SOURCE,
            ),
        )
    except Exception:
        logger.exception("paper orderbook publish failed symbol=%s", symbol)


async def _pump_order_book(
    broker: Broker,
    exchange: PaperExchange,
    symbol: str,
    stop: asyncio.Event,
) -> None:
    """Publish the book whenever it changes.

    Subscribes to the engine, not to ``PaperPublicClient``: the client's
    stream methods take a ``UniversalTicker`` and this has a bare symbol. It
    used to hand the symbol over anyway, which raised ``AttributeError`` on
    the first call and killed this task before it published anything — with
    nothing in the log, because a task whose reference is held is never
    garbage collected and its exception is therefore never retrieved. The
    feed had not worked once; it had only ever failed quietly.
    """
    topic = Topics.paper_order_book(symbol)
    async for book in exchange.subscribe_order_book(symbol):
        if stop.is_set():
            return
        await _publish_book(broker, topic, book, symbol)


async def _tick_order_book(
    broker: Broker,
    exchange: PaperExchange,
    symbol: str,
    stop: asyncio.Event,
    interval: float,
) -> None:
    """And publish it on a cadence, whether or not anything moved it.

    A change-driven feed alone is silent on an idle venue, and an idle venue
    is what a paper stack is until somebody trades on it. So a strategy would
    subscribe, attach, and wait forever for a hook that only fires when the
    book moves — which nothing was going to do, because the only thing that
    would have was the strategy this feed was supposed to be feeding.

    Real venues push snapshots on a schedule for the same reason: a
    subscriber that arrives between two changes still needs to know the book.
    """
    topic = Topics.paper_order_book(symbol)
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
            return  # stop was set
        except TimeoutError:
            pass
        await _publish_book(
            broker, topic, exchange.get_order_book(symbol), symbol
        )


def _watch(task: asyncio.Task[Any]) -> asyncio.Task[Any]:
    """Say so when a background task dies.

    These are held in a list for their lifetime, so Python never collects them
    and never reports what they raised. That is how the order book feed stayed
    broken: it failed on its first line and looked exactly like a feed with
    nothing to say.
    """

    def done(finished: asyncio.Task[Any]) -> None:
        if finished.cancelled():
            return
        exc = finished.exception()
        if exc is not None:
            logger.error(
                "paper background task %s died: %r", finished.get_name(), exc
            )

    task.add_done_callback(done)
    return task


async def run_rpc(
    broker: Broker, exchange: PaperExchange, stop: asyncio.Event
) -> None:
    logger.info("Paper RPC listening on subject=%s", Topics.PAPER)
    async for req in broker.serve(Topics.PAPER, stop=stop):
        try:
            await dispatch(req, exchange=exchange)
        except Exception:
            logger.exception(
                "Paper RPC handler failed type=%s id=%s",
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
        bridge = RedisEventBridge(broker)
        exchange = PaperExchange(
            tick_interval=1.0,
            volatility_bps=0,
            on_order=bridge.on_order,
            on_fill=bridge.on_fill,
            on_balance=bridge.on_balance,
        )
        for api_key, api_secret, balances in SEEDED_ACCOUNTS:
            exchange.register_api(api_key, api_secret, balances=balances)
            logger.info(
                "seeded paper account key=%s balances=%s", api_key, balances
            )
        for api_key, req in LIQUIDITY_ORDERS:
            order = await exchange.place_order(api_key, req)
            logger.info(
                "seeded liquidity %s %s %s@%s cid=%s status=%s",
                req.side.value,
                req.symbol,
                req.price,
                req.qty,
                order.client_order_id,
                order.status.value,
            )
        await exchange.start()
        public = exchange.public()
        await public.connect()

        rpc_task = asyncio.create_task(
            run_rpc(broker, exchange, stop), name="paper-rpc"
        )
        hb_task = asyncio.create_task(
            broker.heartbeat_loop(
                SOURCE,
                interval=5.0,
                stop=stop,
                on_tick=lambda: logger.debug("heartbeat"),
            ),
            name="paper-heartbeat",
        )
        book_interval = _book_interval()
        book_tasks = [
            _watch(task)
            for inst in exchange.list_instruments()
            for task in (
                asyncio.create_task(
                    _pump_order_book(broker, exchange, inst.symbol, stop),
                    name=f"paper-book-{inst.symbol}",
                ),
                asyncio.create_task(
                    _tick_order_book(
                        broker, exchange, inst.symbol, stop, book_interval
                    ),
                    name=f"paper-book-tick-{inst.symbol}",
                ),
            )
        ]
        logger.info(
            "Paper publishing order books on change and every %.1fs", book_interval
        )
        book = exchange.get_order_book("BTCUSDT")
        logger.info(
            "Paper engine started BTCUSDT bids=%s asks=%s",
            [(str(lvl.price), str(lvl.qty)) for lvl in book.bids],
            [(str(lvl.price), str(lvl.qty)) for lvl in book.asks],
        )
        try:
            await stop.wait()
        finally:
            stop.set()
            for task in (rpc_task, hb_task, *book_tasks):
                task.cancel()
            await asyncio.gather(
                rpc_task, hb_task, *book_tasks, return_exceptions=True
            )
            await public.close()
            await exchange.stop()
    logger.info("Paper engine stopped")


def main() -> None:
    configure_logging(SOURCE)
    asyncio.run(amain())
