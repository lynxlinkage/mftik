"""Paper engine process — owns PaperExchange, serves RPC, publishes streams."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from mft import configure_logging
from mft.broker import Broker
from mft.exchange import PaperExchange
from mft.exchange.models import Balance, Fill, Order, OrderBook
from mft.protocol import (
    PAPER_BALANCE,
    PAPER_FILL,
    PAPER_ORDER,
    PAPER_ORDER_BOOK,
    Topics,
    UntypedEnvelope,
)

from mft_paper.accounts import LIQUIDITY_ORDERS, SEEDED_ACCOUNTS
from mft_paper.rpc import dispatch

SOURCE = "paper"
logger = logging.getLogger(SOURCE)


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


async def _pump_order_book(
    broker: Broker,
    public: Any,
    symbol: str,
    stop: asyncio.Event,
) -> None:
    """Bridge in-process public order-book stream → Redis."""
    from mft.exchange.paper.public import PaperPublicClient

    assert isinstance(public, PaperPublicClient)
    topic = Topics.paper_order_book(symbol)
    async for book in public.stream_order_book(symbol):
        if stop.is_set():
            return
        assert isinstance(book, OrderBook)
        try:
            await broker.publish(
                topic,
                UntypedEnvelope.wrap(
                    book.model_dump(mode="json"),
                    type=PAPER_ORDER_BOOK,
                    source="paper",
                ),
            )
        except Exception:
            logger.exception(
                "paper orderbook publish failed symbol=%s", symbol
            )
            return


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
        book_tasks = [
            asyncio.create_task(
                _pump_order_book(broker, public, inst.symbol, stop),
                name=f"paper-book-{inst.symbol}",
            )
            for inst in exchange.list_instruments()
        ]
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
