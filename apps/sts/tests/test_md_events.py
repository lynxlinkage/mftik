"""STS market-data fan-out — one hook per md feed topic."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange.models import BestQuote, BookLevel, Kline, OrderBook, Ticker, Trade
from mft.protocol import (
    MD_BEST_QUOTE,
    MD_KLINE,
    MD_ORDERBOOK,
    MD_TICKER,
    MD_TRADE,
    Topics,
    UntypedEnvelope,
)
from mft_sts.session.session import StsSession
from mft_sts.strategy import Strategy


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-md-events"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


class RecordingStrategy(Strategy):
    name = "recording"
    id = 98

    def __init__(self) -> None:
        super().__init__()
        self.seen: dict[str, list] = {
            "ticker": [],
            "order_book": [],
            "kline": [],
            "trade": [],
            "best_quote": [],
        }

    async def on_ticker(self, ticker: Ticker) -> None:
        self.seen["ticker"].append(ticker)

    async def on_order_book(self, book: OrderBook) -> None:
        self.seen["order_book"].append(book)

    async def on_kline(self, kline: Kline) -> None:
        self.seen["kline"].append(kline)

    async def on_trade(self, trade: Trade) -> None:
        self.seen["trade"].append(trade)

    async def on_best_quote(self, quote: BestQuote) -> None:
        self.seen["best_quote"].append(quote)


def _payloads() -> list[tuple[str, str, dict]]:
    """(md message type, hook key, wire payload) for every wired feed."""
    return [
        (
            MD_TICKER,
            "ticker",
            Ticker(
                symbol="BTCUSDT",
                bid=Decimal("100"),
                ask=Decimal("101"),
                last=Decimal("100.5"),
            ).model_dump(mode="json"),
        ),
        (
            MD_ORDERBOOK,
            "order_book",
            OrderBook(
                symbol="BTCUSDT",
                bids=[BookLevel(price=Decimal("100"), qty=Decimal("1"))],
                asks=[BookLevel(price=Decimal("101"), qty=Decimal("2"))],
            ).model_dump(mode="json"),
        ),
        (
            MD_KLINE,
            "kline",
            Kline(
                symbol="BTCUSDT",
                interval="1m",
                open_time=1_700_000_000.0,
                open=Decimal("100"),
                high=Decimal("105"),
                low=Decimal("99"),
                close=Decimal("104"),
                volume=Decimal("3"),
                closed=True,
            ).model_dump(mode="json"),
        ),
        (
            MD_TRADE,
            "trade",
            Trade(
                trade_id="t1",
                symbol="BTCUSDT",
                price=Decimal("100.5"),
                qty=Decimal("0.5"),
                side="buy",
            ).model_dump(mode="json"),
        ),
        (
            MD_BEST_QUOTE,
            "best_quote",
            BestQuote(
                symbol="BTCUSDT",
                bid=Decimal("100"),
                bid_qty=Decimal("1"),
                ask=Decimal("101"),
                ask_qty=Decimal("2"),
            ).model_dump(mode="json"),
        ),
    ]


@pytest.mark.asyncio
async def test_md_events_reach_every_hook(broker: Broker) -> None:
    """Each md.* type on md.{session_id} lands in its own strategy hook."""
    session_id = "sts-md-hooks"
    strategy = RecordingStrategy()
    sts = StsSession(
        session_id=session_id,
        broker=broker,
        created_by=1,
        strategy=strategy,
        md_ids=["paper.orderbook.BTCUSDT"],
        heartbeat_interval=0.1,
    )
    await sts.start()
    await asyncio.sleep(0.05)

    cases = _payloads()
    for msg_type, _key, payload in cases:
        await broker.publish(
            Topics.md_session(session_id),
            UntypedEnvelope.wrap(payload, type=msg_type, source="md"),
        )

    await _wait_until(
        lambda: all(strategy.seen[key] for _t, key, _p in cases)
    )
    for _msg_type, key, _payload in cases:
        assert len(strategy.seen[key]) == 1, key
        assert strategy.seen[key][0].symbol == "BTCUSDT"

    assert strategy.seen["kline"][0].interval == "1m"
    assert strategy.seen["kline"][0].closed is True
    assert strategy.seen["best_quote"][0].ask_qty == Decimal("2")

    await sts.stop()


@pytest.mark.asyncio
async def test_md_hook_failure_does_not_kill_the_pump(broker: Broker) -> None:
    """A raising hook is logged and the next message still gets through."""
    session_id = "sts-md-raise"

    class BrokenStrategy(RecordingStrategy):
        async def on_ticker(self, ticker: Ticker) -> None:
            raise RuntimeError("boom")

    strategy = BrokenStrategy()
    sts = StsSession(
        session_id=session_id,
        broker=broker,
        created_by=1,
        strategy=strategy,
        md_ids=["paper.ticker.BTCUSDT"],
        heartbeat_interval=0.1,
    )
    await sts.start()
    await asyncio.sleep(0.05)

    by_type = {t: p for t, _k, p in _payloads()}
    for msg_type in (MD_TICKER, MD_TRADE):
        await broker.publish(
            Topics.md_session(session_id),
            UntypedEnvelope.wrap(
                by_type[msg_type], type=msg_type, source="md"
            ),
        )

    await _wait_until(lambda: bool(strategy.seen["trade"]))
    assert not strategy.seen["ticker"]

    await sts.stop()


async def _wait_until(pred, *, timeout: float = 3.0) -> None:  # noqa: ANN001
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.02)
    raise TimeoutError("condition not met")
