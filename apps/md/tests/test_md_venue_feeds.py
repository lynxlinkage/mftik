"""MD venue feed-topic resolution — topic → venue stream + wire type."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from mft.exchange.models import (
    BestQuote,
    BookLevel,
    Kline,
    OrderBook,
    Ticker,
    Trade,
)
from mft.protocol import (
    MD_BEST_QUOTE,
    MD_KLINE,
    MD_ORDERBOOK,
    MD_TICKER,
    MD_TRADE,
    UntypedEnvelope,
)
from mft_md.session.venue import VenueSession


class FakePublic:
    """A venue connector: publishes one item per stream, then holds it open.

    Inherits nothing. That is the point of the shape MD declares — a connector
    satisfies it by having the methods, not by being registered anywhere.
    """

    name = "fake"

    def __init__(self) -> None:
        self.kline_calls: list[tuple[str, str]] = []
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def _once(self, item) -> AsyncIterator:  # noqa: ANN001
        yield item
        await asyncio.Event().wait()

    def stream_ticker(self, symbol: str) -> AsyncIterator[Ticker]:
        return self._once(
            Ticker(
                symbol=symbol,
                bid=Decimal("100"),
                ask=Decimal("101"),
                last=Decimal("100.5"),
            )
        )

    def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        return self._once(
            Trade(
                symbol=symbol,
                price=Decimal("100"),
                qty=Decimal("1"),
                side="buy",
            )
        )

    def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        return self._once(
            OrderBook(
                symbol=symbol,
                bids=[BookLevel(price=Decimal("100"), qty=Decimal("1"))],
                asks=[BookLevel(price=Decimal("101"), qty=Decimal("1"))],
            )
        )

    def stream_kline(self, symbol: str, interval: str) -> AsyncIterator[Kline]:
        self.kline_calls.append((symbol, interval))
        return self._once(
            Kline(
                symbol=symbol,
                interval=interval,
                open_time=1_700_000_000.0,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100.5"),
            )
        )

    def stream_best_quote(self, symbol: str) -> AsyncIterator[BestQuote]:
        return self._once(
            BestQuote(
                symbol=symbol,
                bid=Decimal("100"),
                bid_qty=Decimal("1"),
                ask=Decimal("101"),
                ask_qty=Decimal("2"),
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("topic", "msg_type"),
    [
        ("orderbook", MD_ORDERBOOK),
        ("ticker", MD_TICKER),
        ("trade", MD_TRADE),
        ("bestquote", MD_BEST_QUOTE),
        ("kline_1m", MD_KLINE),
    ],
)
async def test_feed_topic_publishes_its_message_type(
    topic: str, msg_type: str
) -> None:
    seen: list[tuple[str, str, str, UntypedEnvelope]] = []

    async def _on_update(venue, topic_, symbol, env) -> None:  # noqa: ANN001
        seen.append((venue, topic_, symbol, env))

    sess = VenueSession("fake", FakePublic(), on_update=_on_update)
    await sess.start()
    await sess.ensure_feed(topic, "BTCUSDT")

    await _wait_until(lambda: bool(seen))
    venue, topic_, symbol, env = seen[0]
    assert (venue, topic_, symbol) == ("fake", topic, "BTCUSDT")
    assert env.type == msg_type
    assert env.payload["symbol"] == "BTCUSDT"

    await sess.stop()


@pytest.mark.asyncio
async def test_kline_topic_carries_the_interval() -> None:
    public = FakePublic()
    sess = VenueSession("fake", public, on_update=_noop_update)
    await sess.start()
    await sess.ensure_feed("kline_15m", "ETHUSDT")
    assert public.kline_calls == [("ETHUSDT", "15m")]
    await sess.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("topic", ["depth", "kline_", ""])
async def test_unknown_topic_is_rejected_at_subscribe(topic: str) -> None:
    sess = VenueSession("fake", FakePublic(), on_update=_noop_update)
    await sess.start()
    with pytest.raises(ValueError):
        await sess.ensure_feed(topic, "BTCUSDT")
    assert sess.feed_count == 0
    await sess.stop()


@pytest.mark.asyncio
async def test_unsupported_stream_is_rejected_at_subscribe() -> None:
    """A venue that does not publish the feed fails the subscribe, not a task."""

    class NoKlines(FakePublic):
        """Does not publish candles, so it simply has no method for them."""

        stream_kline = None

    sess = VenueSession("fake", NoKlines(), on_update=_noop_update)
    await sess.start()
    with pytest.raises(ValueError, match="does not publish stream_kline"):
        await sess.ensure_feed("kline_1m", "BTCUSDT")
    assert sess.feed_count == 0
    await sess.stop()


async def _noop_update(venue, topic, symbol, env) -> None:  # noqa: ANN001
    return None


async def _wait_until(pred, *, timeout: float = 3.0) -> None:  # noqa: ANN001
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.02)
    raise TimeoutError("condition not met")
