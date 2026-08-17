"""The futures feed — two sockets, and which subscribe lands on which.

Since the endpoint split there is no single market-streams host, and a
subscribe on the wrong one is accepted and then silent. These tests use two
separate stub servers for exactly that reason: a single shared stub would pass
whether or not the routing works.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from binance_stub import FakeBinanceStream
from mftik.exchange.binance.future import streams as st
from mftik.exchange.binance.future.feed import BinanceFutureStream


def _feed(
    public: FakeBinanceStream, market: FakeBinanceStream
) -> BinanceFutureStream:
    return BinanceFutureStream(
        public_url=public.url,  # type: ignore[attr-defined]
        market_url=market.url,  # type: ignore[attr-defined]
        keepalive=0,
        retry_backoff=0.01,
    )


async def test_nothing_is_dialled_until_something_subscribes(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    """A feed reading only the book should not hold a socket to the other host."""
    async with _feed(future_public_stream, future_market_stream) as feed:
        assert future_public_stream.connections == 0
        assert future_market_stream.connections == 0

        await feed.subscribe_order_book("BTCUSDT")
        await asyncio.sleep(0.05)
        assert future_public_stream.connections == 1
        assert future_market_stream.connections == 0


async def test_the_book_goes_to_public_and_the_tape_to_market(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    async with _feed(future_public_stream, future_market_stream) as feed:
        await feed.subscribe_order_book("BTCUSDT")
        await feed.subscribe_book_tickers("BTCUSDT")
        await feed.subscribe_agg_trades("BTCUSDT")
        await feed.subscribe_klines("1m", "BTCUSDT")
        await feed.subscribe_liquidations("BTCUSDT")
        await asyncio.sleep(0.05)

    assert future_public_stream.subscribed == {
        "btcusdt@depth20@100ms",
        "btcusdt@bookTicker",
    }
    assert future_market_stream.subscribed == {
        "btcusdt@aggTrade",
        "btcusdt@kline_1m",
        "btcusdt@forceOrder",
    }


async def test_a_push_reaches_the_stream_that_asked_for_it(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    async with _feed(future_public_stream, future_market_stream) as feed:
        books = await feed.subscribe_order_book("BTCUSDT")
        trades = await feed.subscribe_agg_trades("BTCUSDT")
        book_pump = asyncio.ensure_future(anext(books))
        trade_pump = asyncio.ensure_future(anext(trades))
        await asyncio.sleep(0.05)

        await future_public_stream.push(
            "btcusdt@depth20@100ms",
            {
                "e": "depthUpdate",
                "E": 1,
                "T": 1,
                "s": "BTCUSDT",
                "U": 1,
                "u": 2,
                "pu": 0,
                "b": [["40000", "1"]],
                "a": [["40001", "2"]],
            },
        )
        await future_market_stream.push(
            "btcusdt@aggTrade",
            {
                "e": "aggTrade",
                "E": 2,
                "s": "BTCUSDT",
                "a": 5,
                "p": "40000",
                "q": "1",
                "T": 2,
                "m": False,
            },
        )
        book = await asyncio.wait_for(book_pump, timeout=2.0)
        trade = await asyncio.wait_for(trade_pump, timeout=2.0)

    assert book.s == "BTCUSDT", "the payload arrived parsed, not as a dict"
    assert book.bid_levels()[0].price == Decimal("40000")
    assert trade.p == Decimal("40000")


async def test_one_subscription_cannot_span_two_endpoints(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    """They arrive on different connections; one stream cannot read both."""
    async with _feed(future_public_stream, future_market_stream) as feed:
        with pytest.raises(ValueError, match="endpoints"):
            await feed.subscribe_raw("btcusdt@bookTicker", "btcusdt@aggTrade")


async def test_an_unknown_stream_name_is_refused_before_it_is_sent(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    async with _feed(future_public_stream, future_market_stream) as feed:
        with pytest.raises(st.UnknownStreamError):
            await feed.subscribe_raw("btcusdt@compositeIndex")
    assert future_market_stream.received == []


async def test_unsubscribing_goes_to_the_socket_that_carries_it(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    async with _feed(future_public_stream, future_market_stream) as feed:
        await feed.subscribe_agg_trades("BTCUSDT")
        await feed.subscribe_book_tickers("BTCUSDT")
        await asyncio.sleep(0.05)
        await feed.unsubscribe("btcusdt@aggTrade")
        await asyncio.sleep(0.05)

        assert future_market_stream.subscribed == set()
        assert future_public_stream.subscribed == {"btcusdt@bookTicker"}


async def test_a_reconnect_replays_only_the_streams_that_socket_carried(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    """Each connection restores itself; neither knows about the other's names."""
    async with _feed(future_public_stream, future_market_stream) as feed:
        await feed.subscribe_book_tickers("BTCUSDT")
        await feed.subscribe_agg_trades("BTCUSDT")
        await asyncio.sleep(0.05)

        await future_market_stream.drop()
        for _ in range(50):
            await asyncio.sleep(0.05)
            if future_market_stream.connections > 1:
                break

        assert future_market_stream.connections == 2
        assert future_public_stream.connections == 1, "the other socket was fine"
        replays = future_market_stream.frames_for(st.SUBSCRIBE)
        assert replays[-1]["params"] == ["btcusdt@aggTrade"]
