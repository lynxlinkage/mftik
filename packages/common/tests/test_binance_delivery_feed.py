"""The COIN-M feed — one dstream socket, every subscribe on it."""

from __future__ import annotations

import asyncio
from decimal import Decimal

from binance_stub import FakeBinanceStream
from mftik.exchange.binance.delivery import streams as st
from mftik.exchange.binance.delivery.feed import BinanceDeliveryStream


def _feed(stub: FakeBinanceStream) -> BinanceDeliveryStream:
    return BinanceDeliveryStream(
        url=stub.url,  # type: ignore[attr-defined]
        keepalive=0,
        retry_backoff=0.01,
    )


async def test_every_md_topic_lands_on_the_same_socket(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream) as feed:
        await feed.subscribe_order_book("BTCUSD_PERP")
        await feed.subscribe_book_tickers("BTCUSD_PERP")
        await feed.subscribe_agg_trades("BTCUSD_PERP")
        await feed.subscribe_klines("1m", "BTCUSD_PERP")
        await feed.subscribe_liquidations("BTCUSD_PERP")
        await asyncio.sleep(0.05)

    assert binance_stream.connections == 1
    assert binance_stream.subscribed == {
        "btcusd_perp@depth20@100ms",
        "btcusd_perp@bookTicker",
        "btcusd_perp@aggTrade",
        "btcusd_perp@kline_1m",
        "btcusd_perp@forceOrder",
    }


async def test_a_push_reaches_the_stream_that_asked_for_it(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream) as feed:
        books = await feed.subscribe_order_book("BTCUSD_PERP")
        trades = await feed.subscribe_agg_trades("BTCUSD_PERP")
        book_pump = asyncio.ensure_future(anext(books))
        trade_pump = asyncio.ensure_future(anext(trades))
        await asyncio.sleep(0.05)

        await binance_stream.push(
            "btcusd_perp@depth20@100ms",
            {
                "e": "depthUpdate",
                "E": 1672515782136,
                "T": 1672515782000,
                "s": "BTCUSD_PERP",
                "U": 1,
                "u": 2,
                "pu": 0,
                "b": [["39999", "3"]],
                "a": [["40001", "4"]],
            },
        )
        await binance_stream.push(
            "btcusd_perp@aggTrade",
            {
                "e": "aggTrade",
                "E": 1672515782136,
                "s": "BTCUSD_PERP",
                "a": 1,
                "p": "40000",
                "q": "2",
                "f": 1,
                "l": 1,
                "T": 1672515782136,
                "m": False,
            },
        )
        book = await asyncio.wait_for(book_pump, timeout=2.0)
        trade = await asyncio.wait_for(trade_pump, timeout=2.0)

    assert book.s == "BTCUSD_PERP"
    assert book.bid_levels()[0].price == Decimal("39999")
    assert trade.q == Decimal("2")


async def test_unsubscribing_drops_only_the_named_stream(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream) as feed:
        await feed.subscribe_agg_trades("BTCUSD_PERP")
        await feed.subscribe_book_tickers("BTCUSD_PERP")
        await asyncio.sleep(0.05)
        await feed.unsubscribe("btcusd_perp@aggTrade")
        await asyncio.sleep(0.05)

        assert binance_stream.subscribed == {"btcusd_perp@bookTicker"}
        assert binance_stream.frames_for(st.UNSUBSCRIBE)


async def test_two_consumers_share_one_venue_subscription(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream) as feed:
        first, second = await asyncio.gather(
            feed.subscribe_book_tickers("BTCUSD_PERP"),
            feed.subscribe_book_tickers("BTCUSD_PERP"),
        )
        await asyncio.sleep(0.05)
        assert len(binance_stream.frames_for(st.SUBSCRIBE)) == 1
        await binance_stream.push(
            "btcusd_perp@bookTicker",
            {
                "e": "bookTicker",
                "u": 1,
                "s": "BTCUSD_PERP",
                "b": "39999",
                "B": "3",
                "a": "40001",
                "A": "4",
                "T": 1672515782136,
                "E": 1672515782136,
            },
        )
        assert (await asyncio.wait_for(anext(first), timeout=2.0)).s == "BTCUSD_PERP"
        assert (await asyncio.wait_for(anext(second), timeout=2.0)).s == "BTCUSD_PERP"
