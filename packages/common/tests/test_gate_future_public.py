"""Gate futures public connector — five feeds plus liquidation."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import httpx
import pytest
from gate_future_stub import FakeGateFutures
from mftik.exchange.gate.future import channels as ch
from mftik.exchange.gate.future.client import GateFuturesWebSocket
from mftik.exchange.gate.future.public import GateFuturesPublicClient
from mftik.exchange.gate.future.rest import GateFuturesPublicRest
from mftik.exchange.models import Side
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("GateFutures_Perp_BTCUSDT")
CS = Decimal("0.0001")


class StubResolver:
    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return "BTC_USDT"

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        return UniversalTicker.of(venue, category, "BTCUSDT")

    async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
        return CS


class FakePublicRest:
    def __init__(self) -> None:
        self.routes: dict[str, Any] = {
            "/api/v4/futures/usdt/tickers": [
                {"contract": "BTC_USDT", "last": "60000"}
            ],
        }

    def handler(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json=self.routes.get(request.url.path, {})
        )

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(self.handler),
            base_url="https://api.gateio.ws",
        )


async def _public(gate: FakeGateFutures) -> GateFuturesPublicClient:
    return GateFuturesPublicClient(
        symbols=StubResolver(),
        ws=GateFuturesWebSocket(url=gate.url, ping_interval=0),  # type: ignore[attr-defined]
        rest=GateFuturesPublicRest(client=FakePublicRest().client()),
    )


async def _wait_sub(gate: FakeGateFutures, channel: str) -> None:
    for _ in range(50):
        if gate.frames_for(channel):
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"no subscribe for {channel}")


async def test_five_feeds_and_liquidation(gate_futures: FakeGateFutures) -> None:
    client = await _public(gate_futures)
    async with client:
        ticker_task = asyncio.create_task(anext(client.stream_ticker(TICKER)))
        await _wait_sub(gate_futures, ch.TICKERS)
        await gate_futures.push(
            ch.TICKERS, [{"contract": "BTC_USDT", "last": "60000"}]
        )
        ticker = await asyncio.wait_for(ticker_task, timeout=2)
        assert ticker.last == Decimal("60000")

        trade_task = asyncio.create_task(anext(client.stream_trades(TICKER)))
        await _wait_sub(gate_futures, ch.TRADES)
        await gate_futures.push(
            ch.TRADES,
            [
                {
                    "id": 1,
                    "contract": "BTC_USDT",
                    "size": "-10",
                    "price": "60000",
                    "create_time": 1_700_000_000,
                }
            ],
        )
        trade = await asyncio.wait_for(trade_task, timeout=2)
        assert trade.qty == Decimal("0.001")
        assert trade.side is Side.SELL

        book_task = asyncio.create_task(anext(client.stream_order_book(TICKER)))
        await _wait_sub(gate_futures, ch.ORDER_BOOK)
        await gate_futures.push(
            ch.ORDER_BOOK,
            {
                "s": "BTC_USDT",
                "t": 1_700_000_000_000,
                "bids": [["59999", "20"]],
                "asks": [["60001", "10"]],
            },
        )
        book = await asyncio.wait_for(book_task, timeout=2)
        assert book.bids[0].qty == Decimal("0.002")

        quote_task = asyncio.create_task(anext(client.stream_best_quote(TICKER)))
        await _wait_sub(gate_futures, ch.BOOK_TICKER)
        await gate_futures.push(
            ch.BOOK_TICKER,
            {"s": "BTC_USDT", "b": "59999", "B": "5", "a": "60001", "A": "6", "t": 1},
        )
        quote = await asyncio.wait_for(quote_task, timeout=2)
        assert quote.bid_qty == Decimal("0.0005")

        kline_task = asyncio.create_task(anext(client.stream_kline(TICKER, "1m")))
        await _wait_sub(gate_futures, ch.CANDLESTICKS)
        await gate_futures.push(
            ch.CANDLESTICKS,
            {
                "t": 1_700_000_000,
                "o": "1",
                "h": "2",
                "l": "0.5",
                "c": "1.5",
                "v": "100",
                "sum": "150",
                "n": "1m_BTC_USDT",
                "w": True,
            },
        )
        kline = await asyncio.wait_for(kline_task, timeout=2)
        assert kline.volume == Decimal("0.01")
        assert kline.closed

        liq_task = asyncio.create_task(anext(client.stream_liquidation(TICKER)))
        await _wait_sub(gate_futures, ch.PUBLIC_LIQUIDATES)
        await gate_futures.push(
            ch.PUBLIC_LIQUIDATES,
            [{"contract": "BTC_USDT", "price": "215", "size": "-10", "time": 1}],
        )
        liq = await asyncio.wait_for(liq_task, timeout=2)
        assert liq.side is Side.BUY
        assert liq.qty == Decimal("0.001")


async def test_no_aggtrade_method(gate_futures: FakeGateFutures) -> None:
    client = await _public(gate_futures)
    assert not hasattr(client, "stream_agg_trades")


async def test_wrong_venue_ticker_is_refused(gate_futures: FakeGateFutures) -> None:
    client = await _public(gate_futures)
    async with client:
        with pytest.raises(ValueError, match="Gate ticker"):
            await anext(
                client.stream_ticker(UniversalTicker.parse("Gate_Spot_BTCUSDT"))
            )
