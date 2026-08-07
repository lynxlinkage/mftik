"""Gate spot as a PublicClient — five live feeds over WS, snapshots over REST."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import httpx
import pytest
from gate_stub import FakeGate
from mft.exchange.gate.spot import channels as ch
from mft.exchange.gate.spot.client import GateSpotWebSocket
from mft.exchange.gate.spot.public import GateSpotPublicClient
from mft.exchange.gate.spot.rest import GateRestError, GateSpotPublicRest
from mft.exchange.intervals import InvalidIntervalError

CURRENCY_PAIRS = [
    {
        "id": "BTC_USDT",
        "base": "BTC",
        "quote": "USDT",
        "min_base_amount": "0.0001",
        "min_quote_amount": "1",
        "amount_precision": 4,
        "precision": 2,
        "trade_status": "tradable",
    },
    {
        "id": "OLD_USDT",
        "base": "OLD",
        "quote": "USDT",
        "amount_precision": 3,
        "precision": 6,
        "trade_status": "untradable",
    },
]

TICKER_ROW = {
    "currency_pair": "BTC_USDT",
    "last": "60000",
    "lowest_ask": "60001",
    "highest_bid": "59999",
    "base_volume": "100",
    "quote_volume": "6000000",
}

ORDER_BOOK_ROW = {
    "id": 123,
    "current": 1_700_000_000_500,
    "update": 1_700_000_000_400,
    "asks": [["60001", "1"], ["60002", "2"]],
    "bids": [["59999", "3"], ["59998", "4"]],
}

#: ``/spot/candlesticks`` rows, in Gate's own column order — timestamp, quote
#: volume, **close**, high, low, open, base volume, closed. Every value here is
#: distinct so a transposed index cannot pass by coincidence.
CANDLESTICK_ROWS = [
    ["1700000000", "6000000", "60500", "60900", "59900", "60100", "100", "true"],
    # The window in progress: Gate marks it open, and older rows predate the
    # eighth column entirely.
    ["1700000060", "3000000", "60700", "60800", "60400", "60500", "50", "false"],
]


class FakePublicRest:
    """httpx MockTransport standing in for Gate's public REST v4."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.routes: dict[str, Any] = {
            "/api/v4/spot/currency_pairs": CURRENCY_PAIRS,
            "/api/v4/spot/tickers": [TICKER_ROW],
            "/api/v4/spot/order_book": ORDER_BOOK_ROW,
            "/api/v4/spot/candlesticks": CANDLESTICK_ROWS,
        }

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        for route, payload in self.routes.items():
            if request.url.path.startswith(route):
                return httpx.Response(200, json=payload)
        return httpx.Response(
            404, json={"label": "NOT_FOUND", "message": request.url.path}
        )

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(self.handler),
            base_url="https://api.gateio.ws",
        )


class StubResolver:
    """Stands in for the symbol plane: an exact, two-way lookup table."""

    def __init__(self) -> None:
        self.native = {"BTCUSDT": "BTC_USDT", "ETHUSDT": "ETH_USDT"}
        self.canonical = {v: k for k, v in self.native.items()}
        self.lookups = 0

    async def exch_ticker(
        self, venue: str, symbol: str, *, category: str = "spot"
    ) -> str:
        self.lookups += 1
        return self.native[symbol]

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str = "spot"
    ) -> str:
        return self.canonical[exch_ticker]


@pytest.fixture
def resolver() -> StubResolver:
    return StubResolver()


@pytest.fixture
def rest_stub() -> FakePublicRest:
    return FakePublicRest()


async def _public(
    gate: FakeGate,
    rest_stub: FakePublicRest,
    resolver: StubResolver,
) -> GateSpotPublicClient:
    return GateSpotPublicClient(
        symbols=resolver,
        ws=GateSpotWebSocket(url=gate.url, ping_interval=0),  # type: ignore[attr-defined]
        rest=GateSpotPublicRest(client=rest_stub.client()),
    )


# --- snapshots (REST) ------------------------------------------------------


async def test_fetch_instruments_drops_untradable_and_maps_precision(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    async with await _public(gate, rest_stub, resolver) as client:
        instruments = await client.fetch_instruments()

    assert [i.symbol for i in instruments] == ["BTC_USDT"]
    btc = instruments[0]
    assert (btc.base, btc.quote) == ("BTC", "USDT")
    # precision 2 → 0.01 price step; amount_precision 4 → 0.0001 size step.
    assert btc.tick_size == Decimal("0.01")
    assert btc.lot_size == Decimal("0.0001")
    assert btc.min_qty == Decimal("0.0001")
    assert btc.min_notional == Decimal("1")


async def test_fetch_instruments_stays_in_the_venue_spelling(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    """The plane is built from this, so it cannot depend on the plane."""
    async with await _public(gate, rest_stub, resolver) as client:
        instruments = await client.fetch_instruments()

    assert instruments[0].symbol == "BTC_USDT"
    assert resolver.lookups == 0


async def test_fetch_ticker_resolves_the_pair_and_comes_back_canonical(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    async with await _public(gate, rest_stub, resolver) as client:
        ticker = await client.fetch_ticker("BTCUSDT")

    assert ticker.symbol == "BTCUSDT"
    assert ticker.bid == Decimal("59999")
    assert ticker.ask == Decimal("60001")
    assert ticker.last == Decimal("60000")
    request = rest_stub.requests[-1]
    assert request.url.params["currency_pair"] == "BTC_USDT"


async def test_fetch_order_book_stamps_the_canonical_symbol(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    """Gate's reply carries no pair at all, so one has to be put back on."""
    async with await _public(gate, rest_stub, resolver) as client:
        book = await client.fetch_order_book("BTCUSDT", depth=5)

    assert book.symbol == "BTCUSDT"
    assert book.bids[0].price == Decimal("59999")
    assert book.asks[0].qty == Decimal("1")
    assert book.ts == 1_700_000_000.5
    assert rest_stub.requests[-1].url.params["limit"] == "5"


async def test_fetch_klines_reads_gates_column_order(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    """Close comes before high/low/open in the row; OHLC order would silently
    swap the four."""
    async with await _public(gate, rest_stub, resolver) as client:
        klines = await client.fetch_klines("BTCUSDT", "1m", limit=2)

    first = klines[0]
    assert first.open_time == 1_700_000_000
    assert first.open == Decimal("60100")
    assert first.high == Decimal("60900")
    assert first.low == Decimal("59900")
    assert first.close == Decimal("60500")
    # Gate splits the two volumes; base is index 6, quote is index 1.
    assert first.volume == Decimal("100")
    assert first.quote_volume == Decimal("6000000")
    assert first.closed is True


async def test_fetch_klines_marks_the_window_in_progress_open(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    async with await _public(gate, rest_stub, resolver) as client:
        klines = await client.fetch_klines("BTCUSDT", "1m")

    assert [k.closed for k in klines] == [True, False]


async def test_fetch_klines_treats_a_missing_closed_column_as_open(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    """The column was added late. Absent, "still open" is the safe reading —
    a candle wrongly called closed is appended and never revised."""
    rest_stub.routes["/api/v4/spot/candlesticks"] = [
        ["1700000000", "6000000", "60500", "60900", "59900", "60100", "100"]
    ]
    async with await _public(gate, rest_stub, resolver) as client:
        klines = await client.fetch_klines("BTCUSDT", "1m")

    assert klines[0].closed is False


async def test_fetch_klines_rejects_a_short_row(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    rest_stub.routes["/api/v4/spot/candlesticks"] = [["1700000000", "6000000"]]
    async with await _public(gate, rest_stub, resolver) as client:
        with pytest.raises(GateRestError):
            await client.fetch_klines("BTCUSDT", "1m")


async def test_fetch_klines_resolves_symbol_and_comes_back_canonical(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    async with await _public(gate, rest_stub, resolver) as client:
        klines = await client.fetch_klines("BTCUSDT", "1m", limit=2)

    assert rest_stub.requests[-1].url.params["currency_pair"] == "BTC_USDT"
    assert rest_stub.requests[-1].url.params["limit"] == "2"
    # Gate answered about BTC_USDT; nothing in its spelling escapes.
    assert {k.symbol for k in klines} == {"BTCUSDT"}


async def test_fetch_klines_translates_the_month_and_stamps_it_back(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    """The one interval Gate genuinely spells differently: it refuses ``1M``
    and serves the month as ``30d``."""
    async with await _public(gate, rest_stub, resolver) as client:
        klines = await client.fetch_klines("BTCUSDT", "1mo")

    assert rest_stub.requests[-1].url.params["interval"] == "30d"
    # ...and the canonical spelling is what comes back, not Gate's.
    assert {k.interval for k in klines} == {"1mo"}


async def test_fetch_klines_normalizes_before_translating(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    async with await _public(gate, rest_stub, resolver) as client:
        klines = await client.fetch_klines("BTCUSDT", " 1H ")

    assert rest_stub.requests[-1].url.params["interval"] == "1h"
    assert {k.interval for k in klines} == {"1h"}


async def test_fetch_klines_refuses_an_interval_gate_does_not_serve(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    """Refused here, before the round trip — the table is what knows."""
    before = len(rest_stub.requests)
    async with await _public(gate, rest_stub, resolver) as client:
        with pytest.raises(InvalidIntervalError):
            await client.fetch_klines("BTCUSDT", "2w")

    assert len(rest_stub.requests) == before


async def test_fetch_klines_refuses_capital_m_months(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    async with await _public(gate, rest_stub, resolver) as client:
        with pytest.raises(InvalidIntervalError):
            await client.fetch_klines("BTCUSDT", "1M")


async def test_rest_error_surfaces(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    rest_stub.routes = {"/api/v4/spot/tickers": []}
    async with await _public(gate, rest_stub, resolver) as client:
        with pytest.raises(GateRestError):
            await client.fetch_ticker("BTCUSDT")


# --- streams (WebSocket) ---------------------------------------------------


async def test_stream_ticker(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    async with await _public(gate, rest_stub, resolver) as client:
        stream = client.stream_ticker("BTCUSDT")
        task = asyncio.create_task(anext(stream))
        await _wait_for_subs(client)
        assert gate.frames_for(ch.TICKERS)[0]["payload"] == ["BTC_USDT"]

        await gate.push(ch.TICKERS, TICKER_ROW)
        ticker = await asyncio.wait_for(task, timeout=2.0)

    assert ticker.symbol == "BTCUSDT"
    assert ticker.bid == Decimal("59999")


async def test_stream_trades(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    async with await _public(gate, rest_stub, resolver) as client:
        stream = client.stream_trades("BTCUSDT")
        task = asyncio.create_task(anext(stream))
        await _wait_for_subs(client)

        await gate.push(
            ch.TRADES,
            [
                {
                    "id": 7,
                    "create_time": 1648725035,
                    "create_time_ms": "1648725035923.0",
                    "side": "sell",
                    "currency_pair": "BTC_USDT",
                    "amount": "0.5",
                    "price": "60000",
                }
            ],
        )
        trade = await asyncio.wait_for(task, timeout=2.0)

    assert trade.symbol == "BTCUSDT"
    assert trade.trade_id == "7"
    assert trade.qty == Decimal("0.5")
    assert trade.side == "sell"


async def test_stream_order_book_is_a_full_snapshot(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    """MD gets whole books, so the diff channel is deliberately not used."""
    async with await _public(gate, rest_stub, resolver) as client:
        stream = client.stream_order_book("BTCUSDT")
        task = asyncio.create_task(anext(stream))
        await _wait_for_subs(client)
        assert gate.frames_for(ch.ORDER_BOOK)[0]["payload"] == [
            "BTC_USDT",
            "20",
            "1000ms",
        ]

        await gate.push(
            ch.ORDER_BOOK,
            {
                "t": 1_700_000_000_500,
                "lastUpdateId": 9,
                "s": "BTC_USDT",
                "bids": [["59999", "3"]],
                "asks": [["60001", "1"]],
            },
        )
        book = await asyncio.wait_for(task, timeout=2.0)

    assert book.symbol == "BTCUSDT"
    assert book.bids[0].price == Decimal("59999")
    assert book.asks[0].price == Decimal("60001")
    assert not gate.frames_for(ch.ORDER_BOOK_UPDATE)


async def test_stream_kline(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    async with await _public(gate, rest_stub, resolver) as client:
        stream = client.stream_kline("BTCUSDT", "1m")
        task = asyncio.create_task(anext(stream))
        await _wait_for_subs(client)
        # Gate wants the interval first on this channel.
        assert gate.frames_for(ch.CANDLESTICKS)[0]["payload"] == [
            "1m",
            "BTC_USDT",
        ]

        await gate.push(
            ch.CANDLESTICKS,
            {
                "t": "1700000000",
                "v": "6000000",
                "c": "60500",
                "h": "60800",
                "l": "59900",
                "o": "60000",
                "n": "1m_BTC_USDT",
                "a": "100",
                "w": True,
            },
        )
        kline = await asyncio.wait_for(task, timeout=2.0)

    assert kline.symbol == "BTCUSDT"
    assert kline.interval == "1m"
    assert kline.open_time == 1_700_000_000.0
    assert (kline.open, kline.high, kline.low, kline.close) == (
        Decimal("60000"),
        Decimal("60800"),
        Decimal("59900"),
        Decimal("60500"),
    )
    # ``a`` is base amount, ``v`` is quote turnover — not the same number.
    assert kline.volume == Decimal("100")
    assert kline.quote_volume == Decimal("6000000")
    assert kline.closed is True


async def test_stream_best_quote(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    async with await _public(gate, rest_stub, resolver) as client:
        stream = client.stream_best_quote("BTCUSDT")
        task = asyncio.create_task(anext(stream))
        await _wait_for_subs(client)

        await gate.push(
            ch.BOOK_TICKER,
            {
                "t": 1_700_000_000_500,
                "u": 42,
                "s": "BTC_USDT",
                "b": "59999",
                "B": "3",
                "a": "60001",
                "A": "1.5",
            },
        )
        quote = await asyncio.wait_for(task, timeout=2.0)

    assert quote.symbol == "BTCUSDT"
    assert quote.bid == Decimal("59999")
    assert quote.bid_qty == Decimal("3")
    assert quote.ask == Decimal("60001")
    assert quote.ask_qty == Decimal("1.5")
    assert quote.ts == 1_700_000_000.5


async def test_streams_are_filtered_per_pair(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    """Gate multiplexes per channel, not per pair.

    MD opens one feed per (topic, symbol), so two streams on the same channel
    both see every pair's pushes. Without a filter here the ETH tape would come
    out of the BTC feed.
    """
    async with await _public(gate, rest_stub, resolver) as client:
        btc = client.stream_trades("BTCUSDT")
        eth = client.stream_trades("ETHUSDT")
        btc_task = asyncio.create_task(anext(btc))
        eth_task = asyncio.create_task(anext(eth))
        await _wait_for_subs(client, count=2)

        for pair, price in (("ETH_USDT", "3000"), ("BTC_USDT", "60000")):
            await gate.push(
                ch.TRADES,
                [
                    {
                        "id": 1,
                        "create_time": 1648725035,
                        "create_time_ms": "1648725035923.0",
                        "side": "buy",
                        "currency_pair": pair,
                        "amount": "1",
                        "price": price,
                    }
                ],
            )

        btc_trade = await asyncio.wait_for(btc_task, timeout=2.0)
        eth_trade = await asyncio.wait_for(eth_task, timeout=2.0)

    assert (btc_trade.symbol, btc_trade.price) == ("BTCUSDT", Decimal("60000"))
    assert (eth_trade.symbol, eth_trade.price) == ("ETHUSDT", Decimal("3000"))


async def test_klines_are_filtered_per_interval(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    """Two intervals on one pair share the channel too."""
    async with await _public(gate, rest_stub, resolver) as client:
        stream = client.stream_kline("BTCUSDT", "5m")
        task = asyncio.create_task(anext(stream))
        await _wait_for_subs(client)

        base = {
            "t": "1700000000",
            "c": "1",
            "h": "1",
            "l": "1",
            "o": "1",
            "a": "1",
            "v": "1",
        }
        await gate.push(ch.CANDLESTICKS, dict(base, n="1m_BTC_USDT"))
        await gate.push(ch.CANDLESTICKS, dict(base, n="5m_BTC_USDT", c="2"))
        kline = await asyncio.wait_for(task, timeout=2.0)

    assert kline.interval == "5m"
    assert kline.close == Decimal("2")


async def test_stream_unhooks_itself_when_the_consumer_stops(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    """MD ends a feed by dropping the iterator; the socket must forget it."""
    async with await _public(gate, rest_stub, resolver) as client:
        stream = client.stream_trades("BTCUSDT")
        task = asyncio.create_task(anext(stream))
        await _wait_for_subs(client)
        assert len(client.ws._subs) == 1  # noqa: SLF001

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await stream.aclose()

        assert client.ws._subs == []  # noqa: SLF001


async def test_streams_need_a_connection(
    gate: FakeGate, rest_stub: FakePublicRest, resolver: StubResolver
) -> None:
    from mft.exchange.errors import ExchangeNotConnectedError

    client = await _public(gate, rest_stub, resolver)
    with pytest.raises(ExchangeNotConnectedError):
        client.stream_order_book("BTCUSDT")


async def _wait_for_subs(
    client: GateSpotPublicClient, *, count: int = 1, timeout: float = 2.0
) -> None:
    """Wait until the socket has ``count`` streams registered.

    Not the server's received frames: the client only registers a stream once
    the ack comes back, so a push sent in that window lands nowhere.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if len(client.ws._subs) >= count:  # noqa: SLF001
            return
        await asyncio.sleep(0.01)
    raise TimeoutError(f"only {len(client.ws._subs)} subs registered")  # noqa: SLF001
