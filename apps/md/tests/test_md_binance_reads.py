"""Binance on MD's read side — the reader, and the venue's error table.

The reader is the one place a venue difference shows through the fetch plane:
Gate answers on-demand reads over REST, Binance over its WebSocket API. Both
have to answer alike, so what is tested here is that they do.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from binance_stub import FakeBinanceApi
from mft.exchange.binance.spot import methods as m
from mft.exchange.binance.spot.client import BinanceSpotWsApi
from mft.exchange.binance.spot.protocol import BinanceWsError
from mft.exchange.intervals import InvalidIntervalError
from mft.exchange.tickers import Category, UniversalTicker
from mft.protocol.query_codes import QueryCode, is_normalized
from mft_md.errors import VENUES, normalize
from mft_md.fetch.readers import (
    BinanceSpotReader,
    NoReaderError,
    VenueReaderFactory,
)
from websockets.asyncio.server import serve

TICKER = UniversalTicker.parse("Binance_Spot_BTCUSDT")
NATIVE = "BTC-USDT"

BINANCE = "Binance"

KLINE_ROW = [
    1499040000000, "1", "2", "0.5", "1.5", "10",
    1499040059999, "20", 5, "1", "1", "0",
]


class StubSymbols:
    """A symbol plane whose venue spelling differs from the canonical one."""

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return NATIVE

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        return UniversalTicker.of(venue, category, "BTCUSDT")


@pytest.fixture
async def binance_api():
    """A FakeBinanceApi on an ephemeral port. No credentials: reads are open."""
    fake = FakeBinanceApi()
    server = await serve(fake.handler, "127.0.0.1", 0)
    fake.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    yield fake
    server.close()
    await server.wait_closed()


def _reader(stub: FakeBinanceApi) -> BinanceSpotReader:
    return BinanceSpotReader(
        symbols=StubSymbols(),
        api=BinanceSpotWsApi(url=stub.url, keepalive=0),  # type: ignore[attr-defined]
    )


# --- the reader ------------------------------------------------------------


async def test_klines_translate_both_spellings_each_way(
    binance_api: FakeBinanceApi,
) -> None:
    binance_api.results[m.KLINES] = [KLINE_ROW]
    reader = _reader(binance_api)
    await reader.connect()
    try:
        klines = await reader.fetch_klines(TICKER, " 1MO ", limit=5)
    finally:
        await reader.close()

    params = binance_api.call(m.KLINES)["params"]
    assert params["symbol"] == NATIVE
    assert params["interval"] == "1M", "Binance's month spelling goes on the wire"
    assert klines[0].symbol == "BTCUSDT"
    assert klines[0].interval == "1mo", "and ours comes back"
    assert klines[0].close == Decimal("1.5")


async def test_an_interval_binance_does_not_serve_is_refused_before_the_call(
    binance_api: FakeBinanceApi,
) -> None:
    """Gate serves ``10s`` candles; Binance does not, and says so first."""
    reader = _reader(binance_api)
    await reader.connect()
    try:
        with pytest.raises(InvalidIntervalError, match="no 10s candles"):
            await reader.fetch_klines(TICKER, "10s", limit=5)
    finally:
        await reader.close()

    assert binance_api.calls(m.KLINES) == []


async def test_order_book_comes_back_under_the_canonical_symbol(
    binance_api: FakeBinanceApi,
) -> None:
    binance_api.results[m.DEPTH] = {
        "lastUpdateId": 1,
        "bids": [["39999", "1"]],
        "asks": [["40001", "2"]],
    }
    reader = _reader(binance_api)
    await reader.connect()
    try:
        book = await reader.fetch_order_book(TICKER, depth=5)
    finally:
        await reader.close()

    assert book.symbol == "BTCUSDT"
    assert binance_api.call(m.DEPTH)["params"]["symbol"] == NATIVE
    assert book.bids[0].price == Decimal("39999")


async def test_best_quote_is_read_out_of_a_depth_one_book(
    binance_api: FakeBinanceApi,
) -> None:
    binance_api.results[m.DEPTH] = {
        "lastUpdateId": 1,
        "bids": [["39999", "1.5"]],
        "asks": [["40001", "2.5"]],
    }
    reader = _reader(binance_api)
    await reader.connect()
    try:
        quote = await reader.fetch_best_quote(TICKER)
    finally:
        await reader.close()

    assert quote is not None
    assert quote.symbol == "BTCUSDT"
    assert (quote.bid, quote.bid_qty) == (Decimal("39999"), Decimal("1.5"))
    assert (quote.ask, quote.ask_qty) == (Decimal("40001"), Decimal("2.5"))
    assert binance_api.call(m.DEPTH)["params"]["limit"] == 1


async def test_an_empty_side_declines_to_answer_rather_than_quoting_zero(
    binance_api: FakeBinanceApi,
) -> None:
    """A zero bid would answer "can my price rest here" wrongly."""
    binance_api.results[m.DEPTH] = {"lastUpdateId": 1, "bids": [], "asks": []}
    reader = _reader(binance_api)
    await reader.connect()
    try:
        assert await reader.fetch_best_quote(TICKER) is None
    finally:
        await reader.close()


async def test_another_venues_ticker_is_refused(
    binance_api: FakeBinanceApi,
) -> None:
    reader = _reader(binance_api)
    await reader.connect()
    try:
        gate = UniversalTicker.of("Gate", Category.SPOT, "BTCUSDT")
        with pytest.raises(ValueError, match="was handed a Gate ticker"):
            await reader.fetch_order_book(gate, depth=5)
    finally:
        await reader.close()


# --- the factory -----------------------------------------------------------


async def test_the_factory_builds_a_binance_reader() -> None:
    factory = VenueReaderFactory(StubSymbols())  # type: ignore[arg-type]
    reader = await factory.create("Binance")
    assert isinstance(reader, BinanceSpotReader)
    assert reader.venue == "Binance"
    # Reads are open at Binance, so the socket carries no credentials.
    assert not reader.api.authenticated


async def test_a_venue_with_no_reader_still_refuses_by_name() -> None:
    factory = VenueReaderFactory(StubSymbols())  # type: ignore[arg-type]
    with pytest.raises(NoReaderError):
        await factory.create("Kraken")


# --- the error table -------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (-1121, QueryCode.VENUE_SYMBOL_NOT_FOUND),
        (-2016, QueryCode.VENUE_SYMBOL_NOT_FOUND),
        (-1120, QueryCode.VENUE_INVALID_PARAM),
        (-1130, QueryCode.VENUE_INVALID_PARAM),
        (-1003, QueryCode.VENUE_RATE_LIMITED),
        (-1008, QueryCode.VENUE_INTERNAL_ERROR),
    ],
)
def test_a_known_binance_code_becomes_a_query_code(
    code: int, expected: QueryCode
) -> None:
    assert normalize(BinanceWsError(code, "nope"), venue=BINANCE) is expected


def test_an_unmapped_binance_code_survives_as_itself() -> None:
    """Today's native code is tomorrow's 2xx; nothing in between changes."""
    assert normalize(BinanceWsError(-9999, "brand new"), venue=BINANCE) == -9999
    assert not is_normalized(-9999)


def test_an_interval_refusal_is_ours_not_the_venues() -> None:
    """It never reached Binance, so it must not read as a venue answer."""
    code = normalize(
        InvalidIntervalError("Binance serves no 10s candles"), venue=BINANCE
    )
    assert code is QueryCode.MD_INTERVAL_NOT_SUPPORTED


def test_a_call_that_never_completed_is_not_read_as_a_refusal() -> None:
    """A timeout carries no code; for a read that means "ask again"."""
    code = normalize(BinanceWsError(None, "no reply within 10.0s"), venue=BINANCE)
    assert code is QueryCode.MD_VENUE_CALL_FAILED


def test_binance_is_a_registered_venue_in_the_table() -> None:
    from mft.exchange import venues

    assert BINANCE in VENUES
    assert set(VENUES) <= set(venues.names())
