"""Venue instrument sources — conversion to canonical instruments."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest
from mftik.exchange.tickers import Category
from mftik_sym.sources import (
    PaperInstrumentSource,
    default_sources,
    tick_from_precision,
)
from mftik_sym.sources.binance import BinanceSpotInstrumentSource
from mftik_sym.sources.binance_delivery import BinanceDeliveryInstrumentSource
from mftik_sym.sources.binance_future import BinanceFutureInstrumentSource
from mftik_sym.sources.bitget import BitgetInstrumentSource
from mftik_sym.sources.bybit import BybitInstrumentSource
from mftik_sym.sources.deribit import DeribitInstrumentSource
from mftik_sym.sources.gate import GateSpotInstrumentSource
from mftik_sym.sources.gate_future import GateFuturesInstrumentSource
from mftik_sym.sources.okx import OkxInstrumentSource

# Trimmed rows in Gate's /spot/currency_pairs shape.
GATE_ROWS = [
    {
        "id": "BTC_USDT",
        "base": "BTC",
        "quote": "USDT",
        "fee": "0.2",
        "min_base_amount": "0.0001",
        "max_base_amount": "1000",
        "min_quote_amount": "1",
        "max_quote_amount": None,
        "amount_precision": 4,
        "precision": 2,
        "trade_status": "tradable",
    },
    {
        "id": "GT_USDT",
        "base": "GT",
        "quote": "USDT",
        "min_base_amount": None,
        "max_base_amount": None,
        "min_quote_amount": None,
        "max_quote_amount": None,
        "amount_precision": 3,
        "precision": 4,
        "trade_status": "untradable",
    },
    # Malformed — missing base; must be skipped, not crash the refresh.
    {"id": "BAD_USDT", "quote": "USDT", "trade_status": "tradable"},
]


def _source(rows: list[dict]) -> GateSpotInstrumentSource:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v4/spot/currency_pairs"
        return httpx.Response(200, json=rows)

    return GateSpotInstrumentSource(
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://api.gateio.ws",
        )
    )


@pytest.mark.parametrize(
    ("precision", "expected"),
    [(0, Decimal("1")), (2, Decimal("0.01")), (8, Decimal("1E-8")), (None, None)],
)
def test_precision_becomes_a_step_size(precision, expected) -> None:
    assert tick_from_precision(precision) == expected


async def test_gate_rows_become_canonical_instruments() -> None:
    instruments = await _source(GATE_ROWS).fetch()

    assert [i.symbol for i in instruments] == ["BTCUSDT", "GTUSDT"]
    btc = instruments[0]
    # Canonical is exact here: base and quote came from the venue, so no
    # suffix guessing is involved.
    assert btc.symbol == "BTCUSDT"
    assert btc.base == "BTC"
    assert btc.quote == "USDT"
    assert btc.exch_ticker == "BTC_USDT"
    assert str(btc.ticker) == "Gate_Spot_BTCUSDT"
    assert btc.category is Category.SPOT
    assert btc.is_active


async def test_gate_precision_maps_to_tick_and_step() -> None:
    btc = (await _source(GATE_ROWS).fetch())[0]

    assert btc.filters["price_tick"] == Decimal("0.01")  # precision 2
    assert btc.filters["qty_step"] == Decimal("0.0001")  # amount_precision 4
    assert btc.filters["min_qty"] == Decimal("0.0001")
    assert btc.filters["max_qty"] == Decimal("1000")
    assert btc.filters["min_notional"] == Decimal("1")


async def test_unbounded_filter_is_published_with_no_value() -> None:
    """Gate's null means "no limit", which differs from not publishing it."""
    btc = (await _source(GATE_ROWS).fetch())[0]

    assert "max_notional" in btc.filters
    assert btc.filters["max_notional"] is None


async def test_untradable_pairs_are_marked_inactive() -> None:
    gt = (await _source(GATE_ROWS).fetch())[1]
    assert gt.symbol == "GTUSDT"
    assert not gt.is_active


async def test_malformed_rows_are_skipped() -> None:
    """One bad row must not fail the whole venue refresh."""
    instruments = await _source(GATE_ROWS).fetch()
    assert "BAD" not in {i.base for i in instruments}
    assert len(instruments) == 2


async def test_http_failure_propagates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"label": "SERVER_ERROR"})

    source = GateSpotInstrumentSource(
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://api.gateio.ws",
        )
    )
    with pytest.raises(httpx.HTTPStatusError):
        await source.fetch()


# --- Binance ---------------------------------------------------------------

# Trimmed rows in Binance's /api/v3/exchangeInfo shape.
BINANCE_ROWS = [
    {
        "symbol": "BTCUSDT",
        "status": "TRADING",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "filters": [
            {
                "filterType": "PRICE_FILTER",
                "minPrice": "0.01000000",
                "maxPrice": "1000000.00000000",
                "tickSize": "0.01000000",
            },
            {
                "filterType": "LOT_SIZE",
                "minQty": "0.00001000",
                "maxQty": "9000.00000000",
                "stepSize": "0.00001000",
            },
            {
                "filterType": "NOTIONAL",
                "minNotional": "5.00000000",
                # Present but unenforced — Binance writes zero, not null.
                "maxNotional": "0.00000000",
            },
        ],
    },
    {
        "symbol": "HALTUSDT",
        "status": "HALT",
        "baseAsset": "HALT",
        "quoteAsset": "USDT",
        # An older symbol, on the superseded filter name.
        "filters": [{"filterType": "MIN_NOTIONAL", "minNotional": "10.00000000"}],
    },
    # Malformed — no baseAsset; must be skipped, not crash the refresh.
    {"symbol": "BADUSDT", "status": "TRADING", "quoteAsset": "USDT"},
]


def _binance(rows: list[dict]) -> BinanceSpotInstrumentSource:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v3/exchangeInfo"
        return httpx.Response(200, json={"symbols": rows})

    return BinanceSpotInstrumentSource(
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://api.binance.com",
        )
    )


async def test_binance_rows_become_canonical_instruments() -> None:
    instruments = await _binance(BINANCE_ROWS).fetch()

    assert [i.symbol for i in instruments] == ["BTCUSDT", "HALTUSDT"]
    btc = instruments[0]
    assert btc.base == "BTC"
    assert btc.quote == "USDT"
    # Binance spells spot pairs the canonical way, which is a coincidence and
    # not a rule — the plane still stores both spellings.
    assert btc.exch_ticker == "BTCUSDT"
    assert str(btc.ticker) == "Binance_Spot_BTCUSDT"
    assert btc.category is Category.SPOT
    assert btc.is_active


async def test_binance_steps_come_out_of_the_filter_list() -> None:
    btc = (await _binance(BINANCE_ROWS).fetch())[0]

    assert btc.filters["price_tick"] == Decimal("0.01")
    assert btc.filters["qty_step"] == Decimal("0.00001")
    assert btc.filters["min_qty"] == Decimal("0.00001")
    assert btc.filters["max_qty"] == Decimal("9000")
    assert btc.filters["min_notional"] == Decimal("5")
    assert btc.filters["max_price"] == Decimal("1000000")


async def test_a_step_is_stored_at_its_granularity_not_binances_formatting() -> None:
    """Binance pads its filters (``"0.00010000"``); the scale then propagates.

    Asserted on the written form, because ``Decimal`` equality cannot see it:
    ``Decimal("0.00001000") == Decimal("0.00001")`` is true, so the assertions
    above pass either way. A padded step is what floored a size to
    ``0.00780000`` and drew a live ``-1111``.
    """
    btc = (await _binance(BINANCE_ROWS).fetch())[0]

    assert str(btc.filters["qty_step"]) == "0.00001"
    assert str(btc.filters["price_tick"]) == "0.01"
    assert str(btc.filters["min_notional"]) == "5"
    # A whole-number bound must not come back exponential either.
    assert str(btc.filters["max_qty"]) == "9000"


async def test_a_zero_bound_reads_as_unbounded_not_as_zero() -> None:
    """Binance writes ``0`` for a filter it publishes but does not enforce."""
    btc = (await _binance(BINANCE_ROWS).fetch())[0]

    assert "max_notional" in btc.filters
    assert btc.filters["max_notional"] is None


async def test_the_superseded_min_notional_filter_is_still_read() -> None:
    halt = (await _binance(BINANCE_ROWS).fetch())[1]
    assert halt.filters["min_notional"] == Decimal("10")


async def test_symbols_that_are_not_trading_are_marked_inactive() -> None:
    halt = (await _binance(BINANCE_ROWS).fetch())[1]
    assert halt.symbol == "HALTUSDT"
    assert not halt.is_active


async def test_malformed_binance_rows_are_skipped() -> None:
    instruments = await _binance(BINANCE_ROWS).fetch()
    assert "BAD" not in {i.base for i in instruments}
    assert len(instruments) == 2


async def test_binance_http_failure_propagates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"code": -1008, "msg": "Server busy"})

    source = BinanceSpotInstrumentSource(
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://api.binance.com",
        )
    )
    with pytest.raises(httpx.HTTPStatusError):
        await source.fetch()


class FakePaperPublic:
    """Stands in for the paper engine's instrument listing over IPC."""

    def __init__(self, rows) -> None:
        self.rows = rows
        self.closed = False

    async def fetch_instruments(self):
        return self.rows

    async def close(self) -> None:
        self.closed = True


async def test_paper_source_reads_the_engines_own_filters() -> None:
    """Paper is pulled, not restated, so it cannot drift from the engine."""
    from mftik.exchange import PaperExchange

    rows = PaperExchange().list_instruments()
    source = PaperInstrumentSource(None, public=FakePaperPublic(rows))

    instruments = await source.fetch()
    by_symbol = {i.symbol: i for i in instruments}

    assert set(by_symbol) == {"BTCUSDT", "ETHUSDT"}
    btc = by_symbol["BTCUSDT"]
    # Paper already spells pairs the canonical way.
    assert btc.exch_ticker == "BTCUSDT"
    assert btc.base == "BTC" and btc.quote == "USDT"
    assert btc.filters["price_tick"] == Decimal("0.01")
    assert btc.filters["qty_step"] == Decimal("0.00001")
    assert btc.filters["min_notional"] == Decimal("5")
    # ETH has a coarser lot size — the source must not flatten them.
    assert by_symbol["ETHUSDT"].filters["qty_step"] == Decimal("0.0001")


# --- Bybit -----------------------------------------------------------------

# Trimmed rows in Bybit's /v5/market/instruments-info shape.
BYBIT_SPOT_ROWS = [
    {
        "symbol": "BTCUSDT",
        "baseCoin": "BTC",
        "quoteCoin": "USDT",
        "status": "Trading",
        "lotSizeFilter": {
            "basePrecision": "0.000001",
            "quotePrecision": "0.00000001",
            "minOrderQty": "0.000048",
            "maxOrderQty": "71.73956243",
            "minOrderAmt": "1",
            "maxOrderAmt": "2000000",
        },
        "priceFilter": {"tickSize": "0.01"},
    },
    {
        "symbol": "SOONUSDT",
        "baseCoin": "SOON",
        "quoteCoin": "USDT",
        "status": "PreLaunch",
        "lotSizeFilter": {"basePrecision": "0.01", "minOrderQty": "1"},
        "priceFilter": {"tickSize": "0.0001"},
    },
    # Malformed — no base coin; must be skipped, not crash the refresh.
    {"symbol": "BADUSDT", "quoteCoin": "USDT", "status": "Trading"},
]

BYBIT_LINEAR_ROWS = [
    {
        "symbol": "BTCUSDT",
        "contractType": "LinearPerpetual",
        "baseCoin": "BTC",
        "quoteCoin": "USDT",
        "settleCoin": "USDT",
        "status": "Trading",
        "lotSizeFilter": {
            "qtyStep": "0.001",
            "minOrderQty": "0.001",
            "maxOrderQty": "1190",
            "minNotionalValue": "5",
        },
        "priceFilter": {"minPrice": "0.10", "maxPrice": "1999999.8",
                        "tickSize": "0.10"},
    },
    {
        # USDC-quoted perpetual: the venue ticker is nothing like the pair.
        "symbol": "BTCPERP",
        "contractType": "LinearPerpetual",
        "baseCoin": "BTC",
        "quoteCoin": "USDC",
        "settleCoin": "USDC",
        "status": "Trading",
        "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
        "priceFilter": {"tickSize": "0.1"},
    },
    {
        # A dated future, listed on the same book as the perpetuals — and
        # sharing the first row's base and quote, so the two canonicalize to
        # the same symbol.
        "symbol": "BTCUSDT-25DEC26",
        "contractType": "LinearFutures",
        "baseCoin": "BTC",
        "quoteCoin": "USDT",
        "settleCoin": "USDT",
        "status": "Trading",
        "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
        "priceFilter": {"tickSize": "0.1"},
    },
]


def _bybit(
    pages: list[list[dict]],
    *,
    category: Category = Category.SPOT,
) -> BybitInstrumentSource:
    """A source over ``pages``, handed out one cursor page at a time."""
    remaining = list(pages)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v5/market/instruments-info"
        seen.append(request)
        rows = remaining.pop(0) if remaining else []
        return httpx.Response(
            200,
            json={
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": rows,
                    "nextPageCursor": "next" if remaining else "",
                },
            },
        )

    source = BybitInstrumentSource(
        category=category,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://api.bybit.com",
        ),
    )
    source.requests = seen  # type: ignore[attr-defined]
    return source


async def test_bybit_spot_rows_become_canonical_instruments() -> None:
    instruments = await _bybit([BYBIT_SPOT_ROWS]).fetch()

    assert [i.symbol for i in instruments] == ["BTCUSDT", "SOONUSDT"]
    btc = instruments[0]
    assert btc.base == "BTC" and btc.quote == "USDT"
    assert btc.exch_ticker == "BTCUSDT"
    assert str(btc.ticker) == "Bybit_Spot_BTCUSDT"
    assert btc.is_active
    # Spot settles in its quote currency; repeating that says nothing.
    assert btc.settlement_asset is None


async def test_bybit_spot_filters_read_the_spot_spellings() -> None:
    btc = (await _bybit([BYBIT_SPOT_ROWS]).fetch())[0]

    assert btc.filters["price_tick"] == Decimal("0.01")
    # ``basePrecision`` is what spot calls the quantity step.
    assert btc.filters["qty_step"] == Decimal("0.000001")
    assert btc.filters["min_qty"] == Decimal("0.000048")
    assert btc.filters["min_notional"] == Decimal("1")
    assert btc.filters["max_notional"] == Decimal("2000000")
    # Published on the contract books only; the key stays so a caller can tell
    # "unbounded" from "not published".
    assert "min_price" in btc.filters
    assert btc.filters["min_price"] is None


async def test_bybit_prelaunch_symbols_are_marked_inactive() -> None:
    soon = (await _bybit([BYBIT_SPOT_ROWS]).fetch())[1]
    assert soon.symbol == "SOONUSDT"
    assert not soon.is_active


async def test_malformed_bybit_rows_are_skipped() -> None:
    instruments = await _bybit([BYBIT_SPOT_ROWS]).fetch()
    assert "BAD" not in {i.base for i in instruments}
    assert len(instruments) == 2


async def test_bybit_perp_filters_read_the_contract_spellings() -> None:
    instruments = await _bybit(
        [BYBIT_LINEAR_ROWS], category=Category.PERP
    ).fetch()
    btc = instruments[0]

    assert str(btc.ticker) == "Bybit_Perp_BTCUSDT"
    # ``qtyStep`` and ``minNotionalValue`` are the contract books' names for
    # what spot calls ``basePrecision`` and ``minOrderAmt``.
    assert btc.filters["qty_step"] == Decimal("0.001")
    assert btc.filters["min_notional"] == Decimal("5")
    assert btc.filters["min_price"] == Decimal("0.1")
    assert btc.filters["max_price"] == Decimal("1999999.8")
    assert btc.settlement_asset == "USDT"


async def test_a_dated_future_is_not_published_as_a_perp() -> None:
    """Bybit's linear book lists both, and a future's base and quote are the
    perpetual's — so one stored as a Perp would not merely be mislabelled, it
    would overwrite ``Bybit_Perp_BTCUSDT`` with a December 2026 contract's
    ``exch_ticker`` and send every perp order there."""
    instruments = await _bybit(
        [BYBIT_LINEAR_ROWS], category=Category.PERP
    ).fetch()

    assert [i.exch_ticker for i in instruments] == ["BTCUSDT", "BTCPERP"]
    # One canonical symbol per ticker, which is the property the filter buys.
    assert len({str(i.ticker) for i in instruments}) == len(instruments)
    # The USDC perpetual survives, spelled by the venue in a way no split of
    # its symbol would have recovered.
    assert instruments[1].symbol == "BTCUSDC"
    assert str(instruments[1].ticker) == "Bybit_Perp_BTCUSDC"


async def test_bybit_pagination_is_followed_to_the_end() -> None:
    """A refresh that read one page would publish a fraction of the venue and
    then deactivate everything it did not see."""
    source = _bybit([BYBIT_SPOT_ROWS[:1], BYBIT_SPOT_ROWS[1:]])
    instruments = await source.fetch()

    assert [i.symbol for i in instruments] == ["BTCUSDT", "SOONUSDT"]
    requests = source.requests  # type: ignore[attr-defined]
    assert len(requests) == 2
    assert "cursor" not in requests[0].url.query.decode()
    assert "cursor=next" in requests[1].url.query.decode()


async def test_bybit_http_failure_propagates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"retCode": 10016, "retMsg": "busy"})

    source = BybitInstrumentSource(
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://api.bybit.com",
        )
    )
    with pytest.raises(httpx.HTTPStatusError):
        await source.fetch()


# --- Binance USDⓈ-M futures ------------------------------------------------

#: Trimmed rows in ``fapi/v1/exchangeInfo``'s shape. The second is a dated
#: future, which shares the perpetual's base and quote; its canonical symbol
#: is the pair, a hyphen, and ``YYMMDD`` so the two tickers cannot collide.
BINANCE_FUTURE_ROWS = [
    {
        "symbol": "BTCUSDT",
        "pair": "BTCUSDT",
        "contractType": "PERPETUAL",
        "status": "TRADING",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "filters": [
            {
                "filterType": "PRICE_FILTER",
                "tickSize": "0.10",
                "minPrice": "556.80",
                "maxPrice": "4529764",
            },
            {
                "filterType": "LOT_SIZE",
                "stepSize": "0.00100000",
                "minQty": "0.001",
                "maxQty": "1000",
            },
            {"filterType": "MIN_NOTIONAL", "notional": "100"},
        ],
    },
    {
        "symbol": "BTCUSDT_250926",
        "pair": "BTCUSDT",
        "contractType": "CURRENT_QUARTER",
        "status": "TRADING",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "deliveryDate": 1758873600000,
        "filters": [],
    },
    {
        "symbol": "SOONUSDT",
        "contractType": "PERPETUAL",
        "status": "PENDING_TRADING",
        "baseAsset": "SOON",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "filters": [],
    },
    # Malformed — no baseAsset; must be skipped, not crash the refresh.
    {
        "symbol": "BADUSDT",
        "contractType": "PERPETUAL",
        "status": "TRADING",
        "quoteAsset": "USDT",
    },
]


def _binance_future(
    rows: list[dict], *, category: Category = Category.PERP
) -> BinanceFutureInstrumentSource:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/fapi/v1/exchangeInfo"
        return httpx.Response(200, json={"symbols": rows})

    return BinanceFutureInstrumentSource(
        category=category,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://fapi.binance.com",
        ),
    )


async def test_binance_future_rows_become_perp_instruments() -> None:
    instruments = await _binance_future(BINANCE_FUTURE_ROWS).fetch()

    btc = instruments[0]
    assert str(btc.ticker) == "BinanceUM_Perp_BTCUSDT"
    assert btc.category is Category.PERP
    # The settlement currency, which spot has no field for because the quote
    # currency is the settlement there.
    assert btc.settlement_asset == "USDT"
    assert btc.filters["min_notional"] == Decimal("100")
    # Padding is the venue's formatting, not the granularity.
    assert str(btc.filters["qty_step"]) == "0.001"


async def test_dated_futures_are_not_stored_as_perpetuals() -> None:
    """A perp refresh must not ingest the quarterly.

    ``BTCUSDT_250926`` shares the perpetual's base and quote. Written under
    ``Perp`` it would steal ``BinanceUM_Perp_BTCUSDT`` and every perp
    order would route to a contract that expires.
    """
    instruments = await _binance_future(BINANCE_FUTURE_ROWS).fetch()

    assert [i.exch_ticker for i in instruments] == ["BTCUSDT", "SOONUSDT"]


async def test_dated_futures_become_future_instruments() -> None:
    instruments = await _binance_future(
        BINANCE_FUTURE_ROWS, category=Category.FUTURE
    ).fetch()

    assert [i.exch_ticker for i in instruments] == ["BTCUSDT_250926"]
    btc = instruments[0]
    assert str(btc.ticker) == "BinanceUM_Future_BTCUSDT-250926"
    assert btc.symbol == "BTCUSDT-250926"
    assert btc.category is Category.FUTURE
    assert btc.settlement_asset == "USDT"
    assert btc.expiry is not None
    assert btc.expiry.year == 2025
    assert btc.expiry.month == 9
    assert btc.expiry.day == 26


async def test_a_contract_not_yet_trading_is_listed_but_inactive() -> None:
    soon = (await _binance_future(BINANCE_FUTURE_ROWS).fetch())[1]
    assert soon.symbol == "SOONUSDT"
    assert not soon.is_active


async def test_malformed_binance_future_rows_are_skipped() -> None:
    instruments = await _binance_future(BINANCE_FUTURE_ROWS).fetch()
    assert "BAD" not in {i.base for i in instruments}


# --- Binance COIN-M futures ------------------------------------------------

#: Pinned to a live ``GET /dapi/v1/exchangeInfo`` row (2026-08-29):
#: ``contractStatus`` not ``status``, ``contractSize`` an unquoted int, no
#: ``MIN_NOTIONAL``. The dated quarterly shares the perp's base and quote.
BINANCE_DELIVERY_ROWS = [
    {
        "symbol": "BTCUSD_PERP",
        "pair": "BTCUSD",
        "contractType": "PERPETUAL",
        "contractStatus": "TRADING",
        "contractSize": 100,
        "baseAsset": "BTC",
        "quoteAsset": "USD",
        "marginAsset": "BTC",
        "filters": [
            {
                "filterType": "PRICE_FILTER",
                "tickSize": "0.1",
                "minPrice": "1000",
                "maxPrice": "4520958",
            },
            {
                "filterType": "LOT_SIZE",
                "stepSize": "1",
                "minQty": "1",
                "maxQty": "1000000",
            },
        ],
    },
    {
        "symbol": "BTCUSD_260925",
        "pair": "BTCUSD",
        "contractType": "CURRENT_QUARTER",
        "contractStatus": "TRADING",
        "contractSize": 100,
        "baseAsset": "BTC",
        "quoteAsset": "USD",
        "marginAsset": "BTC",
        "filters": [],
    },
    {
        "symbol": "SOONUSD_PERP",
        "contractType": "PERPETUAL",
        "contractStatus": "PENDING_TRADING",
        "contractSize": 10,
        "baseAsset": "SOON",
        "quoteAsset": "USD",
        "marginAsset": "SOON",
        "filters": [],
    },
    # Malformed — no baseAsset and no contractSize; must be skipped.
    {
        "symbol": "BADUSD_PERP",
        "contractType": "PERPETUAL",
        "contractStatus": "TRADING",
        "quoteAsset": "USD",
    },
]


def _binance_delivery(
    rows: list[dict], *, category: Category = Category.INVERSE
) -> BinanceDeliveryInstrumentSource:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/dapi/v1/exchangeInfo"
        return httpx.Response(200, json={"symbols": rows})

    return BinanceDeliveryInstrumentSource(
        category=category,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://dapi.binance.com",
        )
    )


async def test_binance_delivery_rows_become_perp_instruments() -> None:
    instruments = await _binance_delivery(BINANCE_DELIVERY_ROWS).fetch()

    btc = instruments[0]
    assert str(btc.ticker) == "BinanceCM_Inverse_BTCUSD"
    assert btc.exch_ticker == "BTCUSD_PERP"
    assert btc.category is Category.INVERSE
    assert btc.contract_size == Decimal("100")
    assert btc.settlement_asset == "BTC"
    assert btc.filters["min_notional"] is None
    assert btc.filters["qty_step"] == Decimal("1")
    assert btc.filters["min_qty"] == Decimal("1")


async def test_dated_delivery_contracts_are_not_stored_as_perpetuals() -> None:
    """``BTCUSD_260925`` canonicalizes to ``BTCUSD`` — the perpetual's symbol."""
    instruments = await _binance_delivery(BINANCE_DELIVERY_ROWS).fetch()

    assert [i.exch_ticker for i in instruments] == ["BTCUSD_PERP", "SOONUSD_PERP"]


async def test_dated_delivery_contracts_become_future_instruments() -> None:
    instruments = await _binance_delivery(
        BINANCE_DELIVERY_ROWS, category=Category.FUTURE
    ).fetch()

    assert [i.exch_ticker for i in instruments] == ["BTCUSD_260925"]
    btc = instruments[0]
    assert str(btc.ticker) == "BinanceCM_Future_BTCUSD-260925"
    assert btc.symbol == "BTCUSD-260925"
    assert btc.category is Category.FUTURE
    assert btc.contract_size == Decimal("100")
    assert btc.settlement_asset == "BTC"
    assert btc.expiry is not None
    assert btc.expiry.year == 2026
    assert btc.expiry.month == 9
    assert btc.expiry.day == 25


async def test_a_delivery_contract_not_yet_trading_is_listed_but_inactive() -> None:
    soon = (await _binance_delivery(BINANCE_DELIVERY_ROWS).fetch())[1]
    assert soon.symbol == "SOONUSD"
    assert not soon.is_active


async def test_malformed_binance_delivery_rows_are_skipped() -> None:
    instruments = await _binance_delivery(BINANCE_DELIVERY_ROWS).fetch()
    assert "BAD" not in {i.base for i in instruments}


GATE_FUTURE_ROWS = [
    {
        "name": "BTC_USDT",
        "type": "direct",
        "quanto_multiplier": "0.0001",
        "order_price_round": "0.1",
        # Gate publishes these as JSON numbers, not strings.
        "order_size_min": 1,
        "order_size_max": 1000000,
        "settle": "usdt",
        "in_delisting": False,
        "expire_time": 0,
    },
    {
        "name": "BTC_USDT_20250926",
        "type": "direct",
        "quanto_multiplier": "0.0001",
        "order_price_round": "0.1",
        "order_size_min": "1",
        "order_size_max": "1000",
        "expire_time": 1758844800,
        "in_delisting": False,
    },
    {
        "name": "OLD_USDT",
        "quanto_multiplier": "1",
        "in_delisting": True,
    },
    {"name": "BAD"},
]


def _gate_future(rows: list[dict]) -> GateFuturesInstrumentSource:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v4/futures/usdt/contracts"
        return httpx.Response(200, json=rows)

    return GateFuturesInstrumentSource(
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://api.gateio.ws",
        )
    )


async def test_gate_futures_rows_become_perp_instruments() -> None:
    instruments = await _gate_future(GATE_FUTURE_ROWS).fetch()

    assert [i.exch_ticker for i in instruments] == ["BTC_USDT"]
    btc = instruments[0]
    assert str(btc.ticker) == "GateFutures_Perp_BTCUSDT"
    assert btc.category is Category.PERP
    assert btc.contract_size == Decimal("0.0001")
    assert btc.settlement_asset == "USDT"
    assert btc.filters["price_tick"] == Decimal("0.1")
    assert btc.filters["qty_step"] == Decimal("0.0001")
    assert btc.filters["min_qty"] == Decimal("0.0001")
    assert btc.filters["max_qty"] == Decimal("100")


async def test_gate_futures_drops_dated_and_delisting_contracts() -> None:
    instruments = await _gate_future(GATE_FUTURE_ROWS).fetch()
    assert {i.symbol for i in instruments} == {"BTCUSDT"}


# --- OKX -------------------------------------------------------------------

# Trimmed rows in OKX's /api/v5/public/instruments shape.
OKX_SPOT_ROWS = [
    {
        "instId": "BTC-USDT",
        "baseCcy": "BTC",
        "quoteCcy": "USDT",
        "state": "live",
        "tickSz": "0.1",
        "lotSz": "0.00000001",
        "minSz": "0.00001",
        "maxLmtSz": "10000000000",
    },
    {
        "instId": "SOON-USDT",
        "baseCcy": "SOON",
        "quoteCcy": "USDT",
        "state": "preopen",
        "tickSz": "0.0001",
        "lotSz": "0.01",
        "minSz": "1",
    },
    # Malformed — no base; must be skipped, not crash the refresh.
    {"instId": "BAD-USDT", "quoteCcy": "USDT", "state": "live"},
]

OKX_SWAP_ROWS = [
    {
        "instId": "BTC-USDT-SWAP",
        "ctType": "linear",
        "state": "live",
        "baseCcy": "",
        "quoteCcy": "",
        "ctValCcy": "BTC",
        "settleCcy": "USDT",
        "ctVal": "0.01",
        "ctMult": "1",
        "tickSz": "0.1",
        "lotSz": "1",
        "minSz": "1",
        "maxLmtSz": "10000",
    },
    {
        # USDC-quoted linear: native ticker is not base+quote.
        "instId": "BTC-USDC-SWAP",
        "ctType": "linear",
        "state": "live",
        "ctValCcy": "BTC",
        "settleCcy": "USDC",
        "ctVal": "0.01",
        "ctMult": "1",
        "tickSz": "0.1",
        "lotSz": "1",
        "minSz": "1",
    },
    {
        # Inverse — same underlier, different settlement. Must not land as
        # a Perp or it would collide with BTCUSDT after canonicalize.
        "instId": "BTC-USD-SWAP",
        "ctType": "inverse",
        "state": "live",
        "ctValCcy": "USD",
        "settleCcy": "BTC",
        "ctVal": "100",
        "lotSz": "1",
        "minSz": "1",
    },
    {
        "instId": "ETH-USDT-SWAP",
        "ctType": "linear",
        "state": "suspend",
        "ctValCcy": "ETH",
        "settleCcy": "USDT",
        "ctVal": "0.1",
        "lotSz": "1",
        "minSz": "1",
    },
]


def _okx(
    rows: list[dict],
    *,
    category: Category = Category.SPOT,
) -> OkxInstrumentSource:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v5/public/instruments"
        assert request.url.params["instType"] == (
            "SPOT" if category is Category.SPOT else "SWAP"
        )
        return httpx.Response(200, json={"code": "0", "msg": "", "data": rows})

    return OkxInstrumentSource(
        category=category,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://www.okx.com",
        ),
    )


async def test_okx_spot_rows_become_canonical_instruments() -> None:
    instruments = await _okx(OKX_SPOT_ROWS).fetch()

    assert [i.symbol for i in instruments] == ["BTCUSDT", "SOONUSDT"]
    btc = instruments[0]
    assert btc.base == "BTC" and btc.quote == "USDT"
    assert btc.exch_ticker == "BTC-USDT"
    assert str(btc.ticker) == "Okx_Spot_BTCUSDT"
    assert btc.is_active
    assert btc.contract_size is None
    # Spot settles in its quote currency; repeating that says nothing.
    assert btc.settlement_asset is None


async def test_okx_spot_filters_are_already_base() -> None:
    btc = (await _okx(OKX_SPOT_ROWS).fetch())[0]

    assert btc.filters["price_tick"] == Decimal("0.1")
    assert btc.filters["qty_step"] == Decimal("0.00000001")
    assert btc.filters["min_qty"] == Decimal("0.00001")
    assert btc.filters["max_qty"] == Decimal("10000000000")
    assert "min_notional" in btc.filters
    assert btc.filters["min_notional"] is None


async def test_okx_preopen_symbols_are_marked_inactive() -> None:
    soon = (await _okx(OKX_SPOT_ROWS).fetch())[1]
    assert soon.symbol == "SOONUSDT"
    assert not soon.is_active


async def test_malformed_okx_rows_are_skipped() -> None:
    instruments = await _okx(OKX_SPOT_ROWS).fetch()
    assert "BAD" not in {i.base for i in instruments}
    assert len(instruments) == 2


async def test_okx_perp_filters_are_converted_to_base() -> None:
    """SWAP sizes in contracts; the plane stores base so STS never sees a
    quanto. ``ctVal=0.01`` and ``lotSz=1`` is a 0.01 BTC step."""
    instruments = await _okx(OKX_SWAP_ROWS, category=Category.PERP).fetch()
    btc = instruments[0]

    assert str(btc.ticker) == "Okx_Perp_BTCUSDT"
    assert btc.exch_ticker == "BTC-USDT-SWAP"
    assert btc.contract_size == Decimal("0.01")
    assert btc.settlement_asset == "USDT"
    assert btc.filters["qty_step"] == Decimal("0.01")
    assert btc.filters["min_qty"] == Decimal("0.01")
    assert btc.filters["max_qty"] == Decimal("100")
    assert btc.filters["price_tick"] == Decimal("0.1")


async def test_an_inverse_swap_is_not_published_as_a_perp() -> None:
    """SWAP lists inverse beside linear. One stored as a Perp would
    canonicalize to a colliding symbol and send every order there."""
    instruments = await _okx(OKX_SWAP_ROWS, category=Category.PERP).fetch()

    assert [i.exch_ticker for i in instruments] == [
        "BTC-USDT-SWAP",
        "BTC-USDC-SWAP",
        "ETH-USDT-SWAP",
    ]
    assert len({str(i.ticker) for i in instruments}) == len(instruments)
    assert instruments[1].symbol == "BTCUSDC"
    assert str(instruments[1].ticker) == "Okx_Perp_BTCUSDC"
    assert not instruments[2].is_active


async def test_okx_http_failure_propagates() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"code": "50001", "msg": "busy"})

    source = OkxInstrumentSource(
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://www.okx.com",
        )
    )
    with pytest.raises(httpx.HTTPStatusError):
        await source.fetch()


async def test_okx_envelope_refusal_does_not_empty_delist() -> None:
    """A 200 with ``code != 0`` must not look like an empty listing."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"code": "50011", "msg": "Rate limit", "data": []}
        )

    source = OkxInstrumentSource(
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://www.okx.com",
        )
    )
    with pytest.raises(RuntimeError, match="50011"):
        await source.fetch()


BITGET_USDT = {
    "symbol": "BTCUSDT",
    "category": "USDT-FUTURES",
    "baseCoin": "BTC",
    "quoteCoin": "USDT",
    "type": "perpetual",
    "status": "online",
    "minOrderQty": "0.001",
    "maxOrderQty": "100",
    "minOrderAmount": "5",
    "priceMultiplier": "0.1",
    "quantityMultiplier": "0.001",
}
BITGET_USDC = {
    "symbol": "BTCPERP",
    "category": "USDC-FUTURES",
    "baseCoin": "BTC",
    "quoteCoin": "USDC",
    "type": "perpetual",
    "status": "online",
    "minOrderQty": "0.001",
    "maxOrderQty": "100",
    "minOrderAmount": "5",
    "priceMultiplier": "0.1",
    "quantityMultiplier": "0.001",
}
BITGET_DELIVERY = {**BITGET_USDT, "symbol": "BTCUSDT-260327", "type": "delivery"}
BITGET_SPOT = {
    "symbol": "BTCUSDT",
    "category": "SPOT",
    "baseCoin": "BTC",
    "quoteCoin": "USDT",
    "status": "online",
    "minOrderQty": "0.0001",
    "maxOrderQty": "100",
    "minOrderAmount": "5",
    "priceMultiplier": "0.1",
    "quantityMultiplier": "0.0001",
}


def _bitget(
    by_product: dict[str, list[dict]],
    *,
    category: Category = Category.SPOT,
) -> BitgetInstrumentSource:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v3/market/instruments"
        product = request.url.params["category"]
        return httpx.Response(
            200,
            json={
                "code": "00000",
                "msg": "success",
                "data": by_product.get(product, []),
            },
        )

    return BitgetInstrumentSource(
        category=category,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://api.bitget.com",
        ),
    )


async def test_bitget_spot_fetch_does_not_emit_a_perp_ticker() -> None:
    instruments = await _bitget({"SPOT": [BITGET_SPOT, {"symbol": "BAD"}]}).fetch()
    assert [str(i.ticker) for i in instruments] == ["Bitget_Spot_BTCUSDT"]
    assert instruments[0].exch_ticker == "BTCUSDT"
    assert instruments[0].settlement_asset is None


async def test_i4_bitget_perp_fetch_unions_two_products_and_drops_delivery() -> None:
    """I4 — one Perp listing, two fetches. Delivery is absent; malformed skipped."""
    source = _bitget(
        {
            "USDT-FUTURES": [BITGET_USDT, BITGET_DELIVERY, {"symbol": "BAD"}],
            "USDC-FUTURES": [BITGET_USDC],
        },
        category=Category.PERP,
    )
    instruments = await source.fetch()
    tickers = {str(i.ticker) for i in instruments}
    assert tickers == {"Bitget_Perp_BTCUSDT", "Bitget_Perp_BTCUSDC"}
    by_ticker = {str(i.ticker): i for i in instruments}
    assert by_ticker["Bitget_Perp_BTCUSDT"].quote == "USDT"
    assert by_ticker["Bitget_Perp_BTCUSDT"].settlement_asset == "USDT"
    assert by_ticker["Bitget_Perp_BTCUSDT"].exch_ticker == "BTCUSDT"
    assert by_ticker["Bitget_Perp_BTCUSDC"].quote == "USDC"
    assert by_ticker["Bitget_Perp_BTCUSDC"].settlement_asset == "USDC"
    assert by_ticker["Bitget_Perp_BTCUSDC"].exch_ticker == "BTCPERP"


def test_i4_default_sources_has_exactly_one_bitget_perp_source() -> None:
    class _Broker:
        pass

    sources = default_sources(_Broker())  # type: ignore[arg-type]
    bitget = [s for s in sources if getattr(s, "venue", None) == "Bitget"]
    assert len(bitget) == 2
    perps = [s for s in bitget if s.category is Category.PERP]
    spots = [s for s in bitget if s.category is Category.SPOT]
    assert len(perps) == 1
    assert len(spots) == 1
    assert perps[0].products == ("USDT-FUTURES", "USDC-FUTURES")


DERIBIT_SPOT = {
    "instrument_name": "BTC_USDC",
    "kind": "spot",
    "base_currency": "BTC",
    "quote_currency": "USDC",
    "tick_size": "0.01",
    "min_trade_amount": "0.0001",
    "is_active": True,
}

DERIBIT_CBE = {
    "instrument_name": "SOL_USDC",
    "kind": "spot",
    "base_currency": "SOL",
    "quote_currency": "USDC",
    "tick_size": "0.01",
    "min_trade_amount": "0.01",
    "is_active": True,
    "is_cbe_routed": True,
}

DERIBIT_LINEAR = {
    "instrument_name": "BTC_USDC-PERPETUAL",
    "kind": "future",
    "instrument_type": "linear",
    "future_type": "linear",
    "settlement_period": "perpetual",
    "base_currency": "BTC",
    "quote_currency": "USDC",
    "settlement_currency": "USDC",
    "tick_size": "0.1",
    "min_trade_amount": "0.0001",
    "is_active": True,
}

DERIBIT_INVERSE = {
    "instrument_name": "BTC-PERPETUAL",
    "kind": "future",
    "instrument_type": "reversed",
    "settlement_period": "perpetual",
    "base_currency": "BTC",
    "quote_currency": "USD",
    "settlement_currency": "BTC",
    "is_active": True,
}

DERIBIT_DATED_INVERSE = {
    "instrument_name": "BTC-6SEP26",
    "kind": "future",
    "instrument_type": "reversed",
    "future_type": "reversed",
    "settlement_period": "day",
    "base_currency": "BTC",
    "quote_currency": "USD",
    "settlement_currency": "BTC",
    "expiration_timestamp": 1_788_681_600_000,
    "is_active": True,
}

DERIBIT_DATED_LINEAR = {
    "instrument_name": "BTC_USDC-6SEP26",
    "kind": "future",
    "instrument_type": "linear",
    "future_type": "linear",
    "settlement_period": "day",
    "base_currency": "BTC",
    "quote_currency": "USDC",
    "settlement_currency": "USDC",
    "expiration_timestamp": 1_788_681_600_000,
    "is_active": True,
}


def _deribit(
    rows: list[dict],
    *,
    category: Category = Category.SPOT,
) -> DeribitInstrumentSource:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/public/get_instruments")
        assert request.url.params["currency"] == "any"
        return httpx.Response(200, json={"jsonrpc": "2.0", "result": rows})

    return DeribitInstrumentSource(
        category=category,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://www.deribit.com/api/v2",
        ),
    )


async def test_deribit_spot_fetch_keeps_cbe_and_native() -> None:
    instruments = await _deribit([DERIBIT_SPOT, DERIBIT_CBE, {"kind": "bad"}]).fetch()
    tickers = {str(i.ticker) for i in instruments}
    assert tickers == {"Deribit_Spot_BTCUSDC", "Deribit_Spot_SOLUSDC"}
    by_ticker = {str(i.ticker): i for i in instruments}
    assert by_ticker["Deribit_Spot_BTCUSDC"].exch_ticker == "BTC_USDC"
    assert by_ticker["Deribit_Spot_SOLUSDC"].exch_ticker == "SOL_USDC"


_DERIBIT_FUTURE_ROWS = [
    DERIBIT_LINEAR,
    DERIBIT_INVERSE,
    DERIBIT_DATED_INVERSE,
    DERIBIT_DATED_LINEAR,
    {"symbol": "BAD"},
]


async def test_deribit_perp_fetch_drops_inverse_and_dated() -> None:
    source = _deribit(_DERIBIT_FUTURE_ROWS, category=Category.PERP)
    instruments = await source.fetch()
    assert [str(i.ticker) for i in instruments] == ["Deribit_Perp_BTCUSDC"]
    assert instruments[0].exch_ticker == "BTC_USDC-PERPETUAL"
    assert instruments[0].settlement_asset == "USDC"


async def test_deribit_inverse_fetch_keeps_the_coin_margined_perp() -> None:
    source = _deribit(_DERIBIT_FUTURE_ROWS, category=Category.INVERSE)
    instruments = await source.fetch()
    assert [str(i.ticker) for i in instruments] == ["Deribit_Inverse_BTCUSD"]
    assert instruments[0].exch_ticker == "BTC-PERPETUAL"
    assert instruments[0].settlement_asset == "BTC"


async def test_deribit_future_fetch_keeps_linear_and_inverse_dated() -> None:
    source = _deribit(_DERIBIT_FUTURE_ROWS, category=Category.FUTURE)
    instruments = await source.fetch()
    tickers = {str(i.ticker): i for i in instruments}
    assert set(tickers) == {
        "Deribit_Future_BTCUSD-260906",
        "Deribit_Future_BTCUSDC-260906",
    }
    assert tickers["Deribit_Future_BTCUSD-260906"].exch_ticker == "BTC-6SEP26"
    assert tickers["Deribit_Future_BTCUSDC-260906"].exch_ticker == (
        "BTC_USDC-6SEP26"
    )


def test_default_sources_has_one_deribit_source_per_book() -> None:
    class _Broker:
        pass

    sources = default_sources(_Broker())  # type: ignore[arg-type]
    deribit = [s for s in sources if getattr(s, "venue", None) == "Deribit"]
    by_book = {s.category: s for s in deribit}
    assert set(by_book) == {
        Category.SPOT,
        Category.PERP,
        Category.INVERSE,
        Category.FUTURE,
    }
    assert by_book[Category.SPOT].kind == "spot"
    assert by_book[Category.PERP].kind == "future"
    assert by_book[Category.INVERSE].kind == "future"
    assert by_book[Category.FUTURE].kind == "future"

