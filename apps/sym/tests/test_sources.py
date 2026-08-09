"""Venue instrument sources — conversion to canonical instruments."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest
from mft.exchange.tickers import Category
from mft_sym.sources import PaperInstrumentSource, tick_from_precision
from mft_sym.sources.binance import BinanceSpotInstrumentSource
from mft_sym.sources.bybit import BybitInstrumentSource
from mft_sym.sources.gate import GateSpotInstrumentSource

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
    from mft.exchange import PaperExchange

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
