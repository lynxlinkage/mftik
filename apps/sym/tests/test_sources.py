"""Venue instrument sources — conversion to canonical instruments."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest
from mft.exchange.tickers import Category
from mft_sym.sources import PaperInstrumentSource, tick_from_precision
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
