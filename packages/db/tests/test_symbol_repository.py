"""Symbol plane persistence — upsert, filter reconciliation, delisting."""

from __future__ import annotations

from decimal import Decimal

import pytest
from db_harness import a_database
from mftik_db.models import SymbolCategory
from mftik_db.repositories import SymbolRepository

SPOT = SymbolCategory.SPOT.value


@pytest.fixture
async def db(database_url):
    async with a_database(database_url) as database, database.maker() as session:
        yield session


async def _btc(repo: SymbolRepository, **overrides):
    payload = {
        "universal_ticker": "Gate_Spot_BTCUSDT",
        "base": "BTC",
        "quote": "USDT",
        "exch_ticker": "BTC_USDT",
        "filters": {
            "price_tick": Decimal("0.0001"),
            "qty_step": Decimal("1"),
            "min_qty": Decimal("1"),
            "max_qty": Decimal("100000"),
            "min_notional": None,
        },
    }
    payload.update(overrides)
    return await repo.upsert(**payload)


async def test_upsert_creates_ticker_and_filter_rows(db) -> None:
    repo = SymbolRepository(db)
    ticker = await _btc(repo)

    assert ticker.universal_ticker == "Gate_Spot_BTCUSDT"
    assert ticker.exch_ticker == "BTC_USDT"
    assert ticker.base == "BTC"
    assert ticker.is_active

    filters = {f.name: f.value for f in await repo.list_filters(ticker.id)}
    assert filters["price_tick"] == Decimal("0.0001")
    assert filters["max_qty"] == Decimal("100000")
    # A published filter with no bound is a row with a NULL value — which is
    # not the same as the venue never publishing it.
    assert "min_notional" in filters
    assert filters["min_notional"] is None


async def test_upsert_is_idempotent(db) -> None:
    repo = SymbolRepository(db)
    first = await _btc(repo)
    second = await _btc(repo)

    assert first.id == second.id
    assert len(await repo.list_filters(second.id)) == 5
    assert len(await repo.list_tickers()) == 1


async def test_upsert_reconciles_changed_and_dropped_filters(db) -> None:
    repo = SymbolRepository(db)
    ticker = await _btc(repo)
    rows = await repo.list_filters(ticker.id)
    tick_row_id = {f.name: f.id for f in rows}["price_tick"]

    ticker = await _btc(
        repo,
        filters={
            "price_tick": Decimal("0.01"),  # changed
            "qty_step": Decimal("1"),  # unchanged
            # min_qty / max_qty / min_notional dropped by the venue
        },
    )
    filters = {f.name: f for f in await repo.list_filters(ticker.id)}
    assert set(filters) == {"price_tick", "qty_step"}
    assert filters["price_tick"].value == Decimal("0.01")
    # Updated in place, so anything referencing the row still resolves.
    assert filters["price_tick"].id == tick_row_id


async def test_same_symbol_in_two_categories_is_two_instruments(db) -> None:
    """A unified-account venue's whole shape: one symbol, two instruments."""
    repo = SymbolRepository(db)
    await _btc(repo, universal_ticker="Bybit_Spot_BTCUSDT")
    await _btc(
        repo,
        universal_ticker="Bybit_Perp_BTCUSDT",
        exch_ticker="BTCUSDT",
        contract_size=Decimal("0.0001"),
        settlement_asset="USDT",
        filters={"price_tick": Decimal("0.1")},
    )

    tickers = await repo.list_tickers()
    assert len(tickers) == 2
    perp = await repo.get_ticker("Bybit_Perp_BTCUSDT")
    assert perp is not None
    assert perp.contract_size == Decimal("0.0001")
    assert perp.settlement_asset == "USDT"
    spot = await repo.get_ticker("Bybit_Spot_BTCUSDT")
    assert spot is not None
    assert spot.contract_size is None


async def test_delisting_deactivates_rather_than_deletes(db) -> None:
    """Orders and sessions still reference instruments that got delisted."""
    repo = SymbolRepository(db)
    await _btc(repo)
    await _btc(
        repo,
        universal_ticker="Gate_Spot_ETHUSDT",
        base="ETH",
        exch_ticker="ETH_USDT",
    )

    dropped = await repo.deactivate_missing(
        venue="Gate", category=SPOT, keep={"Gate_Spot_BTCUSDT"}
    )

    assert dropped == 1
    active = await repo.list_tickers()
    assert [t.universal_ticker for t in active] == ["Gate_Spot_BTCUSDT"]
    everything = await repo.list_tickers(active_only=False)
    assert len(everything) == 2
    eth = await repo.get_ticker("Gate_Spot_ETHUSDT")
    assert eth is not None and not eth.is_active


async def test_relisting_reactivates(db) -> None:
    repo = SymbolRepository(db)
    await _btc(repo)
    await repo.deactivate_missing(venue="Gate", category=SPOT, keep=set())
    assert not (await repo.list_tickers())

    await _btc(repo)
    assert [t.universal_ticker for t in await repo.list_tickers()] == [
        "Gate_Spot_BTCUSDT"
    ]


async def test_list_filters_by_the_parts_of_a_ticker(db) -> None:
    """Every filter is a pattern over the one identity column."""
    repo = SymbolRepository(db)
    await _btc(repo)
    await _btc(repo, universal_ticker="Gate_Spot_ETHUSDT", exch_ticker="ETH_USDT")
    await _btc(repo, universal_ticker="Paper_Spot_BTCUSDT", exch_ticker="BTCUSDT")
    await _btc(repo, universal_ticker="Bybit_Perp_BTCUSDT", exch_ticker="BTCUSDT")

    assert len(await repo.list_tickers(venue="Gate")) == 2
    assert len(await repo.list_tickers(category=SPOT)) == 3
    assert len(await repo.list_tickers(symbol="BTCUSDT")) == 3
    assert len(await repo.list_tickers(venue="Bybit", category="Perp")) == 1
    assert [
        t.universal_ticker
        for t in await repo.list_tickers(venue="Gate", symbol="ETHUSDT")
    ] == ["Gate_Spot_ETHUSDT"]
    assert await repo.venues() == ["Bybit", "Gate", "Paper"]


async def test_a_filter_part_cannot_wildcard_across_the_separator(db) -> None:
    """``_`` separates the parts and is also LIKE's single-char wildcard.

    Without escaping, filtering on venue ``Gate`` would match a venue spelled
    ``GateX`` — every pattern the repository builds goes through
    ``autoescape`` precisely so it cannot.
    """
    repo = SymbolRepository(db)
    await _btc(repo, universal_ticker="Gate_Spot_BTCUSDT")
    await _btc(repo, universal_ticker="GateFutures_Perp_BTCUSDT")

    found = await repo.list_tickers(venue="Gate")
    assert [t.universal_ticker for t in found] == ["Gate_Spot_BTCUSDT"]


async def test_deactivating_one_market_leaves_the_others_alone(db) -> None:
    """A Bybit spot refresh says nothing about Bybit perps."""
    repo = SymbolRepository(db)
    await _btc(repo, universal_ticker="Bybit_Spot_BTCUSDT")
    await _btc(repo, universal_ticker="Bybit_Perp_BTCUSDT")

    dropped = await repo.deactivate_missing(
        venue="Bybit", category=SPOT, keep=set()
    )

    assert dropped == 1
    assert [t.universal_ticker for t in await repo.list_tickers()] == [
        "Bybit_Perp_BTCUSDT"
    ]


async def test_list_tickers_pages_and_searches(db) -> None:
    repo = SymbolRepository(db)
    await _btc(repo)
    await _btc(
        repo,
        universal_ticker="Gate_Spot_ETHUSDT",
        base="ETH",
        exch_ticker="ETH_USDT",
    )
    await _btc(
        repo,
        universal_ticker="Gate_Spot_SOLUSDT",
        base="SOL",
        exch_ticker="SOL_USDT",
    )

    page = await repo.list_tickers(venue="Gate", limit=2, offset=0)
    assert [t.universal_ticker for t in page] == [
        "Gate_Spot_BTCUSDT",
        "Gate_Spot_ETHUSDT",
    ]
    assert await repo.count_tickers(venue="Gate") == 3

    found = await repo.list_tickers(venue="Gate", q="sol")
    assert [t.universal_ticker for t in found] == ["Gate_Spot_SOLUSDT"]
    assert await repo.count_tickers(venue="Gate", q="sol") == 1


async def test_list_tickers_exact_universal_ticker(db) -> None:
    """A detail lookup must not scan every venue's instruments."""
    repo = SymbolRepository(db)
    await _btc(repo)
    await _btc(
        repo,
        universal_ticker="Paper_Spot_BTCUSDT",
        exch_ticker="BTCUSDT",
    )

    found = await repo.list_tickers(
        universal_ticker="Gate_Spot_BTCUSDT", active_only=False
    )
    assert [t.universal_ticker for t in found] == ["Gate_Spot_BTCUSDT"]


async def test_list_filters_for_can_restrict_names(db) -> None:
    repo = SymbolRepository(db)
    ticker = await _btc(repo)

    slim = await repo.list_filters_for(
        [ticker.id], names=("price_tick", "qty_step")
    )
    assert {f.name for f in slim[ticker.id]} == {"price_tick", "qty_step"}
