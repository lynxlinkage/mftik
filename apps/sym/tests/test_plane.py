"""The plane end to end: refresh into real tables, then serve RPC over them."""

from __future__ import annotations

from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange.tickers import Category, UniversalTicker
from mft.protocol import (
    SYM_LIST,
    SYM_REFRESH,
    SYM_VENUES,
    Envelope,
    SymListRequest,
    SymRefreshRequest,
    Topics,
)
from mft.symbols import SymbolClient, SymbolNotFoundError
from mft_db.models import Base
from mft_db.repositories import SymbolRepository
from mft_sym.plane import SymbolPlane
from mft_sym.rpc import dispatch
from mft_sym.sources.base import Instrument
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

VENUE = "Gate"


def _t(symbol: str, venue: str = VENUE, category: str = "Spot") -> UniversalTicker:
    """A universal ticker, for the reads that are keyed by one."""
    return UniversalTicker.parse(f"{venue}_{category}_{symbol}")


class StubSource:
    """An instrument source whose listing the test controls."""

    def __init__(
        self,
        instruments: list[Instrument],
        venue: str = VENUE,
        category: Category = Category.SPOT,
    ) -> None:
        self.venue = venue
        # One source is one (venue, category) — the unit a venue's listing
        # endpoint serves and a refresh can safely delist within.
        self.category = category
        self.instruments = instruments
        self.fail: Exception | None = None
        self.fetches = 0
        self.closed = False

    async def fetch(self) -> list[Instrument]:
        self.fetches += 1
        if self.fail is not None:
            raise self.fail
        return self.instruments

    async def close(self) -> None:
        self.closed = True


def _inst(base: str, quote: str = "USDT", **kwargs) -> Instrument:
    payload = {
        "venue": VENUE,
        "base": base,
        "quote": quote,
        "exch_ticker": f"{base}_{quote}",
        "filters": {
            "price_tick": Decimal("0.01"),
            "min_notional": None,
        },
    }
    payload.update(kwargs)
    return Instrument(**payload)


@pytest.fixture
async def sessionmaker_():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture
def plane_factory(sessionmaker_):
    """Builds a plane bound to the in-memory DB."""

    def build(sources, **kwargs) -> SymbolPlane:
        async def upsert(**payload):
            async with sessionmaker_() as db:
                row = await SymbolRepository(db).upsert(**payload)
                await db.commit()
                return row

        async def deactivate(**payload):
            async with sessionmaker_() as db:
                n = await SymbolRepository(db).deactivate_missing(**payload)
                await db.commit()
                return n

        async def list_tickers(**payload):
            async with sessionmaker_() as db:
                return list(await SymbolRepository(db).list_tickers(**payload))

        async def list_filters_for(ticker_ids):
            async with sessionmaker_() as db:
                return await SymbolRepository(db).list_filters_for(ticker_ids)

        return SymbolPlane(
            sources,
            upsert=upsert,
            deactivate_missing=deactivate,
            list_tickers=list_tickers,
            list_filters_for=list_filters_for,
            **kwargs,
        )

    return build


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


# --- refresh ---------------------------------------------------------------


async def test_refresh_writes_the_golden_record(plane_factory) -> None:
    source = StubSource([_inst("BTC"), _inst("ETH")])
    plane = plane_factory([source])

    result = await plane.refresh()

    assert result["refreshed"] == {VENUE: 2}
    assert result["failed"] == {}
    symbols = await plane.list_symbols(venue=VENUE)
    assert [s.symbol for s in symbols] == ["BTCUSDT", "ETHUSDT"]
    btc = symbols[0]
    assert btc.exch_ticker == "BTC_USDT"
    assert btc.filter("price_tick") == Decimal("0.01")
    assert btc.has_filter("min_notional")
    assert btc.filter("min_notional") is None


async def test_list_symbols_loads_filters_in_one_query(plane_factory) -> None:
    """Regression: this used to be one query per instrument.

    Gate lists 2200+ pairs, so a per-instrument fan-out to a database on
    another host took ~14s and every td/md caller hit the 5s RPC timeout. The
    count is what matters here, not the timing.
    """
    plane = plane_factory([StubSource([_inst("BTC"), _inst("ETH"), _inst("SOL")])])
    await plane.refresh()

    inner = plane._list_filters_for
    calls: list[list[int]] = []

    async def counting(ticker_ids):
        calls.append(list(ticker_ids))
        return await inner(ticker_ids)

    plane._list_filters_for = counting
    symbols = await plane.list_symbols(venue=VENUE)

    assert len(symbols) == 3
    assert len(calls) == 1
    assert len(calls[0]) == 3
    assert symbols[0].filter("price_tick") == Decimal("0.01")


async def test_refresh_deactivates_delisted_instruments(plane_factory) -> None:
    source = StubSource([_inst("BTC"), _inst("ETH")])
    plane = plane_factory([source])
    await plane.refresh()

    source.instruments = [_inst("BTC")]
    result = await plane.refresh()

    assert result["deactivated"] == {VENUE: 1}
    assert [s.symbol for s in await plane.list_symbols()] == ["BTCUSDT"]
    # The row survives — orders and sessions still reference it.
    everything = await plane.list_symbols(active_only=False)
    assert {s.symbol for s in everything} == {"BTCUSDT", "ETHUSDT"}


async def test_inactive_instruments_are_not_kept_alive(plane_factory) -> None:
    """A pair the venue lists but marks untradable must not read as active."""
    source = StubSource([_inst("BTC"), _inst("GT", is_active=False)])
    plane = plane_factory([source])

    await plane.refresh()

    assert [s.symbol for s in await plane.list_symbols()] == ["BTCUSDT"]


async def test_one_venue_failing_does_not_block_the_others(
    plane_factory,
) -> None:
    good = StubSource([_inst("BTC")])
    bad = StubSource([], venue="Paper")
    bad.fail = RuntimeError("endpoint down")
    plane = plane_factory([good, bad])

    result = await plane.refresh()

    assert result["refreshed"] == {VENUE: 1}
    assert "Paper" in result["failed"]
    assert "endpoint down" in result["failed"]["Paper"]
    assert [s.symbol for s in await plane.list_symbols()] == ["BTCUSDT"]


async def test_a_unified_venue_refreshes_each_book_independently(
    plane_factory,
) -> None:
    """Bybit is one venue with two listings, and they must not delist each
    other: a spot response that omits a perp says nothing about the perp."""
    spot = StubSource(
        [_inst("BTC", venue="Bybit", exch_ticker="BTCUSDT")],
        venue="Bybit",
    )
    perp = StubSource(
        [
            _inst(
                "BTC",
                venue="Bybit",
                category=Category.PERP,
                exch_ticker="BTCUSDT",
            ),
            _inst(
                "ETH",
                venue="Bybit",
                category=Category.PERP,
                exch_ticker="ETHUSDT",
            ),
        ],
        venue="Bybit",
        category=Category.PERP,
    )
    plane = plane_factory([spot, perp])

    result = await plane.refresh()

    # The tallies are per venue, so the two books add up rather than the
    # second overwriting the first.
    assert result["refreshed"] == {"Bybit": 3}
    assert plane.venues == ["Bybit"]
    tickers = {
        s.universal_ticker for s in await plane.list_symbols(venue="Bybit")
    }
    assert tickers == {
        "Bybit_Spot_BTCUSDT",
        "Bybit_Perp_BTCUSDT",
        "Bybit_Perp_ETHUSDT",
    }

    # Delisting on one book leaves the other alone.
    perp.instruments = perp.instruments[:1]
    result = await plane.refresh()

    assert result["deactivated"] == {"Bybit": 1}
    tickers = {
        s.universal_ticker for s in await plane.list_symbols(venue="Bybit")
    }
    assert tickers == {"Bybit_Spot_BTCUSDT", "Bybit_Perp_BTCUSDT"}


async def test_a_failure_names_the_book_it_happened_on(plane_factory) -> None:
    """One venue name, two sources — the message is the only place that can
    say which of them was down."""
    spot = StubSource([_inst("BTC", venue="Bybit")], venue="Bybit")
    perp = StubSource([], venue="Bybit", category=Category.PERP)
    perp.fail = RuntimeError("endpoint down")
    plane = plane_factory([spot, perp])

    result = await plane.refresh()

    assert result["refreshed"] == {"Bybit": 1}
    assert "Perp: RuntimeError: endpoint down" in result["failed"]["Bybit"]


async def test_refresh_can_target_one_venue(plane_factory) -> None:
    gate = StubSource([_inst("BTC")])
    paper = StubSource([], venue="Paper")
    plane = plane_factory([gate, paper])

    await plane.refresh(venue=VENUE)

    assert gate.fetches == 1
    assert paper.fetches == 0


async def test_close_releases_sources(plane_factory) -> None:
    source = StubSource([])
    plane = plane_factory([source])
    await plane.close()
    assert source.closed


# --- RPC + client ----------------------------------------------------------


async def _serve(broker: Broker, plane: SymbolPlane, stop) -> None:
    async for req in broker.serve(Topics.SYM, stop=stop):
        await dispatch(req, plane=plane)


@pytest.fixture
async def served(broker: Broker, plane_factory):
    import asyncio

    source = StubSource([_inst("BTC"), _inst("ETH")])
    plane = plane_factory([source])
    await plane.refresh()
    stop = asyncio.Event()
    task = asyncio.create_task(_serve(broker, plane, stop))
    yield plane, source
    stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_rpc_list(broker: Broker, served) -> None:
    reply = await broker.request(
        Topics.SYM,
        Envelope[SymListRequest].wrap(
            SymListRequest(venue=VENUE), type=SYM_LIST, source="test"
        ),
    )
    symbols = reply.payload["symbols"]
    assert [s["universal_ticker"] for s in symbols] == [
        "Gate_Spot_BTCUSDT",
        "Gate_Spot_ETHUSDT",
    ]
    assert symbols[0]["exch_ticker"] == "BTC_USDT"


async def test_rpc_venues_reports_counts(broker: Broker, served) -> None:
    reply = await broker.request(
        Topics.SYM,
        Envelope[SymListRequest].wrap(
            SymListRequest(), type=SYM_VENUES, source="test"
        ),
    )
    assert reply.payload["venues"] == [VENUE]
    assert reply.payload["counts"][VENUE] == 2


async def test_rpc_refresh(broker: Broker, served) -> None:
    _plane, source = served
    reply = await broker.request(
        Topics.SYM,
        Envelope[SymRefreshRequest].wrap(
            SymRefreshRequest(venue=VENUE), type=SYM_REFRESH, source="test"
        ),
    )
    assert reply.payload["refreshed"] == {VENUE: 2}
    assert source.fetches == 2  # once in the fixture, once here


async def test_rpc_unknown_type_errors(broker: Broker, served) -> None:
    reply = await broker.request(
        Topics.SYM,
        Envelope[SymListRequest].wrap(
            SymListRequest(), type="sym.nope", source="test"
        ),
    )
    assert reply.type == "sym.error"
    assert reply.payload["code"] == "unknown_type"


async def test_client_resolves_the_venue_ticker(broker: Broker, served) -> None:
    """The translation TD needs, straight from the golden record."""
    client = SymbolClient(broker)

    assert await client.exch_ticker(_t("BTCUSDT")) == "BTC_USDT"
    assert await client.filter(_t("BTCUSDT"), "price_tick") == Decimal("0.01")
    assert [i.symbol for i in await client.list(VENUE)] == ["BTCUSDT", "ETHUSDT"]


async def test_client_caches_reads(broker: Broker, served) -> None:
    """Listings are near-static; a process must not re-query per order."""
    plane, _source = served
    calls = 0
    original = plane.list_symbols

    async def counting(**kwargs):
        nonlocal calls
        calls += 1
        return await original(**kwargs)

    plane.list_symbols = counting  # type: ignore[method-assign]
    client = SymbolClient(broker)

    for _ in range(5):
        await client.exch_ticker(_t("BTCUSDT"))

    assert calls == 1


async def test_client_refetches_once_for_an_unknown_symbol(
    broker: Broker, served
) -> None:
    """A newly listed pair should resolve without waiting for the TTL."""
    plane, source = served
    client = SymbolClient(broker)
    await client.exch_ticker(_t("BTCUSDT"))  # warm the cache

    source.instruments = [_inst("BTC"), _inst("ETH"), _inst("SOL")]
    await plane.refresh()

    assert await client.exch_ticker(_t("SOLUSDT")) == "SOL_USDT"


async def test_client_raises_for_a_symbol_that_does_not_exist(
    broker: Broker, served
) -> None:
    client = SymbolClient(broker)
    with pytest.raises(SymbolNotFoundError, match="Gate_Spot_NOPEUSDT"):
        await client.get(_t("NOPEUSDT"))


async def test_client_reverse_lookup(broker: Broker, served) -> None:
    """Venue spelling → canonical, the direction TD needs on inbound events."""
    client = SymbolClient(broker)
    assert await client.symbol_for(VENUE, "BTC_USDT", category="Spot") == _t("BTCUSDT")
    assert await client.symbol_for(VENUE, "ETH_USDT", category="Spot") == _t("ETHUSDT")


async def test_client_reverse_lookup_refetches_for_an_unknown_ticker(
    broker: Broker, served
) -> None:
    plane, source = served
    client = SymbolClient(broker)
    await client.symbol_for(VENUE, "BTC_USDT", category="Spot")  # warm

    source.instruments = [_inst("BTC"), _inst("ETH"), _inst("SOL")]
    await plane.refresh()

    assert await client.symbol_for(VENUE, "SOL_USDT", category="Spot") == _t("SOLUSDT")


async def test_client_reverse_lookup_raises_for_unknown(
    broker: Broker, served
) -> None:
    client = SymbolClient(broker)
    with pytest.raises(SymbolNotFoundError, match="NOPE_USDT"):
        await client.symbol_for(VENUE, "NOPE_USDT", category="Spot")


async def test_client_satisfies_the_resolver_protocol(broker: Broker) -> None:
    """The adapter's dependency is the protocol, not this class."""
    from mft.exchange.symbols import SymbolResolver

    resolver: SymbolResolver = SymbolClient(broker)
    assert hasattr(resolver, "exch_ticker")
    assert hasattr(resolver, "symbol_for")
