"""TD builds the private client from ``apis.venue``, not from a hardcoded one."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange
from mft.exchange.errors import ExchangeError
from mft.exchange.gate.spot.private import GateSpotPrivateClient
from mft_td.session import PaperSessionFactory, VenueSessionFactory


@dataclass
class FakeApiRow:
    """Stands in for the ``apis`` row TD loads by api_id."""

    id: int
    venue: str
    api_key: str
    api_secret: str
    passphrase: str | None = None


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


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=3
    ) as ex:
        yield ex


def _factory(broker: Broker, rows: dict[int, FakeApiRow], **kwargs):
    async def load_api(api_id: int) -> FakeApiRow | None:
        return rows.get(api_id)

    return VenueSessionFactory(broker, load_api=load_api, **kwargs)


async def test_paper_venue_builds_a_paper_session(
    broker: Broker, paper: PaperExchange
) -> None:
    rows = {
        1: FakeApiRow(id=1, venue="Paper", api_key="k1", api_secret="s1"),
    }
    factory = _factory(broker, rows, paper=PaperSessionFactory(broker, paper))

    session = await factory.create(1)

    assert session.api_id == 1
    # Credentials come from the row, not synthesized from the api_id.
    assert session.private.api_key == "k1"


async def test_gate_spot_venue_builds_a_gate_client(broker: Broker) -> None:
    rows = {
        7: FakeApiRow(
            id=7, venue="Gate", api_key="gk", api_secret="gs"
        ),
    }
    factory = _factory(broker, rows)

    session = await factory.create(7)

    assert isinstance(session.private, GateSpotPrivateClient)
    assert session.private.name == "Gate"
    assert session.private.api_key == "gk"
    assert session.api_id == 7
    # Built but not connected — the manager starts it.
    assert not session.private.connected


async def test_venue_is_resolved_case_insensitively(broker: Broker) -> None:
    rows = {
        7: FakeApiRow(id=7, venue="  gate ", api_key="gk", api_secret="gs"),
    }
    factory = _factory(broker, rows)

    session = await factory.create(7)
    assert isinstance(session.private, GateSpotPrivateClient)


async def test_unknown_venue_fails_loudly(broker: Broker) -> None:
    """A bad venue must not quietly fall back to paper and trade elsewhere."""
    rows = {
        9: FakeApiRow(id=9, venue="gate-spot", api_key="k", api_secret="s"),
    }
    factory = _factory(broker, rows)

    with pytest.raises(ExchangeError, match="unknown venue"):
        await factory.create(9)


async def test_missing_credential_row_fails(broker: Broker) -> None:
    factory = _factory(broker, {})

    with pytest.raises(ExchangeError, match="no api credential"):
        await factory.create(404)


async def test_gate_client_gets_the_symbol_plane(broker: Broker) -> None:
    """Symbol translation must go through the plane, not string surgery."""
    from mft.exchange.symbols import SymbolResolver

    rows = {7: FakeApiRow(id=7, venue="Gate", api_key="gk", api_secret="gs")}
    factory = _factory(broker, rows)

    session = await factory.create(7)

    resolver: SymbolResolver = session.private.symbols
    assert hasattr(resolver, "exch_ticker")
    assert hasattr(resolver, "symbol_for")


async def test_one_symbol_client_is_shared_across_sessions(
    broker: Broker,
) -> None:
    """Its cache is what keeps symbol lookups off the order path."""
    rows = {
        7: FakeApiRow(id=7, venue="Gate", api_key="a", api_secret="b"),
        8: FakeApiRow(id=8, venue="Gate", api_key="c", api_secret="d"),
    }
    factory = _factory(broker, rows)

    first = await factory.create(7)
    second = await factory.create(8)

    assert first.private.symbols is second.private.symbols
