"""MD venue dispatch — feed venue → the right public client."""

from __future__ import annotations

from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange
from mft.exchange.errors import ExchangeError
from mft.exchange.gate.spot.public import GateSpotPublicClient
from mft.exchange.paper.public import PaperPublicClient
from mft.exchange.venues import UnknownVenueError
from mft_md.session import PaperPublicFactory, VenuePublicFactory


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-md-factory"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05
    ) as ex:
        yield ex


async def test_paper_venue_keeps_the_paper_factory(
    broker: Broker, paper: PaperExchange
) -> None:
    factory = VenuePublicFactory(broker, paper=PaperPublicFactory(broker, paper))
    client = await factory.create("paper")
    assert isinstance(client, PaperPublicClient)


async def test_gate_spot_venue_builds_a_gate_public_client(
    broker: Broker,
) -> None:
    factory = VenuePublicFactory(broker)
    client = await factory.create("gate_spot")
    assert isinstance(client, GateSpotPublicClient)
    assert client.name == "gate_spot"
    # Public market data only — the socket carries no credentials.
    assert not client.ws.authenticated


async def test_venue_name_is_normalized(broker: Broker) -> None:
    factory = VenuePublicFactory(broker)
    client = await factory.create("  GATE_SPOT ")
    assert isinstance(client, GateSpotPublicClient)


async def test_unknown_venue_is_rejected(broker: Broker) -> None:
    factory = VenuePublicFactory(broker)
    with pytest.raises(UnknownVenueError):
        await factory.create("gate-spot")


async def test_registered_venue_without_a_client_is_rejected(
    broker: Broker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A venue TD can trade but MD cannot read must fail at create."""
    from mft.exchange import venues

    binance = venues.Venue(name="binance_spot", label="Binance Spot")
    monkeypatch.setitem(venues.VENUES, binance.name, binance)

    factory = VenuePublicFactory(broker)
    with pytest.raises(ExchangeError, match="no public client"):
        await factory.create("binance_spot")
