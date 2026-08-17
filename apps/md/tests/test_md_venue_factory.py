"""MD venue dispatch — feed venue → the right public client."""

from __future__ import annotations

from decimal import Decimal

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.exchange import PaperExchange
from mftik.exchange.binance.future.public import BinanceFuturePublicClient
from mftik.exchange.binance.spot.public import BinanceSpotPublicClient
from mftik.exchange.bybit.public import BybitPublicClient
from mftik.exchange.errors import ExchangeError
from mftik.exchange.gate.spot.public import GateSpotPublicClient
from mftik.exchange.paper.public import PaperPublicClient
from mftik.exchange.venues import UnknownVenueError
from mftik_md.session import PaperPublicFactory, VenuePublicFactory


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
    client = await factory.create("Gate")
    assert isinstance(client, GateSpotPublicClient)
    assert client.name == "Gate"
    # Public market data only — the socket carries no credentials.
    assert not client.ws.authenticated


async def test_venue_name_is_normalized(broker: Broker) -> None:
    factory = VenuePublicFactory(broker)
    client = await factory.create("  gate ")
    assert isinstance(client, GateSpotPublicClient)


async def test_unknown_venue_is_rejected(broker: Broker) -> None:
    factory = VenuePublicFactory(broker)
    with pytest.raises(UnknownVenueError):
        await factory.create("gate-spot")


async def test_binance_venue_builds_a_binance_public_client(
    broker: Broker,
) -> None:
    factory = VenuePublicFactory(broker)
    client = await factory.create("Binance")
    assert isinstance(client, BinanceSpotPublicClient)
    assert client.name == "Binance"
    # Public market data only — neither of Binance's two sockets carries
    # credentials, so MD can run a feed without a trading account.
    assert not client.api.authenticated


async def test_binance_future_venue_builds_its_own_public_client(
    broker: Broker,
) -> None:
    """A separate venue, not a category of ``Binance``: separate hosts entirely.

    It is also the only Binance client that holds no WebSocket API socket —
    futures serves its candles and its instrument listing over REST alone.
    """
    factory = VenuePublicFactory(broker)
    client = await factory.create("BinanceFuture")
    assert isinstance(client, BinanceFuturePublicClient)
    assert client.name == "BinanceFuture"
    # Liquidations are this venue's own feed; MD refuses the topic elsewhere.
    assert hasattr(client, "stream_liquidation")


async def test_bybit_venue_builds_one_client_for_every_category(
    broker: Broker,
) -> None:
    """A unified venue is still one connector here.

    Bybit needs a socket per category, but that is the connector's business:
    it opens them on first use, so a session streaming only spot never holds
    one to the perp book.
    """
    factory = VenuePublicFactory(broker)
    client = await factory.create("Bybit")

    assert isinstance(client, BybitPublicClient)
    assert client.name == "Bybit"
    # Nothing connected eagerly, and no credentials: market data is open.
    assert client._feeds == {}


async def test_registered_venue_without_a_client_is_rejected(
    broker: Broker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A venue TD can trade but MD cannot read must fail at create."""
    from mftik.exchange import venues

    kraken = venues.Venue(name="Kraken", label="Kraken Spot")
    monkeypatch.setitem(venues.VENUES, kraken.name, kraken)

    factory = VenuePublicFactory(broker)
    with pytest.raises(ExchangeError, match="no public client"):
        await factory.create("Kraken")
