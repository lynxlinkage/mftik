"""TD reads a venue's capabilities off the connector, not off a base class."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange.models import (
    Balance,
    Order,
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
)
from mft.exchange.oms import Position
from mft_td.session.session import Session

API_ID = 11


class MinimalConnector:
    """Everything TD requires of a venue, and nothing it treats as optional.

    Inherits nothing. That is the shape being tested — a connector satisfies
    what TD needs by having the methods, not by being registered anywhere.
    """

    name = "minimal"

    def __init__(self) -> None:
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        return Order(
            order_id="o-1",
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            qty=request.qty,
            price=request.price,
            status=OrderStatus.NEW,
        )

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        raise NotImplementedError

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        return []

    async def fetch_balances(self) -> list[Balance]:
        return [Balance(asset="USDT", free=Decimal("100"), locked=Decimal("0"))]

    def stream_orders(self) -> AsyncIterator[Order]:
        return self._idle()

    def stream_fills(self) -> AsyncIterator[Order]:
        return self._idle()

    def stream_balances(self) -> AsyncIterator[Balance]:
        return self._idle()

    @staticmethod
    async def _idle() -> AsyncIterator:
        while True:
            await asyncio.sleep(3600)
            yield  # pragma: no cover


class FullConnector(MinimalConnector):
    """A venue that does offer the three optional reads."""

    name = "full"

    def __init__(self) -> None:
        super().__init__()
        self.reconnect_hooks: list[object] = []
        self.resolved: list[str] = []

    async def fetch_positions(self) -> list[Position]:
        return [Position(symbol="BTCUSDT", qty=Decimal("2"))]

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, symbol: str | None = None
    ) -> Order | None:
        self.resolved.append(client_order_id)
        return None

    def on_reconnect(self, callback: object) -> None:
        self.reconnect_hooks.append(callback)


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-caps"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


def _session(broker: Broker, private: object) -> Session:
    return Session(api_id=API_ID, broker=broker, private=private)  # type: ignore[arg-type]


def _pending(cid: str = "cid-1") -> Order:
    return Order(
        order_id="",
        client_order_id=cid,
        symbol="BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("1"),
        price=Decimal("50000"),
        status=OrderStatus.PENDING_NEW,
    )


# --- positions --------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_venue_without_positions_reports_none_not_empty(
    broker: Broker,
) -> None:
    """"No such method" and "no positions" are different answers.

    TD used to tell them apart by comparing the implementation against the
    base class. A spot venue simply has no method now, and ``None`` keeps
    recon from overwriting a book with an emptiness the venue never claimed.
    """
    session = _session(broker, MinimalConnector())

    view = await session.reconcile()

    assert view.positions == {}
    assert not hasattr(session.private, "fetch_positions")


@pytest.mark.asyncio
async def test_a_venue_with_positions_has_them_applied(broker: Broker) -> None:
    session = _session(broker, FullConnector())

    view = await session.reconcile()

    assert "BTCUSDT" in view.positions
    assert view.positions["BTCUSDT"].qty == Decimal("2")


# --- resolving an unacknowledged order --------------------------------------


@pytest.mark.asyncio
async def test_a_venue_that_cannot_resolve_by_cid_leaves_it_unknown(
    broker: Broker,
) -> None:
    """The way out of UNKNOWN is optional, and its absence is not an error."""
    session = _session(broker, MinimalConnector())
    assert await session.resolve_unknown(_pending()) is None


@pytest.mark.asyncio
async def test_a_venue_that_can_resolve_by_cid_is_asked(broker: Broker) -> None:
    connector = FullConnector()
    session = _session(broker, connector)

    await session.resolve_unknown(_pending())

    assert connector.resolved == ["cid-1"]
