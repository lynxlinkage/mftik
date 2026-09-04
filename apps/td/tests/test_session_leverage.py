"""Session leverage and reserve — Perp and dated Future share the formula."""

from __future__ import annotations

from decimal import Decimal

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange.errors import ExchangeError
from mftik.exchange.models import (
    Balance,
    OrderType,
    PlaceOrderRequest,
    Side,
)
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol.messages import SymbolInfo
from mftik_td.oms import Ledger
from mftik_td.session.session import Session

DATED = UniversalTicker.parse("BinanceFuture_Future_BTCUSDT250926")
PERP = UniversalTicker.parse("BinanceFuture_Perp_BTCUSDT")
SPOT = UniversalTicker.parse("Binance_Spot_BTCUSDT")


class _Private:
    name = "BinanceFuture"


class _Symbols:
    async def get(self, ticker: UniversalTicker) -> SymbolInfo:
        return SymbolInfo(
            universal_ticker=str(ticker),
            base="BTC",
            quote="USDT",
            exch_ticker="BTCUSDT",
        )


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-session-leverage") as client:
        yield client


def _session(broker: Broker) -> Session:
    return Session(
        api_id=1,
        broker=broker,
        private=_Private(),  # type: ignore[arg-type]
        symbols=_Symbols(),  # type: ignore[arg-type]
        ledger=Ledger(),
    )


def _limit(ticker: UniversalTicker) -> PlaceOrderRequest:
    return PlaceOrderRequest(
        universal_ticker=str(ticker),
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("0.01"),
        price=Decimal("50000"),
        client_order_id="cid-1",
    )


async def test_ensure_leverage_answers_for_a_dated_future(broker: Broker) -> None:
    session = _session(broker)

    async def fetch(ticker: UniversalTicker) -> Decimal:
        assert ticker == DATED
        return Decimal("12")

    session.private.fetch_leverage = fetch  # type: ignore[attr-defined]
    assert await session.ensure_leverage(DATED) == Decimal("12")
    assert session.cached_leverage(DATED) == Decimal("12")


async def test_ensure_leverage_still_answers_for_a_perp(broker: Broker) -> None:
    session = _session(broker)

    async def fetch(ticker: UniversalTicker) -> Decimal:
        assert ticker == PERP
        return Decimal("7")

    session.private.fetch_leverage = fetch  # type: ignore[attr-defined]
    assert await session.ensure_leverage(PERP) == Decimal("7")


async def test_ensure_leverage_still_refuses_spot(broker: Broker) -> None:
    session = _session(broker)
    with pytest.raises(ExchangeError, match="Perp and Future"):
        await session.ensure_leverage(SPOT)


async def test_reserve_uses_cached_leverage_on_a_dated_future(
    broker: Broker,
) -> None:
    """Without the cache a Future order would lock the full notional."""
    session = _session(broker)
    session.ledger.apply_venue(Balance(asset="USDT", free=Decimal("1000")))
    session._leverage[str(DATED)] = Decimal("10")

    assert await session.reserve(_limit(DATED)) is None
    # 0.01 * 50000 / 10 = 50, not 500.
    assert session.ledger.available("USDT") == Decimal("950")
