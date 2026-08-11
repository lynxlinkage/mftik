"""Account RPC — ensure_leverage on ``td.account.{api_id}``."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange
from mft.exchange.errors import ExchangeError
from mft.exchange.tickers import UniversalTicker
from mft.protocol import (
    STS_ENSURE_LEVERAGE,
    STS_LEASE_HEARTBEAT,
    Envelope,
    EnsureLeverage,
    LeaseHeartbeat,
    LeverageAck,
    RejectCode,
    TdAttachRequest,
    Topics,
)
from mft_td.session import PaperSessionFactory, SessionManager

API_ID = 42
SESSION = "sts-lev"
#: Same venue as the paper session; category Perp so ensure_leverage runs.
PERP = "Paper_Perp_BTCUSDT"


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
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=7
    ) as ex:
        yield ex


@pytest.fixture
def factory(broker: Broker, paper: PaperExchange) -> PaperSessionFactory:
    return PaperSessionFactory(broker, paper)


async def _lease_publisher(
    broker: Broker, session_id: str, stop: asyncio.Event
) -> None:
    token = 0
    topic = Topics.sts_td_session(session_id)
    while not stop.is_set():
        token += 1
        await broker.publish(
            topic,
            Envelope[LeaseHeartbeat].wrap(
                LeaseHeartbeat(session_id=session_id, token=token),
                type=STS_LEASE_HEARTBEAT,
                source="sts",
                session_id=session_id,
            ),
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except TimeoutError:
            continue


@pytest.fixture
async def attached(broker: Broker, factory: PaperSessionFactory):
    manager = SessionManager(factory, broker, lease_grace=2.0)
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SESSION, stop))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    yield manager
    stop.set()
    await asyncio.gather(pub, return_exceptions=True)
    await manager.close_all()


def _ensure_envelope(**overrides: Any) -> Envelope[Any]:
    payload: dict[str, Any] = {
        "session_id": SESSION,
        "api_id": API_ID,
        "universal_ticker": PERP,
    }
    payload.update(overrides)
    return Envelope[EnsureLeverage].wrap(
        EnsureLeverage.model_validate(payload),
        type=STS_ENSURE_LEVERAGE,
        source="sts",
        session_id=SESSION,
    )


async def test_paper_venue_refuses_leverage_lookup(
    broker: Broker, attached: SessionManager
) -> None:
    """Paper has no fetch_leverage — TD answers unavailable, not a hang."""
    reply = await broker.request(
        Topics.td_account(API_ID), _ensure_envelope(), timeout=2.0
    )
    ack = LeverageAck.model_validate(reply.payload)
    assert ack.ok is False
    assert ack.error_code == RejectCode.TD_LEVERAGE_UNAVAILABLE


async def test_ensure_leverage_returns_venue_figure(
    broker: Broker, attached: SessionManager
) -> None:
    acct = attached._accounts[API_ID]

    async def _fetch(ticker: UniversalTicker) -> Decimal:
        assert str(ticker) == PERP
        return Decimal("7")

    acct.trading.private.fetch_leverage = _fetch  # type: ignore[attr-defined]

    reply = await broker.request(
        Topics.td_account(API_ID), _ensure_envelope(), timeout=2.0
    )
    ack = LeverageAck.model_validate(reply.payload)
    assert ack.ok is True
    assert ack.leverage == Decimal("7")
    assert acct.trading.cached_leverage(PERP) == Decimal("7")

    # Second call is a cache hit — even if the venue would now fail.
    async def _boom(ticker: UniversalTicker) -> Decimal:
        raise ExchangeError("should not be called")

    acct.trading.private.fetch_leverage = _boom  # type: ignore[attr-defined]
    reply2 = await broker.request(
        Topics.td_account(API_ID), _ensure_envelope(), timeout=2.0
    )
    assert LeverageAck.model_validate(reply2.payload).leverage == Decimal("7")


async def test_wrong_session_is_refused(
    broker: Broker, attached: SessionManager
) -> None:
    reply = await broker.request(
        Topics.td_account(API_ID),
        _ensure_envelope(session_id="not-attached"),
        timeout=2.0,
    )
    ack = LeverageAck.model_validate(reply.payload)
    assert ack.ok is False
    assert ack.error_code == RejectCode.TD_SESSION_NOT_ATTACHED
