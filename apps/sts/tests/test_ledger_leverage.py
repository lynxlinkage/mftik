"""StrategyLedger.ensure_leverage — STS cache + TD account RPC."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.protocol import (
    STS_ENSURE_LEVERAGE,
    TD_LEVERAGE_ACK,
    EnsureLeverage,
    Envelope,
    LeverageAck,
    RejectCode,
    Topics,
)
from mftik.strategy.ledger import StrategyLedger


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


def _ledger(broker: Broker, *api_ids: int) -> StrategyLedger:
    ledger = StrategyLedger()
    session = SimpleNamespace(
        broker=broker,
        td_api_ids=list(api_ids),
        session_id="sts-1",
        strategy=SimpleNamespace(name="test"),
    )
    ledger.bind(SimpleNamespace(session=session))
    return ledger


async def _serve_once(
    broker: Broker,
    api_id: int,
    stop: asyncio.Event,
    *,
    leverage: Decimal | None = Decimal("10"),
    ok: bool = True,
    reason: str = "",
    code: int = RejectCode.NONE,
) -> None:
    async for req in broker.serve(Topics.td_account(api_id), stop=stop):
        payload = EnsureLeverage.model_validate(req.envelope.payload)
        assert req.envelope.type == STS_ENSURE_LEVERAGE
        await req.reply(
            Envelope[LeverageAck].wrap(
                LeverageAck(
                    api_id=api_id,
                    universal_ticker=payload.universal_ticker,
                    ok=ok,
                    leverage=leverage if ok else None,
                    reason=reason,
                    error_code=code,
                ),
                type=TD_LEVERAGE_ACK,
                source="td",
            )
        )
        stop.set()
        return


async def test_ensure_leverage_caches_a_successful_ack(broker: Broker) -> None:
    ledger = _ledger(broker, 7)
    stop = asyncio.Event()
    serve = asyncio.create_task(_serve_once(broker, 7, stop, leverage=Decimal("8")))

    value = await ledger.ensure_leverage("Bybit_Perp_BTCUSDT", 7)

    assert value == Decimal("8")
    assert ledger.leverage("Bybit_Perp_BTCUSDT", 7) == Decimal("8")
    assert ledger.last_reject_code == RejectCode.NONE
    stop.set()
    await asyncio.gather(serve, return_exceptions=True)


async def test_ensure_leverage_skips_rpc_on_cache_hit(broker: Broker) -> None:
    ledger = _ledger(broker, 7)
    ledger._leverage[(7, "Bybit_Perp_BTCUSDT")] = Decimal("12")

    value = await ledger.ensure_leverage("Bybit_Perp_BTCUSDT", 7)

    assert value == Decimal("12")


async def test_ensure_leverage_maps_a_refusal(broker: Broker) -> None:
    ledger = _ledger(broker, 7)
    stop = asyncio.Event()
    serve = asyncio.create_task(
        _serve_once(
            broker,
            7,
            stop,
            ok=False,
            reason="spot has no leverage",
            code=RejectCode.TD_LEVERAGE_UNAVAILABLE,
        )
    )

    value = await ledger.ensure_leverage("Bybit_Spot_BTCUSDT", 7)

    assert value is None
    assert ledger.last_reject_code == RejectCode.TD_LEVERAGE_UNAVAILABLE
    assert "spot" in ledger.last_reject_reason
    stop.set()
    await asyncio.gather(serve, return_exceptions=True)


async def test_ensure_leverage_needs_api_id_with_two_accounts(
    broker: Broker,
) -> None:
    ledger = _ledger(broker, 7, 8)

    value = await ledger.ensure_leverage("Bybit_Perp_BTCUSDT")

    assert value is None
    assert ledger.last_reject_code == RejectCode.TD_INVALID_REQUEST
