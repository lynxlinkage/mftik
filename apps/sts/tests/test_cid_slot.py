"""Per-session client_order_id slots — allocation and ownership filtering."""

from __future__ import annotations

from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange.models import Side
from mft.protocol import StsCreateSessionRequest
from mft_sts.client_order_id import SLOT_MASK, ClientOrderIdFactory, slot_of
from mft_sts.impl import register
from mft_sts.session import SessionManager
from mft_sts.strategy import Strategy


class SlotStrategy(Strategy):
    name = "slot_probe"
    id = 5


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
def manager(broker: Broker) -> SessionManager:
    register(SlotStrategy)
    return SessionManager(broker, heartbeat_interval=0.1)


async def _create(manager: SessionManager, session_id: str):
    await manager.create_session(
        StsCreateSessionRequest(
            session_id=session_id, created_by=1, strategy="slot_probe"
        )
    )
    session = manager.get(session_id)
    assert session is not None
    return session


async def test_same_strategy_class_gets_distinct_slots(
    manager: SessionManager,
) -> None:
    a = await _create(manager, "slot-a")
    b = await _create(manager, "slot-b")

    assert a.strategy.id == b.strategy.id == 5  # same class
    assert a.cid_slot != b.cid_slot
    assert 0 <= a.cid_slot <= SLOT_MASK
    assert 0 <= b.cid_slot <= SLOT_MASK

    await manager.close_all()


async def test_lockstep_sessions_mint_distinct_cids(
    manager: SessionManager,
) -> None:
    """The regression: both submit their n-th order in the same millisecond.

    Driven through standalone factories so ``now`` is pinned — with a real
    clock the two sessions might land in different milliseconds and pass
    even if they shared a slot.
    """
    a = await _create(manager, "lock-a")
    b = await _create(manager, "lock-b")

    now = 1_800_000_000.0
    fa = ClientOrderIdFactory(a.cid_slot)
    fb = ClientOrderIdFactory(b.cid_slot)
    ids_a = [fa.next(now=now) for _ in range(3)]
    ids_b = [fb.next(now=now) for _ in range(3)]

    assert not set(ids_a) & set(ids_b)
    assert all(a.strategy.owns(cid) for cid in ids_a)
    assert not any(a.strategy.owns(cid) for cid in ids_b)
    assert all(slot_of(cid) == b.cid_slot for cid in ids_b)

    await manager.close_all()


async def test_submitted_cid_carries_the_session_slot(
    manager: SessionManager,
) -> None:
    """The live order-entry path must use the session's slot, not the class."""
    a = await _create(manager, "submit-a")
    b = await _create(manager, "submit-b")

    cid = await a.strategy.oms.submit_order(
        7, symbol="BTCUSDT", side=Side.BUY, qty=Decimal("0.01"),
        price=Decimal("1000"),
    )
    assert slot_of(cid) == a.cid_slot
    assert a.strategy.owns(cid)
    assert not b.strategy.owns(cid)

    await manager.close_all()


async def test_owns_rejects_junk(manager: SessionManager) -> None:
    session = await _create(manager, "owns-1")
    strat = session.strategy

    assert not strat.owns(None)
    assert not strat.owns("not-a-number")
    assert not strat.owns("")

    await manager.close_all()


async def test_slot_survives_session_churn(manager: SessionManager) -> None:
    """A closed session's slot is not handed straight back to the next one."""
    a = await _create(manager, "churn-a")
    first = a.cid_slot
    await manager.close("churn-a")

    b = await _create(manager, "churn-b")
    assert b.cid_slot != first

    await manager.close_all()
