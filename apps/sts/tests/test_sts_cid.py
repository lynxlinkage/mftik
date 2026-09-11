"""Per-session client_order_id — the packed session field is the session id."""

from __future__ import annotations

from decimal import Decimal

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange.models import Side
from mftik.protocol import StsCreateSessionRequest
from mftik.strategy import Strategy
from mftik.strategy.client_order_id import (
    ClientOrderIdFactory,
    session_id_of,
)
from mftik_sts.impl import register
from mftik_sts.session import SessionManager


class CidStrategy(Strategy):
    name = "cid_probe"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


@pytest.fixture
def manager(broker: Broker) -> SessionManager:
    register(CidStrategy)
    return SessionManager(broker, heartbeat_interval=0.1)


async def _create(manager: SessionManager, session_id: str):
    await manager.create_session(
        StsCreateSessionRequest(
            session_id=session_id, created_by=1, strategy="cid_probe"
        )
    )
    session = manager.get(session_id)
    assert session is not None
    return session


async def test_same_strategy_class_gets_distinct_session_ids(
    manager: SessionManager,
) -> None:
    a = await _create(manager, "aa0001")
    b = await _create(manager, "aa0002")

    assert a.strategy.name == b.strategy.name == "cid_probe"
    assert a.session_id != b.session_id

    await manager.close_all()


async def test_lockstep_sessions_mint_distinct_cids(
    manager: SessionManager,
) -> None:
    """The regression: both submit their n-th order in the same second.

    Driven through standalone factories so ``now`` is pinned — with a real
    clock the two sessions might land in different seconds and pass even if
    they packed the same session field.
    """
    a = await _create(manager, "aa0003")
    b = await _create(manager, "aa0004")

    now = 1_800_000_000.0
    fa = ClientOrderIdFactory(a.session_id)
    fb = ClientOrderIdFactory(b.session_id)
    ids_a = [fa.next(now=now) for _ in range(3)]
    ids_b = [fb.next(now=now) for _ in range(3)]

    assert not set(ids_a) & set(ids_b)
    assert all(a.strategy.owns(cid) for cid in ids_a)
    assert not any(a.strategy.owns(cid) for cid in ids_b)
    assert all(session_id_of(cid) == b.session_id for cid in ids_b)

    await manager.close_all()


async def test_submitted_cid_carries_the_session_id(
    manager: SessionManager,
) -> None:
    """The live order-entry path must pack this session's id, not a hash.

    No TD is attached, so the submit goes unacked — which also pins down the
    other half of the contract: the minted id is recorded even when the order
    never lands.
    """
    a = await _create(manager, "aa0005")
    b = await _create(manager, "aa0006")

    a.strategy.oms._ack_timeout = 0.2

    assert not await a.strategy.oms.submit_order(
        7, ticker="Paper_Spot_BTCUSDT", side=Side.BUY, qty=Decimal("0.01"),
        price=Decimal("1000"),
    )
    cid = a.strategy.oms.last_client_order_id
    assert cid is not None
    assert session_id_of(cid) == a.session_id
    assert a.strategy.owns(cid)
    assert not b.strategy.owns(cid)

    await manager.close_all()


async def test_owns_rejects_junk(manager: SessionManager) -> None:
    session = await _create(manager, "aa0007")
    strat = session.strategy

    assert not strat.owns(None)
    assert not strat.owns("not-a-number")
    assert not strat.owns("")

    await manager.close_all()
