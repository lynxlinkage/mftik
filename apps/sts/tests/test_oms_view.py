"""Strategy-side OMS reads — resolve must return None, not raise.

``td: {}`` is a legal MD-only run. Several attached accounts must be named.
``td_sole()`` raises in both cases; ``view`` / ``order`` must not.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange.models import Order, OrderStatus, OrderType, Side
from mftik.protocol import Topics
from mftik.strategy.oms import StrategyOms


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


def _oms(broker: Broker, *api_ids: int) -> StrategyOms:
    oms = StrategyOms()
    session = SimpleNamespace(broker=broker, td_api_ids=list(api_ids))
    oms.bind(SimpleNamespace(session=session), cid_slot=1)
    return oms


async def _write(broker: Broker, api_id: int, cid: str) -> None:
    await broker.state_put(
        Topics.td_oms(api_id),
        cid,
        Order(
            client_order_id=cid,
            universal_ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("1"),
            price=Decimal("10"),
            status=OrderStatus.NEW,
        ),
    )


async def test_a_single_account_needs_no_api_id(broker: Broker) -> None:
    oms = _oms(broker, 7)
    await _write(broker, 7, "1")

    view = await oms.view()
    assert set(view.orders) == {"1"}


async def test_no_account_reads_as_an_empty_book(broker: Broker) -> None:
    """MD-only: ``td: {}``. The book is unanswerable, not an exception."""
    oms = _oms(broker)
    await _write(broker, 7, "1")

    assert (await oms.view()).orders == {}
    assert await oms.order("1") is None


async def test_multiple_accounts_must_be_named(broker: Broker) -> None:
    """Guessing which book the strategy meant would be a money bug."""
    oms = _oms(broker, 7, 8)
    await _write(broker, 7, "a")
    await _write(broker, 8, "b")

    assert (await oms.view()).orders == {}
    assert set((await oms.view(7)).orders) == {"a"}
    assert set((await oms.view(8)).orders) == {"b"}
    assert await oms.order("a") is None
    assert (await oms.order("a", 7)) is not None
