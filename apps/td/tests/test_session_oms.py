from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange, Side
from mftik.exchange.models import OrderStatus, limit_order
from mftik_td.session import PaperSessionFactory


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=7,
    ) as ex:
        yield ex


@pytest.fixture
def factory(broker: Broker, paper: PaperExchange) -> PaperSessionFactory:
    return PaperSessionFactory(broker, paper)


@pytest.mark.asyncio
async def test_oms_updates_from_session_callbacks(
    broker: Broker, factory: PaperSessionFactory
) -> None:
    factory.bind_api(1, api_key="key-1", api_secret="sec-1")
    session = await factory.create(1)
    await session.start()

    private = session.private
    order = await private.place_order(limit_order(
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        qty=Decimal("0.01"),
        price=Decimal("1000"),
    ))
    await asyncio.sleep(0.05)

    view = session.oms.view()
    # The book is keyed by client_order_id, not the venue's id.
    assert order.client_order_id in view.orders
    assert view.orders[order.client_order_id].status is OrderStatus.NEW
    assert private.api_key == "key-1"

    await session.destroy()


@pytest.mark.asyncio
async def test_paper_factory_isolates_api_keys(
    broker: Broker, paper: PaperExchange
) -> None:
    factory = PaperSessionFactory(broker, paper)
    factory.bind_api(1, "alice-key", "alice-secret")
    factory.bind_api(2, "bob-key", "bob-secret")

    s1 = await factory.create(1)
    s2 = await factory.create(2)
    assert s1.private.api_key == "alice-key"
    assert s2.private.api_key == "bob-key"
    await s1.destroy()
    await s2.destroy()
