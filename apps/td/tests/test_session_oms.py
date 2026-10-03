from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from mftik.exchange import PaperExchange, Side
from mftik.exchange.models import OrderStatus, limit_order
from mftik_td.session import PaperSessionFactory


class _Quiet:
    """Enough of a broker for a session that only publishes as a side effect."""

    async def publish(self, subject: str, envelope: object) -> None:
        return None


@pytest.fixture
async def paper() -> PaperExchange:
    exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
        seed=7,
    )
    await exchange.start()
    try:
        yield exchange
    finally:
        await exchange.stop()


@pytest.mark.component
@pytest.mark.real_sleep(reason="Session.start arms a sweep that sleeps")
async def test_oms_updates_from_session_callbacks(paper: PaperExchange) -> None:
    """Direct handler call: B4-05 (#205).

    The book is the session's. The broker is not on this path.
    """
    factory = PaperSessionFactory(_Quiet(), paper)  # type: ignore[arg-type]
    factory.bind_api(1, api_key="key-1", api_secret="sec-1")
    session = await factory.create(1)
    await session.start()
    try:
        private = session.private
        order = await private.place_order(
            limit_order(
                ticker="Paper_Spot_BTCUSDT",
                side=Side.BUY,
                qty=Decimal("0.01"),
                price=Decimal("1000"),
            )
        )
        view = session.oms.view()
        for _ in range(5):
            if order.client_order_id in view.orders:
                break
            await asyncio.sleep(0)
            view = session.oms.view()
        # The book is keyed by client_order_id, not the venue's id.
        assert order.client_order_id in view.orders
        assert view.orders[order.client_order_id].status is OrderStatus.NEW
        assert private.api_key == "key-1"
    finally:
        await session.destroy()


async def test_paper_factory_isolates_api_keys() -> None:
    """Direct handler call: B4-05 (#205)."""
    paper = PaperExchange(tick_interval=60)
    factory = PaperSessionFactory(_Quiet(), paper)  # type: ignore[arg-type]
    factory.bind_api(1, "alice-key", "alice-secret")
    factory.bind_api(2, "bob-key", "bob-secret")

    s1 = await factory.create(1)
    s2 = await factory.create(2)
    assert s1.private.api_key == "alice-key"
    assert s2.private.api_key == "bob-key"
