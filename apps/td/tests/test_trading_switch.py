"""Trading-bit apply: idempotent, drain, and a separate paper resident client."""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest
from mftik.exchange.models import Order, OrderStatus, OrderType, Side
from mftik.protocol import TD_ACCOUNT_TRADING, Envelope, TdAccountTrading
from mftik_td.account import AccountWorker
from mftik_td.account._ticket import TICKET
from mftik_td.account.trading import TradingLayer
from mftik_td.oms import Ledger, Oms

API = 7


class _Resident:
    api_id = API


class _Book:
    def __init__(self) -> None:
        self.oms = Oms()
        self.ledger = Ledger()
        self.private = object()
        self.starts = 0
        self.destroys = 0
        self.destroyed = False

    async def start(self) -> None:
        self.starts += 1

    async def destroy(self) -> None:
        self.destroys += 1
        self.destroyed = True


class _FailingBook(_Book):
    async def start(self) -> None:
        self.starts += 1
        raise RuntimeError("recon failed")


class _Conn:
    def __init__(self) -> None:
        self.connected = False
        self.closes = 0

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False
        self.closes += 1


def _push(active: bool):
    return Envelope[TdAccountTrading].wrap(
        TdAccountTrading(api_id=API, active=active),
        type=TD_ACCOUNT_TRADING,
        source="test",
    )


def _order(client_order_id: str) -> Order:
    return Order(
        order_id=f"v-{client_order_id}",
        client_order_id=client_order_id,
        universal_ticker="Bybit_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("0.01"),
        price=Decimal("1"),
        status=OrderStatus.NEW,
    )


async def test_a_false_push_before_the_first_true_keeps_the_unstarted_book() -> None:
    book = _Book()
    layer = TradingLayer(_Resident(), session=book)  # type: ignore[arg-type]
    reply = await layer.handle(_push(False))
    assert reply is not None
    assert reply.payload.active is False  # type: ignore[union-attr]
    assert book.starts == 0
    assert book.destroys == 0
    assert layer.active is False


async def test_a_repeated_true_does_not_start_again() -> None:
    book = _Book()
    layer = TradingLayer(_Resident(), session=book)  # type: ignore[arg-type]
    first = await layer.handle(_push(True))
    second = await layer.handle(_push(True))
    assert first is not None and second is not None
    assert first.payload.active is True  # type: ignore[union-attr]
    assert second.payload.active is True  # type: ignore[union-attr]
    assert book.starts == 1


async def test_deactivate_logs_resting_orders_and_does_not_drop_them(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    book = _Book()
    book.oms.handle_order(_order("cid-rest"))
    layer = TradingLayer(_Resident(), session=book)  # type: ignore[arg-type]
    await layer.activate()
    await layer.deactivate()
    assert layer.active is False
    assert book.destroys == 1
    assert book.oms.get_order("cid-rest") is not None
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "cid-rest" in text
    assert "resting" in text


async def test_a_failed_start_is_dropped_and_the_next_activate_builds_another() -> None:
    built: list[_Book] = []

    async def factory() -> _Book:
        book: _Book = _FailingBook() if not built else _Book()
        built.append(book)
        return book

    layer = TradingLayer(_Resident())  # type: ignore[arg-type]
    layer.set_factory(factory)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="recon failed"):
        await layer.activate()
    assert layer.active is False
    assert built[0].destroyed is True
    assert layer.session is None
    await layer.activate()
    assert layer.active is True
    assert built[1].starts == 1
    assert layer.session is built[1]


async def test_drain_timeout_leaves_the_layer_up() -> None:
    book = _Book()
    worker = AccountWorker(API, venue="Bybit", session=book)  # type: ignore[arg-type]
    await worker.trading.activate()
    worker.trading.drain_timeout_s = 0
    worker.trading._busy = 1  # noqa: SLF001 — the in-flight counter has no other seam
    await worker.trading.deactivate()
    assert worker.trading.active is True
    assert book.destroys == 0


async def test_activate_and_deactivate_without_a_book_still_name_the_ticket() -> None:
    worker = AccountWorker(API, venue="Bybit")
    with pytest.raises(NotImplementedError, match=TICKET):
        await worker.trading.activate()
    with pytest.raises(NotImplementedError, match=TICKET):
        await worker.trading.deactivate()


async def test_cancel_session_while_inactive_does_not_reach_the_book() -> None:
    book = _Book()
    reached: list[str] = []

    class _Orders:
        async def submit(self, request: object) -> None:
            return None

        async def cancel(self, request: object) -> None:
            return None

        async def cancel_session(self, request: object) -> str:
            reached.append(request.session_id)  # type: ignore[attr-defined]
            return "ran"

    layer = TradingLayer(_Resident(), session=book)  # type: ignore[arg-type]
    orders = _Orders()
    layer.arm(orders)  # type: ignore[arg-type]
    refused = await orders.cancel_session(_Cancel("sess-off"))
    assert reached == []
    assert refused.ok is False
    assert refused.session_id == "sess-off"
    assert refused.unconfirmed == []
    assert book.destroys == 0

    await layer.activate()
    ran = await orders.cancel_session(_Cancel("sess-on"))
    assert ran == "ran"
    assert reached == ["sess-on"]


class _Cancel:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id


async def test_paper_deactivate_does_not_close_the_resident_connector() -> None:
    resident = _Conn()
    book = _Book()
    worker = AccountWorker(
        API,
        venue="Paper",
        session=book,  # type: ignore[arg-type]
        resident_connector=resident,  # type: ignore[arg-type]
    )
    await worker.resident.start()
    assert resident.connected is True
    await worker.trading.activate()
    await worker.trading.deactivate()
    assert worker.trading.active is False
    assert book.destroys == 1
    assert resident.connected is True
    assert resident.closes == 0
    await worker.resident.close()
    assert resident.connected is False
