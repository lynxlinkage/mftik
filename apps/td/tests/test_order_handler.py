"""Paper ``td.order`` / unsettled views, called on the handler."""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from mftik.broker import RequestTimeoutError
from mftik.exchange import PaperExchange, Side
from mftik.exchange.errors import OrderError
from mftik.exchange.models import (
    Balance,
    Fill,
    Order,
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    limit_order,
)
from mftik.protocol import (
    STS_ORDER_SUBMIT,
    TD_CANCEL_REJECT,
    TD_ORDER_ACK,
    TD_ORDER_REJECT,
    Envelope,
    OrderAck,
    OrderCancel,
    OrderSubmit,
    RejectCode,
    TdLedgerViewRequest,
    TdOmsViewRequest,
    UntypedEnvelope,
)
from mftik_td.account import TICKET, AccountWorker
from mftik_td.account.session import Session

API = 7
SESSION = "sess"


class _Quiet:
    async def publish(self, subject: str, envelope: object) -> None:
        return None


def _submit(**overrides: object) -> OrderSubmit:
    payload: dict[str, object] = {
        "session_id": SESSION,
        "api_id": API,
        "universal_ticker": "Paper_Spot_BTCUSDT",
        "side": Side.BUY,
        "type": OrderType.LIMIT,
        "qty": Decimal("2"),
        "price": Decimal("49000"),
        "client_order_id": "cid-fill",
    }
    payload.update(overrides)
    return OrderSubmit(**payload)  # type: ignore[arg-type]


async def _started() -> tuple[AccountWorker, PaperExchange]:
    exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
    )
    exchange.register_api(
        "paper-key",
        "paper-secret",
        balances={"USDT": Decimal("1000000")},
    )
    await exchange.start()
    private = exchange.private(
        api_key="paper-key",
        api_secret="paper-secret",
        auto_register=False,
    )
    session = Session(
        api_id=API,
        broker=_Quiet(),  # type: ignore[arg-type]
        private=private,
    )
    worker = AccountWorker(
        API, venue="Paper", incarnation=1, private=private, session=session
    )
    await worker.resident.start()
    await worker.trading.activate()
    exchange.register_api(
        "maker-key",
        "maker-secret",
        balances={"BTC": Decimal("10"), "USDT": Decimal("1000000")},
    )
    maker = exchange.private(
        api_key="maker-key",
        api_secret="maker-secret",
        auto_register=False,
    )
    await maker.connect()
    await maker.place_order(
        limit_order(
            ticker="Paper_Spot_BTCUSDT",
            side=Side.SELL,
            qty=Decimal("1"),
            price=Decimal("49000"),
        )
    )
    return worker, exchange


async def _stop(worker: AccountWorker, exchange: PaperExchange) -> None:
    if worker.trading.active:
        await worker.trading.deactivate()
    await exchange.stop()


@pytest.mark.component
@pytest.mark.real_sleep(reason="Session.start arms a sweep that sleeps")
async def test_paper_submit_cancel_refusal_and_the_book_after_a_fill() -> None:
    worker, exchange = await _started()
    try:
        reply = await worker.orders(
            UntypedEnvelope.model_validate_json(
                Envelope[OrderSubmit]
                .wrap(
                    _submit(),
                    type=STS_ORDER_SUBMIT,
                    source="test",
                    session_id=SESSION,
                )
                .to_json()
            )
        )
        assert reply is not None
        assert reply.type == TD_ORDER_ACK
        ack = OrderAck.model_validate(reply.payload)
        assert ack.accepted is True
        assert ack.client_order_id == "cid-fill"

        view = await worker.oms.view(TdOmsViewRequest(api_id=API))
        booked = view.orders["cid-fill"]
        assert booked.filled_qty > 0
        assert booked.client_order_id == "cid-fill"

        cancelled = await worker.orders.cancel(
            OrderCancel(session_id=SESSION, api_id=API, client_order_id="cid-fill")
        )
        assert cancelled.accepted is True
        assert cancelled.client_order_id == "cid-fill"

        wrong = await worker.orders.submit(
            _submit(api_id=API + 1, client_order_id="cid-wrong")
        )
        assert wrong.accepted is False
        assert wrong.error_code == RejectCode.TD_WRONG_API_ID
        assert wrong.client_order_id == "cid-wrong"

        reduced = await worker.orders.submit(
            _submit(
                reduce_only=True,
                client_order_id="cid-reduce",
                qty=Decimal("0.01"),
                price=Decimal("1"),
            )
        )
        assert reduced.accepted is False
        assert reduced.error_code == RejectCode.TD_REDUCE_ONLY_UNSUPPORTED

        other_venue = await worker.orders.submit(
            _submit(
                universal_ticker="Binance_Spot_BTCUSDT",
                client_order_id="cid-venue",
                qty=Decimal("0.01"),
                price=Decimal("1"),
            )
        )
        assert other_venue.accepted is False
        assert other_venue.error_code == RejectCode.TD_WRONG_INSTRUMENT
        assert other_venue.client_order_id == "cid-venue"

        ledger = await worker.ledger.view(TdLedgerViewRequest(api_id=API))
        assert ledger.api_id == API
        with pytest.raises(NotImplementedError, match=TICKET):
            await worker.oms.view(TdOmsViewRequest(api_id=API, settled=True))
    finally:
        await _stop(worker, exchange)


class _Capture:
    def __init__(self) -> None:
        self.sent: list[tuple[str, UntypedEnvelope]] = []

    async def publish(self, subject: str, envelope: UntypedEnvelope) -> None:
        self.sent.append((subject, envelope))


def _codes(capture: _Capture, type_name: str) -> list[object]:
    return [
        envelope.payload.get("error_code")
        for _, envelope in capture.sent
        if envelope.type == type_name
    ]


class _ApplyThenTimeout:
    """Paper private, minus a cid lookup, that dies after the engine applies.

    ``PaperRemotePrivateClient`` has no ``fetch_order_by_client_order_id``.
    This stands in for that shape: the engine has the order, and the
    resolve path cannot ask for it.
    """

    name = "Paper"

    def __init__(self, inner: object, *, fail: frozenset[str]) -> None:
        self._inner = inner
        self._fail = fail

    @property
    def connected(self) -> bool:
        return bool(getattr(self._inner, "connected", False))

    async def connect(self) -> None:
        await self._inner.connect()  # type: ignore[attr-defined]

    async def close(self) -> None:
        await self._inner.close()  # type: ignore[attr-defined]

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        if "reject" in self._fail:
            raise OrderError("venue said no")
        placed = await self._inner.place_order(request)  # type: ignore[attr-defined]
        if "place" in self._fail:
            raise RequestTimeoutError("paper", "place", 0.5)
        return placed

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        cancelled = await self._inner.cancel_by_client_order_id(  # type: ignore[attr-defined]
            client_order_id
        )
        if "cancel" in self._fail:
            raise RequestTimeoutError("paper", "cancel", 0.5)
        return cancelled

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        return await self._inner.fetch_open_orders(symbol)  # type: ignore[attr-defined]

    async def fetch_balances(self) -> list[Balance]:
        return await self._inner.fetch_balances()  # type: ignore[attr-defined]

    def stream_orders(self) -> AsyncIterator[Order]:
        return _nothing()

    def stream_fills(self) -> AsyncIterator[Fill]:
        return _nothing()

    def stream_balances(self) -> AsyncIterator[Balance]:
        return _nothing()


async def _nothing() -> AsyncIterator[object]:
    if False:
        yield None


async def _timeout_worker(
    fail: frozenset[str],
) -> tuple[AccountWorker, PaperExchange, _Capture, object]:
    exchange = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=60,
    )
    exchange.register_api(
        "paper-key",
        "paper-secret",
        balances={"USDT": Decimal("1000000")},
    )
    await exchange.start()
    inner = exchange.private(
        api_key="paper-key",
        api_secret="paper-secret",
        auto_register=False,
    )
    private = _ApplyThenTimeout(inner, fail=fail)
    capture = _Capture()
    session = Session(
        api_id=API,
        broker=capture,  # type: ignore[arg-type]
        private=private,  # type: ignore[arg-type]
    )
    worker = AccountWorker(
        API, venue="Paper", incarnation=1, private=private, session=session
    )
    await worker.resident.start()
    await worker.trading.activate()
    return worker, exchange, capture, inner


@pytest.mark.component
@pytest.mark.real_sleep(reason="Session.start arms a sweep that sleeps")
async def test_a_timeout_after_the_venue_applied_leaves_the_order_unknown() -> (
    None
):
    worker, exchange, capture, inner = await _timeout_worker(frozenset({"place"}))
    cid = "cid-timeout"
    try:
        ack = await worker.orders.submit(
            _submit(client_order_id=cid, qty=Decimal("0.01"), price=Decimal("1"))
        )
        assert ack.accepted is True
        booked = worker.trading.oms.view().orders[cid]
        assert booked.status is OrderStatus.UNKNOWN
        resting = await inner.fetch_open_orders()
        assert any(order.client_order_id == cid for order in resting)
        assert _codes(capture, TD_ORDER_REJECT) == []
        assert RejectCode.VENUE_REJECTED not in _codes(capture, TD_CANCEL_REJECT)
    finally:
        await _stop(worker, exchange)


@pytest.mark.component
@pytest.mark.real_sleep(reason="Session.start arms a sweep that sleeps")
async def test_a_timeout_after_the_cancel_applied_is_not_reverted() -> None:
    worker, exchange, capture, inner = await _timeout_worker(frozenset({"cancel"}))
    cid = "cid-cancel-timeout"
    try:
        placed = await worker.orders.submit(
            _submit(client_order_id=cid, qty=Decimal("0.01"), price=Decimal("1"))
        )
        assert placed.accepted is True
        before = worker.trading.oms.view().orders[cid].status
        assert before is not OrderStatus.UNKNOWN
        ack = await worker.orders.cancel(
            OrderCancel(session_id=SESSION, api_id=API, client_order_id=cid)
        )
        assert ack.accepted is True
        booked = worker.trading.oms.view().orders[cid]
        assert booked.status is OrderStatus.UNKNOWN
        assert booked.status is not before
        resting = await inner.fetch_open_orders()
        assert all(order.client_order_id != cid for order in resting)
        assert _codes(capture, TD_CANCEL_REJECT) == [RejectCode.TD_SEND_FAILED]
        assert RejectCode.VENUE_REJECTED not in _codes(capture, TD_ORDER_REJECT)
    finally:
        await _stop(worker, exchange)


@pytest.mark.component
@pytest.mark.real_sleep(reason="Session.start arms a sweep that sleeps")
async def test_an_exchange_error_still_rejects_and_drops_the_order() -> None:
    worker, exchange, capture, inner = await _timeout_worker(frozenset({"reject"}))
    cid = "cid-exchange-no"
    try:
        ack = await worker.orders.submit(
            _submit(client_order_id=cid, qty=Decimal("0.01"), price=Decimal("1"))
        )
        assert ack.accepted is True
        assert cid not in worker.trading.oms.view().orders
        resting = await inner.fetch_open_orders()
        assert all(order.client_order_id != cid for order in resting)
        assert _codes(capture, TD_ORDER_REJECT) == [RejectCode.VENUE_REJECTED]
    finally:
        await _stop(worker, exchange)
