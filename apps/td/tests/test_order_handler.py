"""Paper ``td.order`` / unsettled views, called on the handler."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from mftik.broker import RequestTimeoutError
from mftik.clock import FakeClock
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
from mftik.exchange.oms import Position
from mftik.protocol import (
    STS_ORDER_SUBMIT,
    TD_CANCEL_REJECT,
    TD_ORDER_ACK,
    TD_ORDER_CANCEL_SESSION,
    TD_ORDER_REJECT,
    Envelope,
    OrderAck,
    OrderCancel,
    OrderSubmit,
    RejectCode,
    TdCancelSessionRequest,
    TdCancelSessionResult,
    TdLedgerViewRequest,
    TdOmsViewRequest,
    UntypedEnvelope,
)
from mftik.strategy.client_order_id import format_client_order_id
from mftik_td.account import AccountWorker
from mftik_td.account.session import Session
from mftik_td.oms import Oms

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
        settled = await worker.oms.view(
            TdOmsViewRequest(api_id=API, settled=True), timeout=0
        )
        assert all(
            order.status is not OrderStatus.UNKNOWN
            for order in settled.orders.values()
        )
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


# --- cancel_session (B6-03) ------------------------------------------------

SESSION_A = "abc123"
SESSION_B = "def456"
TICKER = "Paper_Spot_BTCUSDT"


def _cid(session_id: str, seq: int) -> str:
    return format_client_order_id(session_id, 1, seq)


def _resting(client_order_id: str, status: OrderStatus) -> Order:
    return Order(
        order_id=f"v-{client_order_id}",
        client_order_id=client_order_id,
        universal_ticker=TICKER,
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("0.01"),
        price=Decimal("60000"),
        status=status,
    )


def _oms(*orders: Order, position: bool = False) -> Oms:
    oms = Oms()
    positions = (
        [Position(universal_ticker="Paper_Perp_BTCUSDT", qty=Decimal("1"))]
        if position
        else None
    )
    oms.apply_reconcile(orders=orders, balances=[], positions=positions)
    return oms


def _with_session(
    private: object,
    oms: Oms,
    *,
    clock: FakeClock | None = None,
) -> tuple[AccountWorker, Session]:
    session = Session(
        api_id=API,
        broker=_Quiet(),  # type: ignore[arg-type]
        private=private,  # type: ignore[arg-type]
        oms=oms,
    )
    worker = AccountWorker(API, venue="Paper", session=session, clock=clock)
    return worker, session


class _Venue:
    """In-memory connector. No network. ``name`` is what Session reads."""

    name = "Paper"

    def __init__(self) -> None:
        self.connected = True
        self.cancelled: list[str] = []
        self.fetched: list[str] = []
        self.open_orders: list[Order] = []
        self.cancel_error: BaseException | None = None
        self.fetch_error: BaseException | None = None
        self.fetch_status: dict[str, OrderStatus] = {}
        self._release: asyncio.Event | None = None
        self._entered: asyncio.Event | None = None
        self.active = 0
        self.peak = 0

    def block_cancels(self) -> tuple[asyncio.Event, asyncio.Event]:
        self._entered = asyncio.Event()
        self._release = asyncio.Event()
        return self._entered, self._release

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        self.cancelled.append(client_order_id)
        self.active += 1
        self.peak = max(self.peak, self.active)
        if self._entered is not None and self.active == 1:
            self._entered.set()
        if self._release is not None:
            await self._release.wait()
        self.active -= 1
        if self.cancel_error is not None:
            raise self.cancel_error
        return _resting(client_order_id, OrderStatus.CANCELED)

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, ticker: object = None
    ) -> Order:
        self.fetched.append(client_order_id)
        if self.fetch_error is not None:
            raise self.fetch_error
        status = self.fetch_status.get(client_order_id, OrderStatus.NEW)
        return _resting(client_order_id, status)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        return list(self.open_orders)

    async def fetch_balances(self) -> list[Balance]:
        return []

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        raise NotImplementedError


class _Paper:
    """Paper's remote client: open orders and cancel, no per-cid lookup.

    Not a subclass of :class:`_Venue`. ``getattr`` would still find an
    inherited ``fetch_order_by_client_order_id``, and ``cancel_session``
    would chase instead of reconciling.
    """

    name = "Paper"

    def __init__(self) -> None:
        self.connected = True
        self.cancelled: list[str] = []
        self.open_orders: list[Order] = []

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        self.cancelled.append(client_order_id)
        return _resting(client_order_id, OrderStatus.CANCELED)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        return list(self.open_orders)

    async def fetch_balances(self) -> list[Balance]:
        return []


async def _arm(clock: FakeClock, task: asyncio.Task[object]) -> None:
    for _ in range(20):
        await asyncio.sleep(0)
        if clock._heap:  # noqa: SLF001 — the sleeper is registered, or the call finished
            return
        if task.done():
            return
    raise AssertionError("cancel_session did not arm its clock")


@pytest.mark.component
async def test_cancel_session_transport_timeout_leaves_the_order_unknown() -> None:
    """A transport failure inside cancel_session is the UNKNOWN path.

    The pending cancel is not reverted. The order stays UNKNOWN and is
    listed, not confirmed. The lookup fails too, so recon is not what
    settles it: this connector has the lookup, and the lookup errors.
    """
    clock = FakeClock()
    cid = _cid(SESSION_A, 1)
    venue = _Venue()
    venue.cancel_error = RequestTimeoutError("paper", "cancel", 0.5)
    venue.fetch_error = RequestTimeoutError("paper", "fetch", 0.5)
    worker, _session = _with_session(
        venue, _oms(_resting(cid, OrderStatus.NEW)), clock=clock
    )
    result = await worker.orders.cancel_session(
        TdCancelSessionRequest(session_id=SESSION_A), timeout=1.0
    )
    booked = worker.trading.oms.view().orders[cid]
    assert booked.status is OrderStatus.UNKNOWN
    assert result.ok is False
    assert result.unconfirmed == [cid]
    assert cid in venue.cancelled


@pytest.mark.component
async def test_unknown_order_filled_is_confirmed_and_resting_is_not() -> None:
    """An exchange refusal is reverted, then resolved. Only a terminal answer confirms.

    ``unknown order`` that the lookup finds filled is confirmed. The one
    it finds still resting is not, and it is not left ``PENDING_CANCEL``.
    """
    clock = FakeClock()
    filled = _cid(SESSION_A, 1)
    resting = _cid(SESSION_A, 2)
    other = _cid(SESSION_B, 1)
    venue = _Venue()
    venue.cancel_error = OrderError("unknown order")
    venue.fetch_status = {
        filled: OrderStatus.FILLED,
        resting: OrderStatus.NEW,
    }
    worker, _session = _with_session(
        venue,
        _oms(
            _resting(filled, OrderStatus.NEW),
            _resting(resting, OrderStatus.NEW),
            _resting(other, OrderStatus.NEW),
        ),
        clock=clock,
    )
    result = await worker.orders.cancel_session(
        TdCancelSessionRequest(session_id=SESSION_A), timeout=1.0
    )
    book = worker.trading.oms.view().orders
    assert filled not in book
    assert book[resting].status is OrderStatus.NEW
    assert book[other].status is OrderStatus.NEW
    assert result.ok is False
    assert result.unconfirmed == [resting]
    assert other not in venue.cancelled
    assert other not in venue.fetched


@pytest.mark.component
async def test_an_in_flight_submit_that_lands_is_cancelled() -> None:
    """A submit inside place_order is waited for, then cancelled with the rest."""
    clock = FakeClock()
    cid = _cid(SESSION_A, 1)
    release = asyncio.Event()
    started = asyncio.Event()

    class _SlowPlace(_Venue):
        async def place_order(self, request: PlaceOrderRequest) -> Order:
            started.set()
            await release.wait()
            return _resting(request.client_order_id or "", OrderStatus.NEW)

    venue = _SlowPlace()
    worker, _session = _with_session(venue, _oms(), clock=clock)
    worker.trading._active = True  # noqa: SLF001 — activate() would start the sweeper
    submit = asyncio.create_task(
        worker.orders.submit(
            OrderSubmit(
                session_id=SESSION_A,
                api_id=API,
                universal_ticker=TICKER,
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=Decimal("1"),
                client_order_id=cid,
            )
        )
    )
    await started.wait()
    cancel = asyncio.create_task(
        worker.orders.cancel_session(
            TdCancelSessionRequest(session_id=SESSION_A), timeout=5.0
        )
    )
    await asyncio.sleep(0)
    assert venue.cancelled == []
    release.set()
    result = await cancel
    ack = await submit
    assert ack.accepted is True
    assert result.ok is True
    assert result.unconfirmed == []
    assert venue.cancelled == [cid]
    assert cid not in worker.trading.oms.view().orders


@pytest.mark.component
async def test_an_in_flight_submit_still_out_at_the_timeout_is_listed() -> None:
    clock = FakeClock()
    cid = _cid(SESSION_A, 1)
    started = asyncio.Event()

    class _NeverPlaces(_Venue):
        async def place_order(self, request: PlaceOrderRequest) -> Order:
            started.set()
            await asyncio.Event().wait()
            return _resting(request.client_order_id or "", OrderStatus.NEW)

    venue = _NeverPlaces()
    worker, _session = _with_session(venue, _oms(), clock=clock)
    worker.trading._active = True  # noqa: SLF001 — activate() would start the sweeper
    submit = asyncio.create_task(
        worker.orders.submit(
            OrderSubmit(
                session_id=SESSION_A,
                api_id=API,
                universal_ticker=TICKER,
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=Decimal("1"),
                client_order_id=cid,
            )
        )
    )
    await started.wait()
    cancel = asyncio.create_task(
        worker.orders.cancel_session(
            TdCancelSessionRequest(session_id=SESSION_A), timeout=1.0
        )
    )
    await _arm(clock, cancel)
    clock.advance(1)
    await asyncio.sleep(0)
    result = await cancel
    assert result.ok is False
    assert result.unconfirmed == [cid]
    assert venue.cancelled == []
    submit.cancel()
    with pytest.raises(asyncio.CancelledError):
        await submit


@pytest.mark.component
async def test_paper_recon_confirms_what_the_venue_does_not_list() -> None:
    """No per-cid lookup: one reconcile, then cancel whatever is still open.

    An in-scope cid missing from the open orders is not resting, and its
    pre-lock drops. Another session's order is put back by that recon
    and not cancelled. The position stays.
    """
    clock = FakeClock()
    unknown = _cid(SESSION_A, 1)
    pending = _cid(SESSION_A, 2)
    resting = _cid(SESSION_A, 3)
    other = _cid(SESSION_B, 1)
    venue = _Paper()
    venue.open_orders = [
        _resting(resting, OrderStatus.NEW),
        _resting(other, OrderStatus.NEW),
    ]
    worker, session = _with_session(
        venue,
        _oms(
            _resting(unknown, OrderStatus.UNKNOWN),
            _resting(pending, OrderStatus.PENDING_NEW),
            _resting(resting, OrderStatus.NEW),
            _resting(other, OrderStatus.NEW),
            position=True,
        ),
        clock=clock,
    )
    session.ledger.apply_venue(
        Balance(asset="USDT", free=Decimal("100"), locked=Decimal("0"))
    )
    session.ledger.reserve(pending, "USDT", Decimal("1"))
    session.ledger.reserve(unknown, "USDT", Decimal("1"))
    result = await worker.orders.cancel_session(
        TdCancelSessionRequest(session_id=SESSION_A), timeout=1.0
    )
    view = worker.trading.oms.view()
    book = view.orders
    assert result.ok is True
    assert result.unconfirmed == []
    assert venue.cancelled == [resting]
    assert unknown not in book
    assert pending not in book
    assert book[other].status is OrderStatus.NEW
    assert "Paper_Perp_BTCUSDT" in view.positions
    assert session.ledger.has_reservation(pending) is False
    assert session.ledger.has_reservation(unknown) is False


@pytest.mark.component
async def test_a_cid_that_does_not_decode_is_not_cancelled() -> None:
    clock = FakeClock()
    resting = _cid(SESSION_A, 1)
    venue = _Venue()
    worker, _session = _with_session(
        venue,
        _oms(
            _resting("not-a-cid", OrderStatus.NEW),
            _resting(resting, OrderStatus.NEW),
        ),
        clock=clock,
    )
    result = await worker.orders.cancel_session(
        TdCancelSessionRequest(session_id=SESSION_A), timeout=1.0
    )
    book = worker.trading.oms.view().orders
    assert result.ok is True
    assert result.unconfirmed == []
    assert venue.cancelled == [resting]
    assert book["not-a-cid"].status is OrderStatus.NEW


@pytest.mark.component
async def test_two_sessions_cancel_concurrently() -> None:
    clock = FakeClock()
    first = _cid(SESSION_A, 1)
    second = _cid(SESSION_B, 1)
    venue = _Venue()
    _entered, release = venue.block_cancels()
    # Two sessions. Clearing the single-entry event lets both reach the venue.
    venue._entered = None  # noqa: SLF001 — both have to get into the venue
    worker, _session = _with_session(
        venue,
        _oms(
            _resting(first, OrderStatus.NEW),
            _resting(second, OrderStatus.NEW),
        ),
        clock=clock,
    )
    one = asyncio.create_task(
        worker.orders.cancel_session(
            TdCancelSessionRequest(session_id=SESSION_A), timeout=5.0
        )
    )
    two = asyncio.create_task(
        worker.orders.cancel_session(
            TdCancelSessionRequest(session_id=SESSION_B), timeout=5.0
        )
    )
    for _ in range(20):
        await asyncio.sleep(0)
        if venue.peak == 2:
            break
    assert venue.peak == 2
    release.set()
    assert (await one).ok is True
    assert (await two).ok is True
    assert sorted(venue.cancelled) == sorted([first, second])


@pytest.mark.component
async def test_a_second_cancel_session_for_the_same_session_does_not_interleave() -> (
    None
):
    clock = FakeClock()
    cid = _cid(SESSION_A, 1)
    venue = _Venue()
    entered, release = venue.block_cancels()
    worker, _session = _with_session(
        venue, _oms(_resting(cid, OrderStatus.NEW)), clock=clock
    )
    first = asyncio.create_task(
        worker.orders.cancel_session(
            TdCancelSessionRequest(session_id=SESSION_A), timeout=5.0
        )
    )
    await entered.wait()
    second = asyncio.create_task(
        worker.orders.cancel_session(
            TdCancelSessionRequest(session_id=SESSION_A), timeout=5.0
        )
    )
    await asyncio.sleep(0)
    assert venue.cancelled == [cid]
    assert venue.peak == 1
    release.set()
    assert (await first).ok is True
    assert (await second).ok is True
    assert venue.peak == 1
    assert venue.cancelled == [cid]


@pytest.mark.component
async def test_cancel_session_dispatch_replies_with_the_result() -> None:
    clock = FakeClock()
    worker, _session = _with_session(_Venue(), _oms(), clock=clock)
    reply = await worker.orders(
        UntypedEnvelope.model_validate_json(
            Envelope[TdCancelSessionRequest]
            .wrap(
                TdCancelSessionRequest(session_id=SESSION_A),
                type=TD_ORDER_CANCEL_SESSION,
                source="test",
                session_id=SESSION_A,
            )
            .to_json()
        )
    )
    assert reply is not None
    assert reply.type == TD_ORDER_CANCEL_SESSION
    result = TdCancelSessionResult.model_validate(reply.payload)
    assert result.ok is True
    assert result.session_id == SESSION_A
    assert result.unconfirmed == []


@pytest.mark.component
@pytest.mark.parametrize("venue_type", [_Paper, _Venue], ids=["paper", "venue"])
async def test_a_live_order_the_book_dropped_is_still_cancelled(
    venue_type: type,
) -> None:
    """A recon that drops PENDING_NEW does not make a later cancel a no-op.

    ``place_order`` then returns NEW and ``accept_venue_order`` ignores
    the ack. The order is resting at the venue and absent from the book.
    ``cancel_session`` has to cancel it. ``ok=True`` with nothing sent
    is the bug.
    """
    clock = FakeClock()
    venue = venue_type()
    worker, session = _with_session(venue, _oms(), clock=clock)
    cid = _cid(SESSION_A, 1)
    await session.record_pending_new(
        _resting(cid, OrderStatus.PENDING_NEW), session_id=SESSION_A
    )
    await session.reconcile()
    live = _resting(cid, OrderStatus.NEW)
    venue.open_orders = [live]
    await session.accept_venue_order(live)
    assert cid not in session.oms.view().orders
    result = await worker.orders.cancel_session(
        TdCancelSessionRequest(session_id=SESSION_A), timeout=5
    )
    assert result.ok is True
    assert result.unconfirmed == []
    assert venue.cancelled == [cid]
    assert cid not in session.oms.view().orders


@pytest.mark.component
async def test_another_sessions_paper_recon_does_not_hide_a_live_order() -> None:
    """``cancel_session`` for B reconciles and drops A's in-flight order.

    A's submit then lands as NEW and the ack is ignored. A's own
    ``cancel_session`` still has to cancel that resting order.
    """
    clock = FakeClock()
    cid_a = _cid(SESSION_A, 1)
    cid_b = _cid(SESSION_B, 1)
    release = asyncio.Event()
    started = asyncio.Event()

    class _SlowPaper(_Paper):
        async def place_order(self, request: PlaceOrderRequest) -> Order:
            started.set()
            await release.wait()
            order = _resting(request.client_order_id or "", OrderStatus.NEW)
            self.open_orders.append(order)
            return order

    venue = _SlowPaper()
    worker, _session = _with_session(
        venue, _oms(_resting(cid_b, OrderStatus.PENDING_NEW)), clock=clock
    )
    worker.trading._active = True  # noqa: SLF001 — activate() would start the sweeper
    submit = asyncio.create_task(
        worker.orders.submit(
            OrderSubmit(
                session_id=SESSION_A,
                api_id=API,
                universal_ticker=TICKER,
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=Decimal("1"),
                client_order_id=cid_a,
            )
        )
    )
    await started.wait()
    assert cid_a in worker.trading.oms.view().orders
    result_b = await worker.orders.cancel_session(
        TdCancelSessionRequest(session_id=SESSION_B), timeout=5
    )
    assert result_b.ok is True
    assert cid_a not in worker.trading.oms.view().orders
    assert cid_a not in venue.cancelled
    release.set()
    ack = await submit
    assert ack.accepted is True
    assert cid_a not in worker.trading.oms.view().orders
    result_a = await worker.orders.cancel_session(
        TdCancelSessionRequest(session_id=SESSION_A), timeout=5
    )
    assert result_a.ok is True
    assert result_a.unconfirmed == []
    assert cid_a in venue.cancelled
    assert cid_a not in worker.trading.oms.view().orders


class _LateIndex(_Venue):
    """The ack landed. The by-cid lookup does not see the order yet."""

    def __init__(self, *, cancel_error: BaseException | None = None) -> None:
        super().__init__()
        self.cancelled_ids: list[str] = []
        self.cancel_by_id_error = cancel_error

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, ticker: object = None
    ) -> None:
        del ticker
        self.fetched.append(client_order_id)
        return None

    async def cancel_order(self, order_id: str) -> Order:
        self.cancelled_ids.append(order_id)
        if self.cancel_by_id_error is not None:
            raise self.cancel_by_id_error
        return _resting(order_id.removeprefix("v-"), OrderStatus.CANCELED)


@pytest.mark.component
async def test_an_acked_pending_new_is_not_rejected_when_the_lookup_misses() -> None:
    """A venue id on ``PENDING_NEW`` means the venue already accepted it.

    OKX, Bybit, Bitget, and sometimes Deribit return that from
    ``place_order``. The by-cid lookup can still miss. Marking the
    order ``REJECTED`` would make ``cancel_session`` answer ``ok``
    while the order rests. Cancel by the venue id instead. If that
    cancel does not confirm, the cid is ``unconfirmed`` and the book
    stays ``PENDING_NEW``.
    """
    clock = FakeClock()
    cid = _cid(SESSION_A, 1)
    order = _resting(cid, OrderStatus.PENDING_NEW)
    venue = _LateIndex()
    worker, _session = _with_session(venue, _oms(order), clock=clock)
    result = await worker.orders.cancel_session(
        TdCancelSessionRequest(session_id=SESSION_A), timeout=1.0
    )
    assert result.ok is True
    assert result.unconfirmed == []
    assert venue.cancelled_ids == [order.order_id]
    assert venue.cancelled == []
    assert cid not in worker.trading.oms.view().orders

    missed = _LateIndex(cancel_error=OrderError("not indexed"))
    worker, _session = _with_session(
        missed, _oms(_resting(cid, OrderStatus.PENDING_NEW)), clock=clock
    )
    result = await worker.orders.cancel_session(
        TdCancelSessionRequest(session_id=SESSION_A), timeout=1.0
    )
    booked = worker.trading.oms.view().orders[cid]
    assert booked.status is OrderStatus.PENDING_NEW
    assert result.ok is False
    assert result.unconfirmed == [cid]
    assert missed.cancelled_ids == [order.order_id]
