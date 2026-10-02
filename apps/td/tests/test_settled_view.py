"""``oms.view(settled=True)``: wait, timeout, and a closed trading layer.

The two contract tests in ``test_account_contract`` are the chase and the
clean book. These are the rest of B6-08: a timeout keeps the UNKNOWN
order, a layer that is down is an error rather than an empty book, and
the wait does not run on the account subject's caller.
"""

from __future__ import annotations

import asyncio
import contextlib
from decimal import Decimal

import pytest
from mftik.broker.handler import Detached
from mftik.exchange.models import Order, OrderStatus, OrderType, Side
from mftik.exchange.oms import OmsView
from mftik.protocol import (
    TD_ACCOUNT_TRADING,
    TD_ERROR,
    TD_LEDGER_VIEW,
    TD_OMS_VIEW,
    Envelope,
    RpcError,
    TdAccountTrading,
    TdLedgerViewRequest,
    TdOmsViewRequest,
)
from mftik.strategy.client_order_id import format_client_order_id
from mftik_td.account import AccountWorker
from mftik_td.account.handlers import (
    TradingClosed,
    account_subject_handler,
)
from mftik_td.account.session import Session
from mftik_td.oms import Oms
from mftik_td.session.settled import view_when_settled

API = 7
SESSION_A = "abc123"
TICKER = "Paper_Spot_BTCUSDT"
CID_RESTING = format_client_order_id(SESSION_A, 1, 1)
CID_UNKNOWN = format_client_order_id(SESSION_A, 1, 2)


def _order(client_order_id: str, status: OrderStatus) -> Order:
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


def _book(*orders: Order) -> Oms:
    oms = Oms()
    oms.apply_reconcile(orders=orders, balances=[], positions=None)
    return oms


class _QuietBroker:
    async def publish(self, subject: str, envelope: object) -> None:
        return None


class _FetchAdapter:
    """``resolve_unknown`` passes ``ticker=``. The fakes record the cid."""

    name = "Paper"

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def __getattr__(self, item: str) -> object:
        return getattr(self._inner, item)

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, ticker: object = None
    ) -> Order:
        fetch = self._inner.fetch_order_by_client_order_id  # type: ignore[attr-defined]
        return await fetch(client_order_id)


class _Venue:
    """Records venue calls. A held fetch waits on ``gate`` instead of sleeping."""

    def __init__(self, gate: asyncio.Event | None = None) -> None:
        self.gate = gate if gate is not None else asyncio.Event()
        self.fetched: list[str] = []
        self.open_orders = 0
        self.balances = 0
        self.entered = asyncio.Event()

    async def fetch_order_by_client_order_id(self, client_order_id: str) -> Order:
        self.fetched.append(client_order_id)
        self.entered.set()
        await self.gate.wait()
        return _order(client_order_id, OrderStatus.CANCELED)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        self.open_orders += 1
        return []

    async def fetch_balances(self) -> list[object]:
        self.balances += 1
        return []


def _worker(oms: Oms, private: object) -> AccountWorker:
    session = Session(
        api_id=API,
        broker=_QuietBroker(),  # type: ignore[arg-type]
        private=_FetchAdapter(private),  # type: ignore[arg-type]
        oms=oms,
    )
    return AccountWorker(API, venue="Paper", session=session)


def _live(oms: Oms, private: object) -> AccountWorker:
    """Started and active, without ``Session.start`` (that path sleeps)."""
    worker = _worker(oms, private)
    session = worker.trading.session
    assert session is not None
    session._started = True
    worker.trading._active = True
    return worker


def _view(*, settled: bool) -> Envelope[TdOmsViewRequest]:
    return Envelope[TdOmsViewRequest].wrap(
        TdOmsViewRequest(api_id=API, settled=settled),
        type=TD_OMS_VIEW,
        source="test",
    )


async def _cancel_chase(worker: AccountWorker) -> None:
    session = worker.trading.session
    if session is None:
        return
    task = session._resolve_all_task
    if task is not None and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def _unknown_book() -> Oms:
    return _book(
        _order(CID_RESTING, OrderStatus.NEW),
        _order(CID_UNKNOWN, OrderStatus.UNKNOWN),
    )


async def test_a_timeout_returns_the_book_with_unknown_still_in_it() -> None:
    """Late and honest: the wait ending does not drop or invent the order."""
    venue = _Venue()
    worker = _live(_unknown_book(), venue)
    try:
        view = await worker.oms.view(
            TdOmsViewRequest(api_id=API, settled=True), timeout=0
        )
    finally:
        await _cancel_chase(worker)

    assert CID_UNKNOWN in view.orders
    assert view.orders[CID_UNKNOWN].status is OrderStatus.UNKNOWN
    assert CID_RESTING in view.orders
    assert venue.open_orders == 0
    assert venue.balances == 0
    session = worker.trading.session
    assert session is not None
    assert session._on_book_settled is None


async def test_an_unsettled_read_does_not_touch_the_venue() -> None:
    """V1. ``settled=False`` is the memory book, UNKNOWN included."""
    venue = _Venue()
    worker = _live(_unknown_book(), venue)

    view = await worker.oms.view(TdOmsViewRequest(api_id=API, settled=False))

    assert view.orders[CID_UNKNOWN].status is OrderStatus.UNKNOWN
    assert venue.fetched == []
    assert venue.open_orders == 0
    reply = await worker.oms(_view(settled=False))
    assert not isinstance(reply, Detached)
    assert reply is not None
    assert reply.type == TD_OMS_VIEW


async def test_a_clean_book_that_was_not_started_is_still_not_a_venue_pass() -> None:
    """The helper answers a book with nothing to chase. It does not recon."""
    venue = _Venue()
    worker = _worker(_book(_order(CID_RESTING, OrderStatus.NEW)), venue)
    session = worker.trading.session
    assert session is not None
    assert session.started is False

    view = await view_when_settled(session, timeout=0)

    assert CID_RESTING in view.orders
    assert venue.fetched == []
    assert venue.open_orders == 0
    assert session._on_book_settled is None


async def test_a_settled_read_while_the_layer_is_down_is_an_error() -> None:
    """An empty snapshot would say the account holds nothing. It is closed."""
    worker = _worker(_unknown_book(), _Venue())

    with pytest.raises(TradingClosed, match="venue is not connected"):
        await worker.oms.view(TdOmsViewRequest(api_id=API, settled=True))

    reply = await worker.oms(_view(settled=True))
    assert not isinstance(reply, Detached)
    assert reply is not None
    assert reply.type == TD_ERROR
    error = RpcError.model_validate(reply.payload)
    assert error.code == "107"
    assert error.message == "venue is not connected"
    # The payload parses as an empty book. The type is what stops that.
    assert OmsView.model_validate(error.model_dump()).orders == {}


async def test_a_missing_session_is_the_same_refusal() -> None:
    worker = AccountWorker(API, venue="Paper")

    reply = await worker.oms(_view(settled=True))

    assert reply is not None
    assert not isinstance(reply, Detached)
    assert reply.type == TD_ERROR
    assert RpcError.model_validate(reply.payload).code == "107"


async def test_a_destroyed_session_is_the_same_refusal() -> None:
    worker = _live(_book(_order(CID_RESTING, OrderStatus.NEW)), _Venue())
    session = worker.trading.session
    assert session is not None
    session._destroyed = True

    with pytest.raises(TradingClosed, match="venue is not connected"):
        await worker.oms.view(TdOmsViewRequest(api_id=API, settled=True))
    reply = await worker.oms(_view(settled=True))
    assert reply is not None
    assert reply.type == TD_ERROR
    assert RpcError.model_validate(reply.payload).code == "107"


async def test_closing_the_layer_during_the_wait_does_not_return_the_book() -> None:
    """Deactivate can land while the read is parked. That is not a snapshot."""
    venue = _Venue()
    worker = _live(_unknown_book(), venue)

    async def read() -> OmsView:
        return await worker.oms.view(
            TdOmsViewRequest(api_id=API, settled=True), timeout=1.0
        )

    task = asyncio.create_task(read())
    try:
        await venue.entered.wait()
        worker.trading._active = False
        venue.gate.set()
        with pytest.raises(TradingClosed, match="venue is not connected"):
            await task
    finally:
        venue.gate.set()
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await _cancel_chase(worker)


async def test_two_settled_reads_share_one_chase() -> None:
    venue = _Venue()
    worker = _live(_unknown_book(), venue)

    async def read() -> OmsView:
        return await worker.oms.view(
            TdOmsViewRequest(api_id=API, settled=True), timeout=1.0
        )

    first = asyncio.create_task(read())
    second = asyncio.create_task(read())
    try:
        await venue.entered.wait()
        await asyncio.sleep(0)
        assert venue.fetched == [CID_UNKNOWN]
        assert venue.open_orders == 0
        venue.gate.set()
        left, right = await asyncio.gather(first, second)
    finally:
        venue.gate.set()
        for task in (first, second):
            if not task.done():
                task.cancel()
        await _cancel_chase(worker)

    for view in (left, right):
        assert all(
            order.status is not OrderStatus.UNKNOWN for order in view.orders.values()
        )
        assert CID_RESTING in view.orders
    assert venue.fetched == [CID_UNKNOWN]
    session = worker.trading.session
    assert session is not None
    assert session._on_book_settled is None


async def test_the_settled_reply_is_detached_so_the_subject_can_keep_reading() -> None:
    """The caller of ``OmsHandler`` gets the wait back as a task, not a block.

    ``td.account.{api_id}`` also serves the trading bit, the ledger and the
    unsettled book. Those three answer while this read is still parked.
    """
    venue = _Venue()
    worker = _live(_unknown_book(), venue)
    outcome = await worker.oms(_view(settled=True))
    assert isinstance(outcome, Detached)
    work = asyncio.create_task(outcome.work)
    try:
        await venue.entered.wait()
        assert not work.done()
        handle = account_subject_handler(worker)
        ledger = await handle(
            Envelope[TdLedgerViewRequest].wrap(
                TdLedgerViewRequest(api_id=API),
                type=TD_LEDGER_VIEW,
                source="test",
            )
        )
        unsettled = await handle(_view(settled=False))
        trading = await handle(
            Envelope[TdAccountTrading].wrap(
                TdAccountTrading(api_id=API, active=True),
                type=TD_ACCOUNT_TRADING,
                source="test",
            )
        )
        assert not work.done()
        assert ledger is not None and not isinstance(ledger, Detached)
        assert ledger.type == TD_LEDGER_VIEW
        assert unsettled is not None and not isinstance(unsettled, Detached)
        assert unsettled.type == TD_OMS_VIEW
        book = OmsView.model_validate(unsettled.payload)
        assert book.orders[CID_UNKNOWN].status is OrderStatus.UNKNOWN
        assert trading is not None and not isinstance(trading, Detached)
        assert trading.type == TD_ACCOUNT_TRADING
        assert TdAccountTrading.model_validate(trading.payload).active is True
        assert venue.open_orders == 0
        venue.gate.set()
        settled = await work
    finally:
        venue.gate.set()
        if not work.done():
            work.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await work
        await _cancel_chase(worker)

    assert settled is not None
    assert settled.type == TD_OMS_VIEW
    done = OmsView.model_validate(settled.payload)
    assert all(
        order.status is not OrderStatus.UNKNOWN for order in done.orders.values()
    )
    assert CID_RESTING in done.orders
