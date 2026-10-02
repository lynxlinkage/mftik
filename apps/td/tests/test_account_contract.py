"""What the account worker will do, written down before it does it (IF-11).

Each test is ``xfail(strict=True)``. It describes behaviour §7.1 already
settles, and it fails today because the surface returns null data.
``strict`` is the point: the ticket that implements one of these cannot
merge while the marker is still on it.

Nothing here reaches a venue or a broker. The fakes are the connector
:class:`~mftik_td.session.session.Session` already talks to
(``fetch_order_by_client_order_id``, ``cancel_by_client_order_id``) and
the OMS the trading layer holds. B6 drives those; these tests are the
description of what "drove them" has to mean.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from decimal import Decimal

import pytest
from mftik.exchange.models import Order, OrderStatus, OrderType, Side
from mftik.exchange.oms import Position
from mftik.protocol import TdCancelSessionRequest, TdOmsViewRequest
from mftik.strategy.client_order_id import format_client_order_id
from mftik_td.account import AccountWorker, deadman_for
from mftik_td.account.session import Session
from mftik_td.oms import Oms

API = 7
SESSION_A = "abc123"
SESSION_B = "def456"
TICKER = "Paper_Spot_BTCUSDT"
POSITION = "Paper_Perp_BTCUSDT"

CID_RESTING = format_client_order_id(SESSION_A, 1, 1)
CID_UNKNOWN = format_client_order_id(SESSION_A, 1, 2)
CID_PENDING = format_client_order_id(SESSION_A, 1, 3)
CID_OTHER = format_client_order_id(SESSION_B, 1, 1)


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


def _book(*orders: Order, position: bool = False) -> Oms:
    oms = Oms()
    positions = (
        [Position(universal_ticker=POSITION, qty=Decimal("1"))] if position else None
    )
    oms.apply_reconcile(orders=orders, balances=[], positions=positions)
    return oms


class _QuietBroker:
    async def publish(self, subject: str, envelope: object) -> None:
        return None


class _FetchAdapter:
    """The chase path passes ``ticker=``. The fakes below do not take it.

    ``resolve_unknown`` calls ``fetch_order_by_client_order_id(cid,
    ticker=...)``. Wrapping here keeps that call working without
    changing what the fake records.
    """

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


def _worker(oms: Oms, private: object) -> AccountWorker:
    """The running worker always has a session. These fakes did not.

    ``cancel_session`` goes through that session (the same book, the
    same connector). Building one here is the construction change;
    the assertions are the contract.
    """
    session = Session(
        api_id=API,
        broker=_QuietBroker(),  # type: ignore[arg-type]
        private=_FetchAdapter(private),  # type: ignore[arg-type]
        oms=oms,
    )
    return AccountWorker(API, venue="Paper", session=session)


class _Probe:
    def __init__(self) -> None:
        self.pings = 0

    async def __call__(self) -> None:
        self.pings += 1


class _ResolvingVenue:
    """Chase converges at once. Cancel confirms at once. No network."""

    def __init__(self) -> None:
        self.fetched: list[str] = []
        self.cancelled: list[str] = []

    async def fetch_order_by_client_order_id(self, client_order_id: str) -> Order:
        self.fetched.append(client_order_id)
        if client_order_id == CID_UNKNOWN:
            # Never reached the venue. Confirmed terminal, not a cancel.
            return _order(client_order_id, OrderStatus.CANCELED)
        if client_order_id == CID_PENDING:
            return _order(client_order_id, OrderStatus.NEW)
        return _order(client_order_id, OrderStatus.NEW)

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        self.cancelled.append(client_order_id)
        return _order(client_order_id, OrderStatus.CANCELED)


class _StuckUnknown:
    """The UNKNOWN lookup does not return inside the caller's timeout."""

    def __init__(self) -> None:
        self.cancelled: list[str] = []

    async def fetch_order_by_client_order_id(self, client_order_id: str) -> Order:
        if client_order_id == CID_UNKNOWN:
            # Never answers. An event, not a sleep: the unit/component
            # guard forbids asyncio.sleep(x > 0), and the caller's
            # timeout is what has to cancel this.
            await asyncio.Event().wait()
        if client_order_id == CID_PENDING:
            return _order(client_order_id, OrderStatus.NEW)
        return _order(client_order_id, OrderStatus.NEW)

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        self.cancelled.append(client_order_id)
        return _order(client_order_id, OrderStatus.CANCELED)


class _ChaseWhenReleased:
    def __init__(self, gate: asyncio.Event) -> None:
        self._gate = gate

    async def fetch_order_by_client_order_id(self, client_order_id: str) -> Order:
        await self._gate.wait()
        return _order(client_order_id, OrderStatus.CANCELED)


# --- trading layer vs resident layer (F35) ---------------------------------


@pytest.mark.xfail(
    strict=True,
    reason="B6-02 switches the trading layer; B6-01 keeps the resident pool",
)
async def test_trading_layer_toggle_leaves_the_resident_layer_up() -> None:
    """Activate and deactivate do not rebuild the pool or drop keepalive (T1, R2).

    The resident layer was already up, and it is still the same object
    afterwards: same pool, same hook, still open. The trading layer
    itself does change. That is the whole of the switch.
    """
    probe = _Probe()
    worker = AccountWorker(API, venue="Paper", keepalive=probe)
    await worker.resident.start()
    pool = worker.resident.pool
    assert worker.resident.started
    assert pool is not None

    await worker.trading.activate()
    assert worker.trading.active
    await worker.resident.keepalive_once()

    await worker.trading.deactivate()
    assert worker.trading.active is False
    assert worker.resident.started
    assert worker.resident.pool is pool
    assert worker.resident.keepalive is probe
    await worker.resident.keepalive_once()
    assert probe.pings == 2


# --- cancel_session confirmation (F10) -------------------------------------


async def test_cancel_session_confirms_only_this_sessions_orders() -> None:
    """Success is every in-scope order confirmed, and nothing else touched.

    The UNKNOWN order is chased and comes back already terminal, so it
    is confirmed without a second cancel. The PENDING_NEW order is
    chased, found resting, and then cancelled. The other session's
    order is not fetched and not cancelled. The position is still
    there: cancel_session does not flatten (C4).
    """
    venue = _ResolvingVenue()
    worker = _worker(
        _book(
            _order(CID_RESTING, OrderStatus.NEW),
            _order(CID_UNKNOWN, OrderStatus.UNKNOWN),
            _order(CID_PENDING, OrderStatus.PENDING_NEW),
            _order(CID_OTHER, OrderStatus.NEW),
            position=True,
        ),
        venue,
    )

    result = await worker.orders.cancel_session(
        TdCancelSessionRequest(session_id=SESSION_A), timeout=1.0
    )

    assert result.session_id == SESSION_A
    assert result.ok is True
    assert result.unconfirmed == []
    assert CID_UNKNOWN in venue.fetched
    assert CID_PENDING in venue.fetched
    assert CID_RESTING in venue.cancelled
    assert CID_PENDING in venue.cancelled
    assert CID_UNKNOWN not in venue.cancelled
    assert CID_OTHER not in venue.fetched
    assert CID_OTHER not in venue.cancelled
    book = worker.trading.oms.view()
    assert CID_OTHER in book.orders
    assert book.orders[CID_OTHER].status is OrderStatus.NEW
    assert POSITION in book.positions


# The call waits out timeout=0.05, which is the unit call cap.
@pytest.mark.component
async def test_cancel_session_timeout_lists_what_did_not_confirm() -> None:
    """A timeout is not success, and the reply names the orders still open.

    The UNKNOWN lookup does not return. ``ok`` is false. That cid is
    in ``unconfirmed``. The other session's cid is not, because it was
    never in scope (C1, C3).
    """
    venue = _StuckUnknown()
    worker = _worker(
        _book(
            _order(CID_RESTING, OrderStatus.NEW),
            _order(CID_UNKNOWN, OrderStatus.UNKNOWN),
            _order(CID_PENDING, OrderStatus.PENDING_NEW),
            _order(CID_OTHER, OrderStatus.NEW),
        ),
        venue,
    )

    result = await asyncio.wait_for(
        worker.orders.cancel_session(
            TdCancelSessionRequest(session_id=SESSION_A), timeout=0.05
        ),
        timeout=1.0,
    )

    assert result.ok is False
    assert result.session_id == SESSION_A
    assert CID_UNKNOWN in result.unconfirmed
    assert CID_OTHER not in result.unconfirmed
    assert set(result.unconfirmed) <= {CID_RESTING, CID_UNKNOWN, CID_PENDING}
    assert CID_OTHER in worker.trading.oms.view().orders


# --- settled view (F13) ----------------------------------------------------


@pytest.mark.xfail(strict=True, reason="B6-08 waits for UNKNOWN on settled=True")
async def test_settled_view_waits_until_unknown_converges() -> None:
    """``settled=True`` does not answer while an UNKNOWN order is on the book.

    The chase is released after a short pause. The view returns only
    after that, and the answer has no UNKNOWN left. The order that was
    already known is still in the snapshot.
    """
    gate = asyncio.Event()
    released = False

    async def release() -> None:
        nonlocal released
        await asyncio.sleep(0.05)
        released = True
        gate.set()

    worker = AccountWorker(
        API,
        venue="Paper",
        oms=_book(
            _order(CID_RESTING, OrderStatus.NEW),
            _order(CID_UNKNOWN, OrderStatus.UNKNOWN),
        ),
        private=_ChaseWhenReleased(gate),
    )
    task = asyncio.create_task(release())
    try:
        view = await asyncio.wait_for(
            worker.oms.view(TdOmsViewRequest(api_id=API, settled=True), timeout=1.0),
            timeout=1.0,
        )
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert released
    assert all(
        order.status is not OrderStatus.UNKNOWN for order in view.orders.values()
    )
    assert CID_RESTING in view.orders


@pytest.mark.xfail(strict=True, reason="B6-08 answers a clean settled view immediately")
async def test_settled_view_of_a_clean_book_does_not_wait() -> None:
    """No UNKNOWN means no venue pass and no pause (V2)."""
    worker = AccountWorker(
        API,
        venue="Paper",
        oms=_book(_order(CID_RESTING, OrderStatus.NEW)),
        private=_ResolvingVenue(),
    )
    started = time.monotonic()
    view = await worker.oms.view(
        TdOmsViewRequest(api_id=API, settled=True), timeout=1.0
    )
    assert time.monotonic() - started < 0.2
    assert CID_RESTING in view.orders
    assert view.orders[CID_RESTING].status is OrderStatus.NEW


# --- which countdowns exist (F37) ------------------------------------------


@pytest.mark.xfail(strict=True, reason="B6-07 arms only the countdown venues F37 names")
@pytest.mark.parametrize("venue", ["BinanceUM", "BinanceCM", "Bitget", "Okx"])
def test_countdown_venues_are_supported(venue: str) -> None:
    assert deadman_for(venue).supported() is True


@pytest.mark.xfail(
    strict=True,
    reason="B6-07 does not arm Deribit COD, Bybit DCP, spot or paper",
)
@pytest.mark.parametrize("venue", ["Deribit", "Bybit", "Binance", "Paper"])
def test_venues_outside_f37_are_not_countdown_switches(venue: str) -> None:
    """Deribit must not be COD. Bybit must not be DCP. Neither places a call.

    Spot and paper are absent from F37's list, so they are not armed
    either. Gate and GateFutures are not in this test: the plan says
    "Gate" and the registry has two of them.
    """
    assert deadman_for(venue).supported() is False
