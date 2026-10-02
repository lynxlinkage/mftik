"""``serve`` on one subject: sequential unless the handler opts out.

B6-08. ``td.account.{api_id}`` is one loop. A settled OMS read waits
up to 30 seconds, and that wait must not hold the trading bit, the
ledger, or an unsettled read. :class:`~mftik.broker.handler.Detached`
is the opt-in. A handler that returns a reply does not take it.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from decimal import Decimal

import pytest
from broker_harness import session_loop, subjects_under
from mftik.broker import Broker
from mftik.broker.handler import (
    SETTLED_MAX_CONCURRENT,
    Detached,
    Reply,
    serve,
)
from mftik.exchange.models import Order, OrderStatus, OrderType, Side
from mftik.exchange.oms import LedgerView, OmsView
from mftik.protocol import (
    TD_ACCOUNT_TRADING,
    TD_LEDGER_VIEW,
    TD_OMS_VIEW,
    Envelope,
    TdAccountTrading,
    TdLedgerViewRequest,
    TdOmsViewRequest,
    Topics,
    UntypedEnvelope,
)
from mftik.strategy.client_order_id import format_client_order_id
from mftik_td.account import AccountWorker
from mftik_td.account.handlers import account_subject_handler
from mftik_td.account.session import Session
from mftik_td.oms import Oms

API = 226
SESSION_A = "abc123"
TICKER = "Paper_Spot_BTCUSDT"
CID_RESTING = format_client_order_id(SESSION_A, 1, 1)
CID_UNKNOWN = format_client_order_id(SESSION_A, 1, 2)
SUBJECT = "demo.settled"


def test_detached_is_off_unless_a_handler_returns_one() -> None:
    """The cap is the settled read's. Nothing else spends it."""
    assert SETTLED_MAX_CONCURRENT == 8
    default = inspect.signature(serve).parameters["detached_limit"].default
    assert default == SETTLED_MAX_CONCURRENT


async def test_a_bad_detached_cap_is_refused_before_the_loop() -> None:
    async def handle(message: UntypedEnvelope) -> Reply | None:
        return None

    with pytest.raises(ValueError):
        await serve(None, SUBJECT, handle, detached_limit=0)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        await serve(None, SUBJECT, handle, detached_limit=True)  # type: ignore[arg-type]


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


class _QuietBroker:
    async def publish(self, subject: str, envelope: object) -> None:
        return None


class _FetchAdapter:
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


class _Hold:
    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.entered = asyncio.Event()
        self.fetched: list[str] = []
        self.open_orders = 0

    async def fetch_order_by_client_order_id(self, client_order_id: str) -> Order:
        self.fetched.append(client_order_id)
        self.entered.set()
        await self.gate.wait()
        return _order(client_order_id, OrderStatus.CANCELED)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        self.open_orders += 1
        return []

    async def fetch_balances(self) -> list[object]:
        return []


def _live(hold: _Hold) -> AccountWorker:
    oms = Oms()
    oms.apply_reconcile(
        orders=[
            _order(CID_RESTING, OrderStatus.NEW),
            _order(CID_UNKNOWN, OrderStatus.UNKNOWN),
        ],
        balances=[],
        positions=None,
    )
    session = Session(
        api_id=API,
        broker=_QuietBroker(),  # type: ignore[arg-type]
        private=_FetchAdapter(hold),  # type: ignore[arg-type]
        oms=oms,
    )
    session._started = True
    worker = AccountWorker(API, venue="Paper", session=session)
    worker.trading._active = True
    return worker


def _request(n: int) -> Envelope[dict[str, int]]:
    return Envelope[dict[str, int]].wrap({"n": n}, type="demo", source="test")


def _answer(n: int) -> Reply:
    return Envelope[dict[str, int]].wrap({"n": n}, type="demo.reply", source="server")


async def _until_subscribed(broker: Broker, subject: str) -> None:
    """The SUB is on the shared connection. A request before that sleeps."""
    connection = broker.transport.nc  # type: ignore[attr-defined]
    full = broker.transport._rpc_subject(subject)  # type: ignore[attr-defined]
    deadline = asyncio.get_running_loop().time() + 1.0
    while asyncio.get_running_loop().time() < deadline:
        if full in subjects_under(connection, broker.config.key_prefix):
            await connection.flush(timeout=1)
            return
        await asyncio.sleep(0)
    raise TimeoutError(full)


async def _stop(task: asyncio.Task[None], stop: asyncio.Event) -> None:
    stop.set()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.component
@session_loop
async def test_serve_stays_sequential_unless_the_handler_returns_detached(
    broker: Broker,
) -> None:
    """H3, the default. The second message is on the wire and still unstarted.

    A reply (not :class:`Detached`) is what every handler returns unless
    it opts in. This one does not.
    """
    stop = asyncio.Event()
    first = asyncio.Event()
    release = asyncio.Event()
    both_on_the_wire = asyncio.Event()
    seen: list[int] = []
    arrived = 0

    async def handle(message: UntypedEnvelope) -> Reply | None:
        n = int(message.payload["n"])
        seen.append(n)
        if n == 1:
            first.set()
            await release.wait()
        return _answer(n)

    connection = broker.transport.nc  # type: ignore[attr-defined]
    full = broker.transport._rpc_subject(SUBJECT)  # type: ignore[attr-defined]

    async def tap(msg: object) -> None:
        nonlocal arrived
        arrived += 1
        if arrived >= 2:
            both_on_the_wire.set()

    monitor = await connection.subscribe(full, cb=tap)
    task = asyncio.create_task(serve(broker, SUBJECT, handle, stop=stop))
    one: asyncio.Task[UntypedEnvelope] | None = None
    two: asyncio.Task[UntypedEnvelope] | None = None
    try:
        await _until_subscribed(broker, SUBJECT)
        await connection.flush(timeout=1)
        one = asyncio.create_task(broker.request(SUBJECT, _request(1), timeout=2))
        await asyncio.wait_for(first.wait(), timeout=1)
        two = asyncio.create_task(broker.request(SUBJECT, _request(2), timeout=2))
        await asyncio.wait_for(both_on_the_wire.wait(), timeout=1)
        assert seen == [1]
        assert not two.done()
        release.set()
        assert (await asyncio.wait_for(one, timeout=1)).payload == {"n": 1}
        assert (await asyncio.wait_for(two, timeout=1)).payload == {"n": 2}
    finally:
        release.set()
        with contextlib.suppress(Exception):
            await monitor.unsubscribe()
        for pending in (one, two):
            if pending is not None and not pending.done():
                pending.cancel()
        await _stop(task, stop)

    assert seen == [1, 2]


@pytest.mark.component
@session_loop
async def test_a_detached_reply_does_not_hold_the_next_message(
    broker: Broker,
) -> None:
    """The opt-in, on its own: the second request is answered while the first waits."""
    stop = asyncio.Event()
    parked = asyncio.Event()
    release = asyncio.Event()

    async def handle(message: UntypedEnvelope) -> Reply | Detached | None:
        n = int(message.payload["n"])
        if n == 1:
            async def _later() -> Reply:
                parked.set()
                await release.wait()
                return _answer(1)

            return Detached(_later())
        return _answer(n)

    task = asyncio.create_task(serve(broker, SUBJECT, handle, stop=stop))
    one: asyncio.Task[UntypedEnvelope] | None = None
    try:
        await _until_subscribed(broker, SUBJECT)
        one = asyncio.create_task(broker.request(SUBJECT, _request(1), timeout=2))
        await asyncio.wait_for(parked.wait(), timeout=1)
        second = await asyncio.wait_for(
            broker.request(SUBJECT, _request(2), timeout=1), timeout=1
        )
        assert second.payload == {"n": 2}
        assert not one.done()
        release.set()
        assert (await asyncio.wait_for(one, timeout=1)).payload == {"n": 1}
    finally:
        release.set()
        if one is not None and not one.done():
            one.cancel()
        await _stop(task, stop)


@pytest.mark.component
@session_loop
async def test_a_settled_read_does_not_hold_the_account_subject(
    broker: Broker,
) -> None:
    """Trading push, ledger, and an unsettled view answer while UNKNOWN is open.

    The settled request is the one still waiting. It is on
    ``td.account.{api_id}``, the same subject as the three that return.
    """
    hold = _Hold()
    worker = _live(hold)
    subject = Topics.td_account(API)
    stop = asyncio.Event()
    task = asyncio.create_task(
        serve(broker, subject, account_subject_handler(worker), stop=stop)
    )
    settled: asyncio.Task[UntypedEnvelope] | None = None
    try:
        await _until_subscribed(broker, subject)
        settled = asyncio.create_task(
            broker.request(
                subject,
                Envelope[TdOmsViewRequest].wrap(
                    TdOmsViewRequest(api_id=API, settled=True),
                    type=TD_OMS_VIEW,
                    source="test",
                ),
                timeout=2,
            )
        )
        await asyncio.wait_for(hold.entered.wait(), timeout=1)
        assert not settled.done()

        ledger = await asyncio.wait_for(
            broker.request(
                subject,
                Envelope[TdLedgerViewRequest].wrap(
                    TdLedgerViewRequest(api_id=API),
                    type=TD_LEDGER_VIEW,
                    source="test",
                ),
                timeout=1,
            ),
            timeout=1,
        )
        unsettled = await asyncio.wait_for(
            broker.request(
                subject,
                Envelope[TdOmsViewRequest].wrap(
                    TdOmsViewRequest(api_id=API, settled=False),
                    type=TD_OMS_VIEW,
                    source="test",
                ),
                timeout=1,
            ),
            timeout=1,
        )
        trading = await asyncio.wait_for(
            broker.request(
                subject,
                Envelope[TdAccountTrading].wrap(
                    TdAccountTrading(api_id=API, active=True),
                    type=TD_ACCOUNT_TRADING,
                    source="test",
                ),
                timeout=1,
            ),
            timeout=1,
        )

        assert not settled.done()
        assert ledger.type == TD_LEDGER_VIEW
        assert LedgerView.model_validate(ledger.payload).api_id == API
        book = OmsView.model_validate(unsettled.payload)
        assert book.orders[CID_UNKNOWN].status is OrderStatus.UNKNOWN
        assert book.orders[CID_RESTING].status is OrderStatus.NEW
        assert TdAccountTrading.model_validate(trading.payload).active is True
        assert hold.open_orders == 0

        hold.gate.set()
        done = OmsView.model_validate(
            (await asyncio.wait_for(settled, timeout=1)).payload
        )
        assert all(
            order.status is not OrderStatus.UNKNOWN
            for order in done.orders.values()
        )
        assert CID_RESTING in done.orders
        assert hold.fetched == [CID_UNKNOWN]
        assert hold.open_orders == 0
    finally:
        hold.gate.set()
        if settled is not None and not settled.done():
            settled.cancel()
        session = worker.trading.session
        chase = None if session is None else session._resolve_all_task
        if chase is not None and not chase.done():
            chase.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await chase
        await _stop(task, stop)
