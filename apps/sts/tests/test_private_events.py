from __future__ import annotations

import asyncio
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.exchange import PaperExchange, Side
from mftik.exchange.models import (
    Balance,
    Fill,
    Order,
    OrderStatus,
    OrderType,
    limit_order,
)
from mftik.exchange.oms import Position
from mftik.protocol import (
    TD_BALANCE_UPDATE,
    TD_CANCEL_REJECT,
    TD_FILL,
    TD_ORDER_REJECT,
    TD_ORDER_UPDATE,
    TD_POSITION_UPDATE,
    CancelReject,
    OrderReject,
    ReconDone,
    RejectCode,
    StsCreateSessionRequest,
    TdAccountRef,
    TdAttachRequest,
    Topics,
    UntypedEnvelope,
)
from mftik.strategy import Strategy
from mftik.strategy.client_order_id import unpack
from mftik_sts.impl import register
from mftik_sts.session import SessionManager as StsSessionManager
from mftik_td.session import PaperSessionFactory
from mftik_td.session import SessionManager as TdSessionManager


class PrivateEventsStrategy(Strategy):
    name = "private_events"
    id = 42

    def __init__(self) -> None:
        super().__init__()
        self.recon_done = asyncio.Event()
        self.events: list[tuple[str, int, str]] = []
        self.order_updates: asyncio.Queue[Order] = asyncio.Queue()
        self.fills: asyncio.Queue[Fill] = asyncio.Queue()
        self.order_rejects: asyncio.Queue[OrderReject] = asyncio.Queue()
        self.cancel_rejects: asyncio.Queue[CancelReject] = asyncio.Queue()
        self.balances: asyncio.Queue[Balance] = asyncio.Queue()
        self.positions: asyncio.Queue[Position] = asyncio.Queue()

    async def on_recon_done(self, msg: ReconDone) -> None:
        self.recon_done.set()

    async def on_order_update(self, api_id: int, order: Order) -> None:
        self.events.append(("order", api_id, TD_ORDER_UPDATE))
        await self.order_updates.put(order)

    async def on_fill(self, api_id: int, fill: Fill) -> None:
        self.events.append(("fill", api_id, TD_FILL))
        await self.fills.put(fill)

    async def on_order_reject(self, api_id: int, reject: OrderReject) -> None:
        self.events.append(("order_reject", api_id, TD_ORDER_REJECT))
        await self.order_rejects.put(reject)

    async def on_cancel_reject(self, api_id: int, reject: CancelReject) -> None:
        self.events.append(("cancel_reject", api_id, TD_CANCEL_REJECT))
        await self.cancel_rejects.put(reject)

    async def on_balance_update(self, api_id: int, balance: Balance) -> None:
        self.events.append(("balance", api_id, TD_BALANCE_UPDATE))
        await self.balances.put(balance)

    async def on_position_update(self, api_id: int, position: Position) -> None:
        self.events.append(("position", api_id, TD_POSITION_UPDATE))
        await self.positions.put(position)


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


async def _boot(
    broker: Broker,
) -> tuple[
    PrivateEventsStrategy,
    StsSessionManager,
    TdSessionManager,
    PaperExchange,
]:
    register(PrivateEventsStrategy)
    instances: list[PrivateEventsStrategy] = []

    def factory(name: str | None) -> Strategy:
        assert name == "private_events"
        s = PrivateEventsStrategy()
        instances.append(s)
        return s

    paper = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=3
    )
    paper.register_api(
        "maker",
        "maker-sec",
        balances={"BTC": Decimal("10"), "USDT": Decimal("500000")},
    )
    paper.register_api(
        "paper-key-7",
        "sec-7",
        balances={"BTC": Decimal("1"), "USDT": Decimal("100000")},
    )
    maker = paper.private(
        api_key="maker", api_secret="maker-sec", auto_register=False
    )
    await maker.connect()
    await maker.place_order(limit_order(
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        qty=Decimal("10"),
        price=Decimal("49999"),
    ))
    await maker.place_order(limit_order(
        ticker="Paper_Spot_BTCUSDT",
        side=Side.SELL,
        qty=Decimal("10"),
        price=Decimal("50001"),
    ))
    await maker.close()
    await paper.start()
    paper_factory = PaperSessionFactory(broker, paper)
    paper_factory.bind_api(7, "paper-key-7", "sec-7")

    sts = StsSessionManager(
        broker, heartbeat_interval=0.1, strategy_factory=factory
    )
    td = TdSessionManager(paper_factory, broker, lease_grace=2.0)

    await sts.create_session(
        StsCreateSessionRequest(
            session_id="priv-1",
            created_by=1,
            strategy="private_events",
            td={"paper": TdAccountRef(api_id=7)},
        )
    )
    await td.attach(
        TdAttachRequest(
            session_id="priv-1",
            api_id=7,
            timeout=2.0,
            created_by=1,
        )
    )

    strat = instances[0]
    await asyncio.wait_for(strat.recon_done.wait(), timeout=3.0)
    return strat, sts, td, paper


async def _await_status(strat, status: OrderStatus, timeout: float = 3.0):
    """Next order_update carrying ``status``.

    TD now books an order PENDING_NEW before it reaches the venue, so every
    submit raises two updates: the local one, then the venue's.
    """
    async def _scan():
        while True:
            order = await strat.order_updates.get()
            if order.status == status:
                return order

    return await asyncio.wait_for(_scan(), timeout=timeout)


@pytest.mark.asyncio
async def test_private_events_from_td_global(broker: Broker) -> None:
    strat, sts, td, paper = await _boot(broker)

    # Drain any seed balance pushes from session start.
    await asyncio.sleep(0.1)
    while not strat.balances.empty():
        strat.balances.get_nowait()
    while not strat.order_updates.empty():
        strat.order_updates.get_nowait()
    while not strat.fills.empty():
        strat.fills.get_nowait()
    strat.events.clear()

    assert await strat.oms.submit_order(
        7,
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        qty=Decimal("0.01"),
        type=OrderType.MARKET,
    )
    cid = strat.oms.last_client_order_id
    slot, _ts, seq = unpack(cid)
    assert slot == strat.cid_slot
    assert seq == 1
    assert strat.owns(cid)

    # TD books it locally first, then the venue answers.
    pending = await _await_status(strat, OrderStatus.PENDING_NEW)
    assert pending.client_order_id == cid
    order = await _await_status(strat, OrderStatus.FILLED)
    fill = await asyncio.wait_for(strat.fills.get(), timeout=3.0)
    bal = await asyncio.wait_for(strat.balances.get(), timeout=3.0)

    assert order.client_order_id == cid
    assert order.status == OrderStatus.FILLED

    assert fill.client_order_id == cid
    assert bal.asset

    assert ("order", 7, TD_ORDER_UPDATE) in strat.events
    assert ("fill", 7, TD_FILL) in strat.events
    assert ("balance", 7, TD_BALANCE_UPDATE) in strat.events

    await td.close_all()
    await sts.close_all()
    await paper.stop()


@pytest.mark.asyncio
async def test_order_and_cancel_reject_paths(broker: Broker) -> None:
    strat, sts, td, paper = await _boot(broker)
    await asyncio.sleep(0.1)
    while not strat.order_rejects.empty():
        strat.order_rejects.get_nowait()
    while not strat.cancel_rejects.empty():
        strat.cancel_rejects.get_nowait()
    while not strat.order_updates.empty():
        strat.order_updates.get_nowait()

    # Insufficient balance → on_order_reject. TD still acks: the ack means it
    # took the request, the venue's refusal comes back separately.
    assert await strat.oms.submit_order(
        7,
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        qty=Decimal("1000"),
        type=OrderType.MARKET,
    )
    bad_cid = strat.oms.last_client_order_id
    reject = await asyncio.wait_for(strat.order_rejects.get(), timeout=3.0)
    assert reject.client_order_id == bad_cid
    assert reject.api_id == 7
    assert unpack(bad_cid)[2] == 1
    # The venue's refusal arrives normalized, with its own words kept in
    # ``reason``.
    assert reject.error_code == RejectCode.VENUE_INSUFFICIENT_BALANCE
    assert reject.reason

    # Cancel unknown cid → on_cancel_reject
    assert await strat.oms.cancel_order(7, bad_cid)
    cancel_rej = await asyncio.wait_for(strat.cancel_rejects.get(), timeout=3.0)
    assert cancel_rej.client_order_id == bad_cid
    # The venue knows this order — it rejected it a moment ago — so "not open"
    # is the sharper answer than "not found".
    assert cancel_rej.error_code == RejectCode.VENUE_ORDER_ALREADY_CLOSED

    # Resting limit + cancel by client_order_id → on_order_update
    assert await strat.oms.submit_order(
        7,
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        qty=Decimal("0.01"),
        type=OrderType.LIMIT,
        price=Decimal("1000"),
    )
    cid = strat.oms.last_client_order_id
    assert unpack(cid)[2] == 2
    open_order = await _await_status(strat, OrderStatus.NEW)
    assert open_order.client_order_id == cid

    assert await strat.oms.cancel_order(7, cid)
    canceled = await _await_status(strat, OrderStatus.CANCELED)
    assert canceled.client_order_id == cid

    await td.close_all()
    await sts.close_all()
    await paper.stop()


@pytest.mark.asyncio
async def test_a_position_update_reaches_the_strategy(broker: Broker) -> None:
    """The hook a contract venue needs, delivered the same way as the rest.

    Driven by publishing on ``td.{api_id}.global`` directly rather than through
    the paper venue: paper is spot and has no positions, which is exactly why
    the hook exists — a strategy on a contract venue cannot infer its exposure
    from its own fills, because funding and ADL move it too.
    """
    strat, sts, td, paper = await _boot(broker)
    await asyncio.sleep(0.1)

    await broker.publish(
        Topics.td_global(7),
        UntypedEnvelope.wrap(
            Position(
                universal_ticker="Bybit_Perp_BTCUSDT",
                qty=Decimal("-2"),
                entry_price=Decimal("60000"),
            ).model_dump(mode="json"),
            type=TD_POSITION_UPDATE,
            source="td",
            session_id="7",
        ),
    )

    position = await asyncio.wait_for(strat.positions.get(), timeout=3.0)

    assert position.qty == Decimal("-2")
    assert position.entry_price == Decimal("60000")
    # The instrument, not the symbol: on a unified account BTCUSDT names two.
    assert position.universal_ticker == "Bybit_Perp_BTCUSDT"
    assert position.symbol == "BTCUSDT"
    assert ("position", 7, TD_POSITION_UPDATE) in strat.events

    await td.close_all()
    await sts.close_all()
    await paper.stop()
