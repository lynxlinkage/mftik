from __future__ import annotations

import asyncio
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange, Side
from mft.exchange.models import OrderStatus, OrderType
from mft.protocol import (
    TD_BALANCE_UPDATE,
    TD_CANCEL_REJECT,
    TD_FILL,
    TD_ORDER_REJECT,
    TD_ORDER_UPDATE,
    ReconDone,
    StsCreateSessionRequest,
    TdAttachRequest,
    UntypedEnvelope,
)
from mft_sts.client_order_id import unpack
from mft_sts.impl import register
from mft_sts.session import SessionManager as StsSessionManager
from mft_sts.strategy import Strategy
from mft_td.session import PaperSessionFactory
from mft_td.session import SessionManager as TdSessionManager


class PrivateEventsStrategy(Strategy):
    name = "private_events"
    id = 42

    def __init__(self) -> None:
        super().__init__()
        self.recon_done = asyncio.Event()
        self.events: list[tuple[str, int, str]] = []
        self.order_updates: asyncio.Queue[UntypedEnvelope] = asyncio.Queue()
        self.fills: asyncio.Queue[UntypedEnvelope] = asyncio.Queue()
        self.order_rejects: asyncio.Queue[UntypedEnvelope] = asyncio.Queue()
        self.cancel_rejects: asyncio.Queue[UntypedEnvelope] = asyncio.Queue()
        self.balances: asyncio.Queue[UntypedEnvelope] = asyncio.Queue()

    async def on_recon_done(self, msg: ReconDone) -> None:
        self.recon_done.set()

    async def on_order_update(self, api_id: int, msg: UntypedEnvelope) -> None:
        self.events.append(("order", api_id, msg.type))
        await self.order_updates.put(msg)

    async def on_fill(self, api_id: int, msg: UntypedEnvelope) -> None:
        self.events.append(("fill", api_id, msg.type))
        await self.fills.put(msg)

    async def on_order_reject(self, api_id: int, msg: UntypedEnvelope) -> None:
        self.events.append(("order_reject", api_id, msg.type))
        await self.order_rejects.put(msg)

    async def on_cancel_reject(self, api_id: int, msg: UntypedEnvelope) -> None:
        self.events.append(("cancel_reject", api_id, msg.type))
        await self.cancel_rejects.put(msg)

    async def on_balance_update(self, api_id: int, msg: UntypedEnvelope) -> None:
        self.events.append(("balance", api_id, msg.type))
        await self.balances.put(msg)


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
    await maker.place_limit_order(
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=Decimal("10"),
        price=Decimal("49999"),
    )
    await maker.place_limit_order(
        symbol="BTCUSDT",
        side=Side.SELL,
        qty=Decimal("10"),
        price=Decimal("50001"),
    )
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
            td=[7],
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

    cid = await strat.oms.submit_order(
        7,
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=Decimal("0.01"),
        type=OrderType.MARKET,
    )
    slot, _ts, seq = unpack(cid)
    assert slot == strat.cid_slot
    assert seq == 1
    assert strat.owns(cid)

    order_env = await asyncio.wait_for(strat.order_updates.get(), timeout=3.0)
    fill_env = await asyncio.wait_for(strat.fills.get(), timeout=3.0)
    bal_env = await asyncio.wait_for(strat.balances.get(), timeout=3.0)

    assert order_env.type == TD_ORDER_UPDATE
    assert order_env.payload["client_order_id"] == cid
    assert order_env.payload["status"] == OrderStatus.FILLED.value

    assert fill_env.type == TD_FILL
    assert fill_env.payload["client_order_id"] == cid

    assert bal_env.type == TD_BALANCE_UPDATE
    assert "asset" in bal_env.payload

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

    # Insufficient balance → on_order_reject
    bad_cid = await strat.oms.submit_order(
        7,
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=Decimal("1000"),
        type=OrderType.MARKET,
    )
    reject_env = await asyncio.wait_for(strat.order_rejects.get(), timeout=3.0)
    assert reject_env.type == TD_ORDER_REJECT
    assert reject_env.payload["client_order_id"] == bad_cid
    assert reject_env.payload["api_id"] == 7
    assert unpack(bad_cid)[2] == 1

    # Cancel unknown cid → on_cancel_reject
    await strat.oms.cancel_order(7, bad_cid)
    cancel_rej = await asyncio.wait_for(strat.cancel_rejects.get(), timeout=3.0)
    assert cancel_rej.type == TD_CANCEL_REJECT
    assert cancel_rej.payload["client_order_id"] == bad_cid

    # Resting limit + cancel by client_order_id → on_order_update
    cid = await strat.oms.submit_order(
        7,
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=Decimal("0.01"),
        type=OrderType.LIMIT,
        price=Decimal("1000"),
    )
    assert unpack(cid)[2] == 2
    open_env = await asyncio.wait_for(strat.order_updates.get(), timeout=3.0)
    assert open_env.payload["client_order_id"] == cid
    assert open_env.payload["status"] == OrderStatus.OPEN.value

    await strat.oms.cancel_order(7, cid)
    cancel_env = await asyncio.wait_for(strat.order_updates.get(), timeout=3.0)
    assert cancel_env.payload["client_order_id"] == cid
    assert cancel_env.payload["status"] == OrderStatus.CANCELED.value

    await td.close_all()
    await sts.close_all()
    await paper.stop()
