from __future__ import annotations

import asyncio
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange
from mft.exchange.models import Side
from mft.protocol import (
    ReconDone,
    StsCreateSessionRequest,
    TdAttachRequest,
)
from mft_sts.impl import register
from mft_sts.session import SessionManager as StsSessionManager
from mft_sts.strategy import Strategy
from mft_td.session import PaperSessionFactory
from mft_td.session import SessionManager as TdSessionManager


class ReconStrategy(Strategy):
    name = "recon_probe"

    def __init__(self) -> None:
        super().__init__()
        self.done = asyncio.Event()
        self.last: ReconDone | None = None

    async def on_recon_done(self, msg: ReconDone) -> None:
        self.last = msg
        self.done.set()


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


@pytest.mark.asyncio
async def test_recon_handshake_and_strategy_oms(broker: Broker) -> None:
    register(ReconStrategy)
    instances: list[ReconStrategy] = []

    def factory(name: str | None) -> Strategy:
        assert name == "recon_probe"
        s = ReconStrategy()
        instances.append(s)
        return s

    paper = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=11
    )
    await paper.start()
    paper_factory = PaperSessionFactory(broker, paper)
    paper_factory.bind_api(1, "paper-key-1", "sec-1")

    # Seed an open order so recon has something to load.
    priv = paper.private(
        api_key="paper-key-1", api_secret="sec-1", auto_register=False
    )
    await priv.connect()
    await priv.place_limit_order(
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=Decimal("0.01"),
        price=Decimal("1000"),
    )
    await priv.close()

    sts = StsSessionManager(
        broker, heartbeat_interval=0.1, strategy_factory=factory
    )
    td = TdSessionManager(paper_factory, broker, lease_grace=2.0)

    await sts.create_session(
        StsCreateSessionRequest(
            session_id="recon-1",
            created_by=1,
            strategy="recon_probe",
            td=[1],
        )
    )
    await td.attach(
        TdAttachRequest(
            session_id="recon-1",
            api_id=1,
            timeout=2.0,
            created_by=1,
        )
    )

    strat = instances[0]
    await asyncio.wait_for(strat.done.wait(), timeout=3.0)
    assert strat.last is not None
    assert strat.last.api_id == 1
    assert strat.last.session_id == "recon-1"
    assert strat.last.oms.balances
    assert len(strat.last.oms.orders) >= 1

    # OMS mirror populated from td.oms.{api_id}
    for _ in range(20):
        view = strat.oms.get(1)
        if view is not None and view.orders:
            break
        await asyncio.sleep(0.05)
    view = strat.oms[1]
    assert len(view.orders) >= 1
    assert view.balances  # paper seeds quote/base balances
    # Local mirror should match ReconDone snapshot balances.
    assert {
        a: (str(b.free), str(b.locked)) for a, b in view.balances.items()
    } == {
        a: (str(b.free), str(b.locked))
        for a, b in strat.last.oms.balances.items()
    }

    await td.close_all()
    await sts.close_all()
    await paper.stop()
