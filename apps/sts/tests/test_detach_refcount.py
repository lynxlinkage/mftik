from __future__ import annotations

import asyncio
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange
from mft.protocol import StsCreateSessionRequest, TdAttachRequest, Topics
from mft_sts.session import SessionManager as StsSessionManager
from mft_td.rpc import dispatch as td_dispatch
from mft_td.session import PaperSessionFactory
from mft_td.session import SessionManager as TdSessionManager


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


async def _serve_td(
    broker: Broker, td: TdSessionManager, stop: asyncio.Event
) -> None:
    """TD's control-plane RPC, which is where a detach arrives."""
    async for req in broker.serve(Topics.TD, stop=stop):
        await td_dispatch(req, sessions=td)


@pytest.mark.asyncio
async def test_stop_one_sts_drops_td_refcount(broker: Broker) -> None:
    paper = PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=3
    )
    await paper.start()
    factory = PaperSessionFactory(broker, paper)
    sts = StsSessionManager(broker, heartbeat_interval=0.1)
    td = TdSessionManager(factory, broker, lease_grace=2.0)
    td_stop = asyncio.Event()
    td_rpc = asyncio.create_task(_serve_td(broker, td, td_stop))

    await sts.create_session(
        StsCreateSessionRequest(
            session_id="a", created_by=1, strategy="noop", td=[1]
        )
    )
    await sts.create_session(
        StsCreateSessionRequest(
            session_id="b", created_by=1, strategy="noop", td=[1]
        )
    )
    r1 = await td.attach(
        TdAttachRequest(session_id="a", api_id=1, timeout=2.0, created_by=1)
    )
    r2 = await td.attach(
        TdAttachRequest(session_id="b", api_id=1, timeout=2.0, created_by=1)
    )
    assert r1.refcount == 1
    assert r2.refcount == 2
    assert td.get(1) is not None

    await sts.close("a")
    # The detach is answered before close returns, but the teardown behind it
    # (stopping the link, destroying an account at refcount 0) settles just
    # after — so read the refcount rather than race it.
    for _ in range(30):
        acct = td._accounts.get(1)
        if acct is not None and acct.refcount == 1:
            break
        await asyncio.sleep(0.05)
    assert td.get(1) is not None
    assert td._accounts[1].refcount == 1

    buf = await broker.fetch_log_buffer(Topics.log_td(1))
    assert any("refcount 2→1" in line for line in buf)

    await sts.close("b")
    for _ in range(30):
        if td.get(1) is None:
            break
        await asyncio.sleep(0.05)
    assert td.get(1) is None

    await sts.close_all()
    td_stop.set()
    td_rpc.cancel()
    await asyncio.gather(td_rpc, return_exceptions=True)
    await td.close_all()
    await paper.stop()
