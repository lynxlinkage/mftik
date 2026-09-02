from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.protocol import (
    StsCreateSessionRequest,
    TdAccountRef,
    TdAttachRequest,
    Topics,
)
from mftik_sts.session import SessionManager as StsSessionManager
from mftik_td.rpc import dispatch as td_dispatch
from mftik_td.session import PaperSessionFactory
from mftik_td.session import SessionManager as TdSessionManager


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


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
            session_id="a",
            created_by=1,
            strategy="noop",
            td={"paper": TdAccountRef(api_id=1)},
        )
    )
    await sts.create_session(
        StsCreateSessionRequest(
            session_id="b",
            created_by=1,
            strategy="noop",
            td={"paper": TdAccountRef(api_id=1)},
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
