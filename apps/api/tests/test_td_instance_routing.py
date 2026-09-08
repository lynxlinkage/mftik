"""A credential is only ever used from the instance its `apis` row names.

This is the compliance requirement, and it is one question asked in two very
different places. A deploy asks it once, in the open, and would notice a wrong
answer immediately. The backfill sweep asks it on a timer, for every account
with history, and a wrong answer there is a US TD opening a venue connection
with a JP-only key — on a schedule, quietly, forever.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from db_harness import a_database, an_instance, an_owner
from mftik.broker import Broker
from mftik.protocol import Envelope, TdBackfill, Topics
from mftik_api import backfill_cron, orchestrate
from mftik_api.backfill_cron import sweep
from mftik_api.orchestrate import _td_instance
from mftik_db.models.api import Api
from mftik_db.models.history import Attribution, Source
from mftik_db.repositories import OrderRepository

JP = "td-jp-1"
US = "td-us-1"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            jp = await an_instance(session, JP, "td")
            us = await an_instance(session, US, "td")
            session.add(
                Api(
                    id=1,
                    owner_id=1,
                    venue="Bybit",
                    api_key="jp-key",
                    api_secret="s",
                    type="HMAC",
                    instance_id=jp.id,
                )
            )
            session.add(
                Api(
                    id=2,
                    owner_id=1,
                    venue="Bybit",
                    api_key="us-key",
                    api_secret="s",
                    type="HMAC",
                    instance_id=us.id,
                )
            )
            await session.commit()
        for module in (backfill_cron, orchestrate):
            monkeypatch.setattr(module, "session_scope", database.scope)
        yield database.scope


def an_order(api_id: int) -> dict:
    from decimal import Decimal

    return {
        "api_id": api_id,
        "order_key": f"cid-{api_id}",
        "client_order_id": f"cid-{api_id}",
        "venue_order_id": None,
        "session_id": f"sess-{api_id}",
        "strategy": None,
        "cid_slot": None,
        "attribution": Attribution.DIRECT,
        "universal_ticker": "Bybit_Spot_BTCUSDT",
        "side": "buy",
        "order_type": "limit",
        "status": "filled",
        "qty": Decimal("1"),
        "price": Decimal("100"),
        "filled_qty": Decimal("1"),
        "avg_price": Decimal("100"),
        "submitted_at": 1000.0,
        "ts": 1000.0,
        "source": Source.STREAM,
    }


async def test_a_deploy_resolves_each_credential_to_its_own_instance(
    db,
) -> None:
    assert await _td_instance(1) == JP
    assert await _td_instance(2) == US


async def test_a_deploy_against_a_missing_credential_refuses(db) -> None:
    """Rather than falling back to a subject any TD could take."""
    from mftik_api.broker_rpc import DomainRpcError

    with pytest.raises(DomainRpcError) as refused:
        await _td_instance(999)
    assert refused.value.code == "unknown_api"


async def test_the_sweep_posts_each_account_to_its_own_queue(
    broker, db
) -> None:
    """The compliance hole this ticket closes, in the place it would fire.

    Unkeyed, both of these would land on one subject and whichever TD was free
    would take them — including the JP-only credential, from the US.
    """
    async with db() as session:
        await OrderRepository(session).bulk_upsert([an_order(1), an_order(2)])

    jp: list[TdBackfill] = []
    us: list[TdBackfill] = []
    jp_stop, jp_task = await _serve_instance(broker, JP, jp)
    us_stop, us_task = await _serve_instance(broker, US, us)
    try:
        asked = await sweep(broker)
        assert asked == 2
        assert [a.api_id for a in jp] == [1]
        assert [a.api_id for a in us] == [2]
    finally:
        jp_stop.set()
        us_stop.set()
        await asyncio.gather(jp_task, us_task, return_exceptions=True)


async def test_a_jp_credential_never_reaches_the_us_queue(broker, db) -> None:
    """Stated on its own because it is the sentence the requirement is in."""
    async with db() as session:
        await OrderRepository(session).bulk_upsert([an_order(1)])

    jp: list[TdBackfill] = []
    us: list[TdBackfill] = []
    jp_stop, jp_task = await _serve_instance(broker, JP, jp)
    us_stop, us_task = await _serve_instance(broker, US, us)
    try:
        await sweep(broker)
        assert [a.api_id for a in us] == []
        assert [a.api_id for a in jp] == [1]
    finally:
        jp_stop.set()
        us_stop.set()
        await asyncio.gather(jp_task, us_task, return_exceptions=True)


async def _serve_instance(
    broker: Broker, instance: str, seen: list[TdBackfill]
) -> tuple[asyncio.Event, asyncio.Task[None]]:
    stop = asyncio.Event()

    async def serve() -> None:
        async for req in broker.serve(Topics.td_backfill(instance), stop=stop):
            seen.append(TdBackfill.model_validate(req.envelope.payload))
            await req.reply(
                Envelope[dict].wrap(
                    {"ok": True}, type="td.backfill.result", source="td"
                )
            )

    task = asyncio.create_task(serve())
    await asyncio.sleep(0.2)
    return stop, task
