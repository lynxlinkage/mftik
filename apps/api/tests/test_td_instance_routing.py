"""A credential is only ever used from the instance its `apis` row names.

This is the compliance requirement, and it is one question asked in two very
different places. A deploy asks it once, in the open, and would notice a wrong
answer immediately. The backfill sweep asks it on a timer, for every account
with history, and a wrong answer there is a US TD opening a venue connection
with a JP-only key — on a schedule, quietly, forever.
"""

from __future__ import annotations

import pytest
from broker_harness import a_broker, queued_requests
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

    Unkeyed, both of these would land on one queue and whichever TD was free
    would take them — including the JP-only credential, from the US.
    """
    async with db() as session:
        await OrderRepository(session).bulk_upsert([an_order(1), an_order(2)])

    asked = await sweep(broker)

    assert asked == 2
    assert await _queued(broker, JP) == [1]
    assert await _queued(broker, US) == [2]


async def test_a_jp_credential_never_reaches_the_us_queue(broker, db) -> None:
    """Stated on its own because it is the sentence the requirement is in."""
    async with db() as session:
        await OrderRepository(session).bulk_upsert([an_order(1)])

    await sweep(broker)

    assert await _queued(broker, US) == []


async def _queued(broker: Broker, instance: str) -> list[int]:
    raw = await queued_requests(broker, Topics.td_backfill(instance))
    return [
        TdBackfill.model_validate(
            Envelope.model_validate_json(item).payload
        ).api_id
        for item in raw
    ]
