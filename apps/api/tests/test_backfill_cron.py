"""The sweep that keeps settlement lines moving.

The other two triggers fire when something happens. This one fires when nothing
does, which is the case that matters: an account that stopped trading is never
detached from again and never held by a process that shuts down, and its record
is exactly the one nothing else would ever repair.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import fakeredis.aioredis
import pytest
from db_harness import a_database
from mft.broker import Broker, BrokerConfig
from mft.protocol import Envelope, TdBackfill, Topics
from mft_api import backfill_cron
from mft_api.backfill_cron import run_backfill_cron, sweep
from mft_db.models.history import Attribution, Source
from mft_db.repositories import OrderRepository


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


@pytest.fixture
async def db(monkeypatch, database_url):
    """The database the sweep reads, in place of the one it would open."""
    async with a_database(database_url) as database:
        monkeypatch.setattr(backfill_cron, "session_scope", database.scope)
        yield database.scope


def an_order(api_id: int, *, ticker: str = "Binance_Spot_BTCUSDT") -> dict:
    return {
        "api_id": api_id,
        "order_key": f"cid-{api_id}",
        "client_order_id": f"cid-{api_id}",
        "venue_order_id": None,
        "session_id": f"sess-{api_id}",
        "strategy": None,
        "cid_slot": None,
        "attribution": Attribution.DIRECT,
        "universal_ticker": ticker,
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


async def queued(broker: Broker) -> list[TdBackfill]:
    raw = await broker.redis.lrange(f"test:rpc:{Topics.td_backfill()}", 0, -1)
    return [
        TdBackfill.model_validate(Envelope[dict].model_validate_json(i).payload)
        for i in raw
    ]


@pytest.fixture(autouse=True)
def no_pause(monkeypatch):
    monkeypatch.setattr(backfill_cron, "ACCOUNT_PAUSE_S", 0)


async def test_every_account_with_history_is_asked_about(broker, db) -> None:
    async with db() as session:
        await OrderRepository(session).bulk_upsert(
            [an_order(1), an_order(2), an_order(3)]
        )

    asked = await sweep(broker)

    assert asked == 3
    assert sorted(a.api_id for a in await queued(broker)) == [1, 2, 3]
    assert {a.reason for a in await queued(broker)} == {"cron"}


async def test_an_account_that_stopped_trading_is_still_swept(broker, db) -> None:
    """The case no per-event trigger reaches: nothing detaches from it again."""
    async with db() as session:
        await OrderRepository(session).bulk_upsert([an_order(7)])

    await sweep(broker)

    assert [a.api_id for a in await queued(broker)] == [7]


async def test_a_credential_that_never_traded_is_not_asked_about(
    broker, db
) -> None:
    """Derived from the record, not from the ``apis`` table."""
    assert await sweep(broker) == 0
    assert await queued(broker) == []


async def test_one_account_is_asked_about_once_per_sweep(broker, db) -> None:
    """Several instruments are one walk, not one request each."""
    async with db() as session:
        repo = OrderRepository(session)
        await repo.bulk_upsert([an_order(5)])
        await repo.bulk_upsert(
            [
                {
                    **an_order(5),
                    "order_key": "cid-b",
                    "universal_ticker": "Binance_Spot_ETHUSDT",
                }
            ]
        )

    await sweep(broker)

    assert [a.api_id for a in await queued(broker)] == [5]


async def test_the_loop_sweeps_on_its_interval(broker, db, monkeypatch) -> None:
    async with db() as session:
        await OrderRepository(session).bulk_upsert([an_order(1)])
    monkeypatch.setattr(backfill_cron, "Broker", lambda *a, **kw: broker)
    monkeypatch.setattr(broker, "close", _noop)

    stop = asyncio.Event()
    task = asyncio.create_task(run_backfill_cron(stop, interval=0.05))
    for _ in range(60):
        await asyncio.sleep(0.02)
        if await queued(broker):
            break
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert [a.api_id for a in await queued(broker)] == [1]


async def test_a_failed_sweep_does_not_end_the_loop(broker, db, monkeypatch) -> None:
    """The next tick asks again; a stalled cron is the only real failure."""
    calls = {"n": 0}

    async def flaky(_broker, *, reason="cron"):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("database is down")
        return 1

    monkeypatch.setattr(backfill_cron, "Broker", lambda *a, **kw: broker)
    monkeypatch.setattr(broker, "close", _noop)
    monkeypatch.setattr(backfill_cron, "sweep", flaky)

    stop = asyncio.Event()
    task = asyncio.create_task(run_backfill_cron(stop, interval=0.05))
    for _ in range(60):
        await asyncio.sleep(0.02)
        if calls["n"] >= 2:
            break
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert calls["n"] >= 2, "the loop survived the first sweep failing"


async def _noop(*a, **kw) -> None:
    return None
