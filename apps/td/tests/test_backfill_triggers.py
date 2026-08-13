"""Who asks for a backfill, and what happens when asking fails.

The ranking is the design. A detach and a shutdown are latency — they settle
the record soon after somebody wants to read it. Neither is why it settles at
all; the schedule is. So both of these must be unable to hurt the thing they
are attached to: a detach that cannot reach Redis still detaches, and a
shutdown still shuts down.
"""

from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.protocol import Envelope, TdAttachRequest, TdBackfill, Topics
from mft_td.backfill.trigger import request_backfill
from mft_td.session import PaperSessionFactory, SessionManager

API_ID = 42
SESSION = "sts-trigger"


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


async def queued(broker: Broker) -> list[TdBackfill]:
    """Whatever is sitting on ``td.backfill`` right now."""
    key = f"test:rpc:{Topics.td_backfill()}"
    raw = await broker.redis.lrange(key, 0, -1)
    out = []
    for item in raw:
        envelope = Envelope[dict].model_validate_json(item)
        out.append(TdBackfill.model_validate(envelope.payload))
    return out


# --- posting ---------------------------------------------------------------


async def test_a_request_is_left_on_the_queue_for_whoever_takes_it(
    broker,
) -> None:
    """Posted, not requested: nobody here waits minutes for a walk."""
    assert await request_backfill(broker, API_ID, reason="cron")

    asks = await queued(broker)
    assert [(a.api_id, a.reason) for a in asks] == [(API_ID, "cron")]


async def test_a_request_survives_having_nobody_to_serve_it(broker) -> None:
    """The case a keyed subject cannot serve, and pub/sub would drop.

    Nothing is listening. The message waits in the list, and the next TD to
    come up takes it — which is exactly the account whose record has a hole.
    """
    await request_backfill(broker, API_ID, reason="shutdown")
    await asyncio.sleep(0.05)

    assert len(await queued(broker)) == 1


async def test_a_request_may_name_instruments(broker) -> None:
    await request_backfill(
        broker, API_ID, reason="detach", tickers=["Binance_Spot_BTCUSDT"]
    )

    assert (await queued(broker))[0].tickers == ["Binance_Spot_BTCUSDT"]


async def test_asking_never_raises_on_a_broken_broker(broker) -> None:
    """Every caller has something more important to be doing."""

    class Broken:
        async def post(self, *a, **kw):
            raise RuntimeError("redis is gone")

        config = broker.config

    assert await request_backfill(Broken(), API_ID, reason="cron") is False


async def test_asking_gives_up_rather_than_holding_a_teardown(broker) -> None:
    """An unreachable Redis must not hold a container past its stop timeout."""

    class Hanging:
        async def post(self, *a, **kw):
            await asyncio.sleep(30)

        config = broker.config

    result = await request_backfill(
        Hanging(), API_ID, reason="shutdown", timeout=0.05
    )
    assert result is False


async def test_a_cancelled_ask_is_not_swallowed(broker) -> None:
    """Best-effort is about Redis being unwell, not about ignoring a stop."""

    class Hanging:
        async def post(self, *a, **kw):
            await asyncio.sleep(30)

        config = broker.config

    task = asyncio.create_task(
        request_backfill(Hanging(), API_ID, reason="shutdown", timeout=30)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# --- detach ----------------------------------------------------------------


@pytest.fixture
async def paper():
    from decimal import Decimal

    from mft.exchange import PaperExchange

    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=7
    ) as ex:
        yield ex


async def _lease(broker: Broker, stop: asyncio.Event) -> None:
    from mft.protocol import STS_LEASE_HEARTBEAT, LeaseHeartbeat

    token = 0
    while not stop.is_set():
        token += 1
        await broker.publish(
            Topics.sts_td_session(SESSION),
            Envelope[LeaseHeartbeat].wrap(
                LeaseHeartbeat(session_id=SESSION, token=token),
                type=STS_LEASE_HEARTBEAT,
                source="sts",
                session_id=SESSION,
            ),
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except TimeoutError:
            continue


async def test_a_detach_asks_for_the_account_it_just_released(
    broker, paper
) -> None:
    """The moment somebody goes to look at what the run did."""
    manager = SessionManager(PaperSessionFactory(broker, paper), broker)
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease(broker, stop))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    try:
        await manager.detach(session_id=SESSION, api_id=API_ID)
    finally:
        stop.set()
        await asyncio.gather(pub, return_exceptions=True)
        await manager.close_all()

    asks = await queued(broker)
    assert [(a.api_id, a.reason) for a in asks] == [(API_ID, "detach")]


async def test_a_detach_for_an_account_that_was_never_attached_asks_nothing(
    broker, paper
) -> None:
    """There is no run to settle, and the row was closed by another path."""
    manager = SessionManager(PaperSessionFactory(broker, paper), broker)

    await manager.detach(session_id="never", api_id=API_ID)

    assert await queued(broker) == []
