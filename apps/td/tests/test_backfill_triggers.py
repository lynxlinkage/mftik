"""Who asks for a backfill, and what happens when asking fails.

The ranking is the design. A detach is latency — it settles the record soon
after somebody wants to read it. The schedule is why it settles at all. So
these must be unable to hurt the thing they are attached to.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.protocol import Envelope, TdAttachRequest, TdBackfill, Topics
from mftik_td.backfill.trigger import request_backfill
from mftik_td.session import PaperSessionFactory, SessionManager

API_ID = 42
SESSION = "sts-trigger"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


async def _serve_backfill(broker: Broker, stop: asyncio.Event, seen: list) -> None:
    async for req in broker.serve(Topics.td_backfill("td"), stop=stop):
        payload = TdBackfill.model_validate(req.envelope.payload)
        seen.append(payload)
        await req.reply(
            Envelope[dict].wrap(
                {"ok": True, "api_id": payload.api_id},
                type="td.backfill.result",
                source="td",
            )
        )


# --- asking ---------------------------------------------------------------


async def test_a_request_is_answered_when_td_is_there(broker) -> None:
    seen: list[TdBackfill] = []
    stop = asyncio.Event()
    task = asyncio.create_task(_serve_backfill(broker, stop, seen))
    await asyncio.sleep(0.2)
    try:
        assert await request_backfill(broker, API_ID, instance="td", reason="cron")
        assert [(a.api_id, a.reason) for a in seen] == [(API_ID, "cron")]
    finally:
        stop.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_a_request_fails_at_once_when_nobody_is_serving(broker) -> None:
    assert (
        await request_backfill(
            broker, API_ID, instance="td", reason="shutdown", timeout=0.3
        )
        is False
    )


async def test_a_request_may_name_instruments(broker) -> None:
    seen: list[TdBackfill] = []
    stop = asyncio.Event()
    task = asyncio.create_task(_serve_backfill(broker, stop, seen))
    await asyncio.sleep(0.2)
    try:
        await request_backfill(
            broker,
            API_ID,
            instance="td",
            reason="detach",
            tickers=["Binance_Spot_BTCUSDT"],
        )
        assert seen[0].tickers == ["Binance_Spot_BTCUSDT"]
    finally:
        stop.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_asking_never_raises_on_a_broken_broker(broker) -> None:
    class Broken:
        async def request(self, *a, **kw):
            raise RuntimeError("nats is gone")

        config = broker.config

    assert (
        await request_backfill(Broken(), API_ID, instance="td", reason="cron")
        is False
    )


async def test_asking_gives_up_rather_than_holding_a_teardown(broker) -> None:
    class Hanging:
        async def request(self, *a, **kw):
            await asyncio.sleep(30)

        config = broker.config

    result = await request_backfill(
        Hanging(), API_ID, instance="td", reason="detach", timeout=0.05
    )
    assert result is False


async def test_a_cancelled_ask_is_not_swallowed(broker) -> None:
    class Hanging:
        async def request(self, *a, **kw):
            await asyncio.sleep(30)

        config = broker.config

    task = asyncio.create_task(
        request_backfill(
            Hanging(), API_ID, instance="td", reason="detach", timeout=30
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# --- detach ----------------------------------------------------------------


@pytest.fixture
async def paper():
    from decimal import Decimal

    from mftik.exchange import PaperExchange

    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=7
    ) as ex:
        yield ex


async def _lease(broker: Broker, stop: asyncio.Event) -> None:
    from mftik.protocol import STS_LEASE_HEARTBEAT, LeaseHeartbeat

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
    seen: list[TdBackfill] = []
    stop_bf = asyncio.Event()
    bf = asyncio.create_task(_serve_backfill(broker, stop_bf, seen))
    await asyncio.sleep(0.2)

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
        stop_bf.set()
        await asyncio.gather(pub, bf, return_exceptions=True)
        await manager.close_all()

    assert [(a.api_id, a.reason) for a in seen] == [(API_ID, "detach")]


async def test_a_detach_for_an_account_that_was_never_attached_asks_nothing(
    broker, paper
) -> None:
    seen: list[TdBackfill] = []
    stop_bf = asyncio.Event()
    bf = asyncio.create_task(_serve_backfill(broker, stop_bf, seen))
    await asyncio.sleep(0.1)
    manager = SessionManager(PaperSessionFactory(broker, paper), broker)

    await manager.detach(session_id="never", api_id=API_ID)
    stop_bf.set()
    await asyncio.gather(bf, return_exceptions=True)

    assert seen == []
