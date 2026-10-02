"""Who asks for a backfill, and what happens when asking fails.

The ranking is the design. The schedule is why the record settles at all, and
an ask on top of it is only latency. So an ask must be unable to hurt the
thing it is attached to.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.protocol import Envelope, TdBackfill, Topics
from mftik_td.backfill.trigger import request_backfill

# Integration until B2-05. These borrow a private NATS connection
# to test behaviour, and the slow cases miss the 50 ms unit cap.
pytestmark = pytest.mark.integration

API_ID = 42


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


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
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


@pytest.mark.real_sleep(
    reason="NATS no-responders grace is a real asyncio.sleep"
)
async def test_a_request_fails_at_once_when_nobody_is_serving(broker) -> None:
    assert (
        await request_backfill(
            broker, API_ID, instance="td", reason="shutdown", timeout=0.3
        )
        is False
    )


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
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


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
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


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_an_in_flight_refusal_is_not_accepted(broker) -> None:
    """Saturated TD replies ``ok=False``; that is not a successful ask."""
    stop = asyncio.Event()

    async def refuse() -> None:
        async for req in broker.serve(Topics.td_backfill("td"), stop=stop):
            payload = TdBackfill.model_validate(req.envelope.payload)
            await req.reply(
                Envelope[dict].wrap(
                    {
                        "ok": False,
                        "api_id": payload.api_id,
                        "reason": "4 runs already in flight",
                    },
                    type="td.backfill.result",
                    source="td",
                )
            )
            break

    task = asyncio.create_task(refuse())
    await asyncio.sleep(0.2)
    try:
        assert (
            await request_backfill(broker, API_ID, instance="td", reason="cron")
            is False
        )
    finally:
        stop.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
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
