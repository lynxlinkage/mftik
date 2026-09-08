"""Stopping a session must not wait on the domains it is detaching from.

The lease is what actually ends an attach: TD and MD each watch this session's
heartbeat and run the identical teardown when it stops. The detach request is
promptness and a reason on the row, not the mechanism — so a domain that is
slow, busy or gone can cost a stop nothing.
"""

from __future__ import annotations

import asyncio
import time

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.protocol import (
    MD_SESSION_DETACH,
    TD_SESSION_DETACH,
    Envelope,
    Topics,
)
from mftik.strategy import Strategy
from mftik_sts.session.session import StsSession


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-detach") as client:
        yield client


def _session(broker: Broker, **kwargs) -> StsSession:
    return StsSession(
        session_id=kwargs.pop("session_id", "d-1"),
        broker=broker,
        created_by=1,
        strategy=Strategy(),
        heartbeat_interval=0.05,
        **kwargs,
    )


async def test_stop_does_not_wait_for_a_domain_that_never_answers(
    broker: Broker,
) -> None:
    """Nothing is serving TD or MD here — the old code took ten seconds."""
    session = _session(broker, td_api_ids=[1, 2], md_ids=["ticker.Paper_Spot_BTCUSDT"])
    await session.start()

    started = time.monotonic()
    await session.stop()
    elapsed = time.monotonic() - started

    assert elapsed < 2.0, f"stop took {elapsed:.1f}s"


async def test_a_serving_domain_receives_the_detach(broker: Broker) -> None:
    seen: list[str] = []
    stop = asyncio.Event()

    async def serve_td() -> None:
        async for req in broker.serve(Topics.td("td"), stop=stop):
            seen.append(req.envelope.type)
            await req.reply(
                Envelope[dict].wrap({}, type="td.session.detach", source="td")
            )

    task = asyncio.create_task(serve_td())
    await asyncio.sleep(0.2)
    session = _session(broker, session_id="d-2", td_api_ids=[7])
    await session.start()
    await session.stop()
    stop.set()
    await asyncio.gather(task, return_exceptions=True)

    assert TD_SESSION_DETACH in seen


async def test_a_broker_that_cannot_take_the_detach_still_stops(
    broker: Broker, monkeypatch
) -> None:
    """The lease covers it, so a failed request is a log line and not a hang."""

    async def boom(subject, envelope, **kwargs):  # noqa: ANN001, ANN202
        raise RuntimeError("nats is down")

    session = _session(broker, session_id="d-4", td_api_ids=[1])
    await session.start()
    monkeypatch.setattr(broker, "request", boom)

    started = time.monotonic()
    await session.stop()

    assert time.monotonic() - started < 2.0
    assert session.destroyed


async def test_the_heartbeat_stops_which_is_what_ends_the_attach(
    broker: Broker,
) -> None:
    """The detach is the courtesy; this is the mechanism."""
    session = _session(broker, session_id="d-5", td_api_ids=[1])
    await session.start()

    seen: list[str] = []
    stop = asyncio.Event()

    async def listen() -> None:
        async for env in broker.subscribe(
            Topics.sts_td_session("d-5"), stop=stop
        ):
            seen.append(env.type)

    task = asyncio.create_task(listen())
    await asyncio.sleep(0.2)
    assert seen, "no heartbeat while running"

    await session.stop()
    seen.clear()
    await asyncio.sleep(0.2)
    stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert seen == [], "heartbeat outlived the session"
