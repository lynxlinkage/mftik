"""What must not end a serve loop.

``Broker.serve`` is a domain's control plane. When it returns, the process stays
up: sessions keep trading, the heartbeat keeps ticking, and every request piles
up unread.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from broker_harness import a_broker, inject_raw_request
from mftik.broker import Broker, IncomingRequest
from mftik.protocol import Envelope

SUBJECT = "demo"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-serve") as client:
        yield client


def _envelope(n: int) -> Envelope[dict[str, Any]]:
    return Envelope[dict].wrap({"n": n}, type="demo", source="test")


@pytest.mark.asyncio
async def test_an_unreadable_request_is_dropped_rather_than_served(
    broker: Broker,
) -> None:
    """The only choice is which one dies: the message or the control plane."""
    stop = asyncio.Event()
    got: asyncio.Future[IncomingRequest] = asyncio.get_running_loop().create_future()

    async def serve() -> None:
        async for req in broker.serve(SUBJECT, stop=stop):
            if not got.done():
                got.set_result(req)
            await req.reply(
                Envelope[dict].wrap({"ok": True}, type="demo.reply", source="test")
            )
            break

    task = asyncio.create_task(serve())
    await asyncio.sleep(0.2)
    await inject_raw_request(broker, SUBJECT, "{not an envelope")
    await broker.request(SUBJECT, _envelope(2), timeout=2)

    req = await asyncio.wait_for(got, timeout=10)
    stop.set()
    await asyncio.gather(task, return_exceptions=True)

    assert req.envelope.payload == {"n": 2}


@pytest.mark.asyncio
async def test_a_stop_event_ends_the_loop(broker: Broker) -> None:
    """The one thing that may end it, and it must actually end it."""
    stop = asyncio.Event()
    served: list[IncomingRequest] = []

    async def serve() -> None:
        async for req in broker.serve(SUBJECT, stop=stop):
            served.append(req)

    task = asyncio.create_task(serve())
    await asyncio.sleep(0.3)
    stop.set()

    await asyncio.wait_for(task, timeout=10)
    assert served == []
