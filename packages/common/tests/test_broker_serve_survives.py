"""What must not end a serve loop.

``Broker.serve`` is a domain's control plane. When it returns, the process stays
up: sessions keep trading, the heartbeat keeps ticking, and every request piles
up unread. On 2026-08-18 STS spent seven hours in that state after one ``BLPOP``
raised ``TimeoutError`` on a socket that stalled — a failure that costs one poll,
ended a control plane, and left no line in any log.

The transport-shaped half of that — what a failed read looks like and that the
next one is tried — is in ``test_redis_transport.py`` and
``test_nats_transport.py``, because a stalled blocking pop and a reaped consumer
have nothing in common but the answer. What is here is the half that is the
broker's own: a message the broker itself cannot read.
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


async def _first_request(broker: Broker, stop: asyncio.Event) -> IncomingRequest:
    async for req in broker.serve(SUBJECT, stop=stop):
        return req
    raise AssertionError("serve ended without yielding")


@pytest.mark.asyncio
async def test_an_unreadable_request_is_dropped_rather_than_served(
    broker: Broker,
) -> None:
    """The transport has already taken it, so the only choice is which one dies.

    There is no way to hand it back and nothing to skip past — a serve loop that
    raised here would take the subject's whole control plane down over one
    malformed message. So it drops that one and keeps serving, which is what the
    good request arriving behind it proves.
    """
    stop = asyncio.Event()
    await inject_raw_request(broker, SUBJECT, "{not an envelope")
    await broker.post(SUBJECT, _envelope(2))

    req = await asyncio.wait_for(_first_request(broker, stop), timeout=10)

    assert req.envelope.payload == {"n": 2}
    stop.set()


@pytest.mark.asyncio
async def test_posted_work_waits_for_a_consumer(broker: Broker) -> None:
    """``post``'s whole reason to be durable, on whichever store is underneath.

    TD's shutdown hands off a backfill and the API's cron sweeps account
    history; both post to a subject whose owner may not be up yet. A transport
    that dropped this would lose a jurisdiction-bound credential quietly not
    being read, which is the kind of failure nobody notices for a quarter.
    """
    stop = asyncio.Event()
    await broker.post(SUBJECT, _envelope(7))

    req = await asyncio.wait_for(_first_request(broker, stop), timeout=10)

    assert req.envelope.payload == {"n": 7}
    # Nobody is waiting, so there is no address to answer — and a handler that
    # always replies has to be safe to post to.
    assert req.envelope.reply_to is None
    stop.set()


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
