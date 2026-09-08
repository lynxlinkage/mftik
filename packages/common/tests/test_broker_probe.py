"""``Broker.probe`` — a liveness question that leaves nothing behind.

Every other subject in this tree promises that a request nobody serves waits
for the next consumer. This one deliberately breaks that promise, because a
health check has no value once its caller has stopped waiting — and a dashboard
polling an instance that is down would otherwise leave a record per probe for
that instance to find when it finally boots.

What each transport does to keep the promise is its own business and is tested
in ``test_redis_transport.py`` and ``test_nats_transport.py`` — Redis caps and
expires a list, NATS stores nothing at all. What is here is the promise itself,
which is not about a queue: an instance coming up must not be handed a pile of
questions that grows with how long the dashboard has been asking.
"""

from __future__ import annotations

import asyncio
import time

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    Envelope,
    HealthCheck,
    HealthStatus,
    Topics,
    probe_is_stale,
)

SUBJECT = Topics.health("md", "md-jp-1")


def _probe_envelope() -> Envelope[HealthCheck]:
    return Envelope[HealthCheck].wrap(HealthCheck(), type="md.health", source="api")


def _status() -> Envelope[HealthStatus]:
    return Envelope[HealthStatus].wrap(
        HealthStatus(
            status="ok",
            service="md",
            instance="md-jp-1",
            domain="md",
            venues=["Bybit"],
        ),
        type="md.health",
        source="md",
    )


async def test_a_probe_to_nobody_times_out() -> None:
    """Which is the caller's answer of *down*, not an error to handle."""
    async with a_broker() as broker:
        with pytest.raises(RequestTimeoutError):
            await broker.probe(SUBJECT, _probe_envelope(), timeout=0.15)


#: Probes at a subject nobody serves. Far more than either transport will keep,
#: which is the point: what a booting instance is offered has to stop growing
#: well before this.
DEAD_PROBES = 64


async def test_probing_a_dead_instance_does_not_pile_up() -> None:
    """The leak this method exists to prevent, stated as the caller sees it.

    ``request`` would leave one record per probe for the next consumer —
    roughly 17k a day at a five-second refresh, per down instance — and the
    instance would open its next boot by answering every one of them. So this
    probes a subject nobody serves many times over, and only then starts
    serving, to see what the instance is handed.
    """
    async with a_broker() as broker:
        for _ in range(DEAD_PROBES):
            with pytest.raises(RequestTimeoutError):
                await broker.probe(SUBJECT, _probe_envelope(), timeout=0.02)

        stop = asyncio.Event()
        served: list[Envelope] = []

        async def serve() -> None:
            async for req in broker.serve(SUBJECT, stop=stop):
                served.append(req.envelope)

        task = asyncio.create_task(serve())
        try:
            # Long enough that anything waiting would have arrived. The probes
            # above each gave up in 20ms, so a queued one has had many times
            # that to turn up.
            await asyncio.sleep(1.0)
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        # Bounded, and by something other than how many were asked. Redis keeps
        # a capped tail and NATS keeps none, so the exact number is a
        # transport's to state — but neither may hand over the pile. What an
        # instance does with the few it may still find is ``probe_is_stale``'s
        # job, below.
        assert len(served) < DEAD_PROBES


async def test_a_served_probe_answers_like_any_request() -> None:
    """Leaving nothing behind is about the unanswered case, not the reply path."""
    async with a_broker() as broker:
        stop = asyncio.Event()

        async def serve() -> None:
            async for req in broker.serve(SUBJECT, stop=stop):
                await req.reply(_status())
                return

        task = asyncio.create_task(serve())
        await asyncio.sleep(0.3)
        try:
            reply = await broker.probe(SUBJECT, _probe_envelope(), timeout=2.0)
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        status = HealthStatus.model_validate(reply.payload)
        assert status.instance == "md-jp-1"
        assert status.venues == ["Bybit"]


async def test_a_probe_carries_a_reply_address_its_handler_can_use(
    a_probe_broker: Broker,
) -> None:
    """However a transport addresses a reply, the handler reads it the same way.

    Redis writes the address into the envelope because the serving process sees
    nothing else; NATS carries it beside the message. A handler checks
    ``reply_to`` before doing expensive work — ``mftik_td.backfill.session``
    does — so it has to be populated on both.
    """
    stop = asyncio.Event()
    seen: list[str | None] = []

    async def serve() -> None:
        async for req in a_probe_broker.serve(SUBJECT, stop=stop):
            seen.append(req.envelope.reply_to)
            await req.reply(_status())
            return

    task = asyncio.create_task(serve())
    await asyncio.sleep(0.3)
    try:
        await a_probe_broker.probe(SUBJECT, _probe_envelope(), timeout=2.0)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert seen and seen[0]


@pytest.fixture
async def a_probe_broker() -> Broker:
    async with a_broker() as client:
        yield client


def test_a_fresh_probe_is_not_stale() -> None:
    assert probe_is_stale(_probe_envelope()) is False


def test_a_probe_older_than_its_caller_is_stale() -> None:
    """What an instance coming back from an outage finds, if it finds anything.

    Answering these is not merely wasted work: the caller has stopped waiting,
    so every reply is addressed to nobody. This is the second line of defence
    behind ``probe`` leaving nothing to be found in the first place.
    """
    envelope = _probe_envelope().model_copy(update={"ts": time.time() - 3600})
    assert probe_is_stale(envelope) is True


def test_a_clock_ahead_of_ours_is_never_stale() -> None:
    """Skew should cost a wasted reply, never a healthy instance called down."""
    envelope = _probe_envelope().model_copy(update={"ts": time.time() + 3600})
    assert probe_is_stale(envelope) is False


def test_an_envelope_without_a_timestamp_is_not_dropped() -> None:
    """A peer old enough not to stamp one still deserves an answer."""

    class _NoTs:
        pass

    assert probe_is_stale(_NoTs()) is False
