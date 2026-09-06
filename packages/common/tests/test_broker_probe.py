"""``Broker.probe`` — a liveness question that leaves nothing behind.

Every other subject in this tree promises that a request nobody serves waits
for the next consumer. This one deliberately breaks that promise, because a
health check has no value once its caller has stopped waiting — and a
dashboard polling an instance that is down would otherwise write a record per
probe into a list nobody will ever drain, in the Redis that also carries order
entry.
"""

from __future__ import annotations

import asyncio
import time

import pytest
from broker_harness import a_broker
from mftik.broker.client import PROBE_QUEUE_MAXLEN, PROBE_QUEUE_TTL_SECONDS
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    Envelope,
    HealthCheck,
    HealthStatus,
    Topics,
    probe_is_stale,
)


def _probe_envelope() -> Envelope[HealthCheck]:
    return Envelope[HealthCheck].wrap(
        HealthCheck(), type="md.health", source="api"
    )


async def test_a_probe_to_nobody_times_out() -> None:
    """Which is the caller's answer of *down*, not an error to handle."""
    async with a_broker() as broker:
        with pytest.raises(RequestTimeoutError):
            await broker.probe(
                Topics.health("md", "md-jp-1"), _probe_envelope(), timeout=0.15
            )


async def test_probing_a_dead_instance_does_not_grow_without_end() -> None:
    """The leak this method exists to prevent.

    ``request`` would leave one record per probe in a list forever — roughly
    17k a day at a five-second refresh, per down instance. The cap is what
    makes a dashboard safe to point at something that is not there.
    """
    async with a_broker() as broker:
        subject = Topics.health("md", "md-jp-1")
        queue = f"{broker.config.key_prefix}:rpc:{subject}"

        for _ in range(PROBE_QUEUE_MAXLEN * 3):
            with pytest.raises(RequestTimeoutError):
                await broker.probe(subject, _probe_envelope(), timeout=0.02)

        assert await broker.redis.llen(queue) == PROBE_QUEUE_MAXLEN


async def test_a_probe_queue_expires_so_a_forgotten_key_goes_away() -> None:
    """The cap bounds a queue being written to; this is what removes it."""
    async with a_broker() as broker:
        subject = Topics.health("md", "md-jp-1")
        queue = f"{broker.config.key_prefix}:rpc:{subject}"

        with pytest.raises(RequestTimeoutError):
            await broker.probe(subject, _probe_envelope(), timeout=0.02)

        ttl = await broker.redis.ttl(queue)
        assert 0 < ttl <= PROBE_QUEUE_TTL_SECONDS


async def test_a_probe_reply_key_is_not_left_behind_on_timeout() -> None:
    """The other half of leaving nothing: the caller cleans up after itself."""
    async with a_broker() as broker:
        envelope = _probe_envelope()
        reply_key = f"{broker.config.key_prefix}:rpc:reply:{envelope.id}"

        with pytest.raises(RequestTimeoutError):
            await broker.probe(
                Topics.health("td", "td-jp-1"), envelope, timeout=0.02
            )

        assert await broker.redis.exists(reply_key) == 0


async def test_a_served_probe_answers_like_any_request() -> None:
    """The expiry is about the queue, not about the reply path."""
    async with a_broker() as broker:
        subject = Topics.health("md", "md-jp-1")
        stop = asyncio.Event()

        async def serve() -> None:
            async for req in broker.serve(subject, stop=stop):
                await req.reply(
                    Envelope[HealthStatus].wrap(
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
                )
                return

        task = asyncio.create_task(serve())
        try:
            reply = await broker.probe(subject, _probe_envelope(), timeout=2.0)
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        status = HealthStatus.model_validate(reply.payload)
        assert status.instance == "md-jp-1"
        assert status.venues == ["Bybit"]


def test_a_fresh_probe_is_not_stale() -> None:
    assert probe_is_stale(_probe_envelope()) is False


def test_a_probe_older_than_its_caller_is_stale() -> None:
    """What an instance coming back from an outage finds in its queue.

    Answering these is not merely wasted work: the caller deleted its reply key
    when it gave up, so every reply creates a key nobody will ever read — the
    litter the capped queue exists to avoid, manufactured on the way back up.
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
