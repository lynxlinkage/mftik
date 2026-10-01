"""The shared NATS connection: one per xdist worker, prefixes unsubscribed.

F31. ``broker`` borrows the session connection; this file checks that borrow
and the ``/connz`` count the ticket asks for. pytest-xdist is not installed
yet (B2-04), so the count under a normal run is 1. The comparison is against
:func:`xdist_worker_count`, which reads ``PYTEST_XDIST_WORKER_COUNT`` when a
worker is running.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import (
    fetch_connz,
    prefixed_broker,
    session_loop,
    subjects_under,
    xdist_worker_count,
)
from mftik.broker import Broker
from nats.aio.client import Client as NatsClient


def test_xdist_worker_count_is_one_without_xdist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_XDIST_WORKER_COUNT", raising=False)
    assert xdist_worker_count() == 1


def test_xdist_worker_count_reads_the_worker_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "4")
    assert xdist_worker_count() == 4


@session_loop
async def test_two_prefixes_share_the_connection_and_leave_it_up(
    nats_connection: NatsClient,
) -> None:
    """Teardown unsubscribes one prefix and does not close the socket."""
    async with prefixed_broker(nats_connection) as first:
        async with prefixed_broker(nats_connection) as second:
            assert first.transport.nc is second.transport.nc is nats_connection
            assert first.config.key_prefix != second.config.key_prefix
            second_prefix = second.config.key_prefix
            stop = asyncio.Event()
            ready = asyncio.Event()

            async def reader() -> None:
                async for _envelope in second.subscribe(
                    "topic.cleanup", stop=stop, ready=ready
                ):
                    return

            task = asyncio.create_task(reader())
            await asyncio.wait_for(ready.wait(), timeout=5)
            assert subjects_under(nats_connection, second_prefix)
        # ``second`` has unsubscribed its prefix. ``first`` is still on the
        # same socket.
        assert not subjects_under(nats_connection, second_prefix)
        assert first.transport.nc is nats_connection
        assert nats_connection.is_connected
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert nats_connection.is_connected
    assert not nats_connection.is_closed


@session_loop
async def test_one_nats_connection_per_xdist_worker(
    broker: Broker,
    nats_connection: NatsClient,
) -> None:
    """``/connz`` reports one live connection per xdist worker.

    Sampled in this test, while only the shared connection is held. A test
    that has not moved to ``broker`` still opens a private socket for its
    own body and closes it before returning, so it is not in this count.
    With one worker — xdist is not installed yet — the count is 1.
    """
    assert broker.transport.nc is nats_connection
    assert nats_connection.is_connected

    report = fetch_connz()
    expected = xdist_worker_count()
    names = [row.get("name", "") for row in report.get("connections", [])]
    assert report["num_connections"] == expected, (
        f"/connz num_connections={report['num_connections']}, "
        f"xdist workers={expected}, names={names}"
    )
