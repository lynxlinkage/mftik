"""The shared NATS connection: one per xdist worker, prefixes unsubscribed.

F31. ``broker`` borrows the session connection; this file checks that borrow
and the ``/connz`` count. The count is shared clients only
(:func:`shared_client_rows`). B2-05 moved behaviour tests off ``just test``;
a private socket is still not this worker's connection, and the count
keeps ignoring it.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import (
    fetch_connz,
    prefixed_broker,
    session_loop,
    shared_client_name,
    shared_client_rows,
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


def test_connz_count_ignores_private_clients() -> None:
    """A private socket must not inflate the per-worker count."""
    report = {
        "num_connections": 4,
        "connections": [
            {"name": "mftik-pytest-gw0"},
            {"name": "mftik-pytest-gw1"},
            {"name": ""},
            {"cid": 7},
        ],
    }
    rows = shared_client_rows(report)
    assert [row["name"] for row in rows] == [
        "mftik-pytest-gw0",
        "mftik-pytest-gw1",
    ]
    assert report["num_connections"] == 4


@session_loop
@pytest.mark.component
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
@pytest.mark.component
async def test_one_nats_connection_per_xdist_worker(
    broker: Broker,
    nats_connection: NatsClient,
) -> None:
    """This worker's shared client is on ``/connz``, and only one of them.

    Private sockets are not named ``mftik-pytest-``, so they cannot inflate
    the count. Other workers open a shared client only when one of their
    tests asks for it, so the total is at most the worker count rather
    than exactly it.
    """
    assert broker.transport.nc is nats_connection
    assert nats_connection.is_connected

    names = [
        str(row.get("name", "")) for row in shared_client_rows(fetch_connz())
    ]
    assert names.count(shared_client_name()) == 1
    assert len(names) == len(set(names))
    assert len(names) <= xdist_worker_count()
