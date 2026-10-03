"""The broker every test runs against.

This is not a fake. Isolation is a ``key_prefix`` nobody else has. Core
NATS stores nothing, so a prefix that has been unsubscribed is gone.

Two ways in:

* :func:`broker` is the shared connection. One xdist worker opens one
  NATS connection, on pytest-asyncio's session event loop, and each test
  borrows it behind a fresh prefix. Teardown unsubscribes that prefix and
  leaves the connection up (F31).
* :func:`a_broker` still opens a private connection and closes it. Tests
  that have not moved to :func:`broker` keep using it.
"""

from __future__ import annotations

import contextlib
import json
import os
import socket
import urllib.error
import urllib.request
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, closing
from typing import Any
from urllib.parse import urlsplit

import nats
import pytest
import pytest_asyncio
from mftik.broker import Broker, BrokerConfig
from mftik.broker.transport.nats import NatsTransport
from nats.aio.client import Client as NatsClient

#: Monitoring port compose and CI publish (``nats-server -m 8222``).
_MONITOR_PORT = 8222

#: How :meth:`NatsTransport.connect` opens a client. Repeated here so the
#: shared connection is the same shape as a private one; ``name`` is the
#: only extra, and it is what ``/connz`` shows.
_CONNECT_KWARGS: dict[str, Any] = {
    "max_reconnect_attempts": -1,
    "pending_size": 8 * 1024 * 1024,
}

#: Async tests that take :func:`broker` run on the session loop. The shared
#: client is bound to that loop; a function-scoped loop cannot drive it.
session_loop = pytest.mark.asyncio(loop_scope="session")


def xdist_worker_id() -> str:
    """This process's xdist worker name, or ``gw0`` when xdist is not driving."""
    return os.environ.get("PYTEST_XDIST_WORKER", "gw0")


def xdist_worker_count() -> int:
    """How many xdist workers this run has.

    ``PYTEST_XDIST_WORKER_COUNT`` is set in each worker. Without xdist there
    is one process, and the count is 1.
    """
    raw = os.environ.get("PYTEST_XDIST_WORKER_COUNT")
    if raw is None or raw == "":
        return 1
    return int(raw)


#: Prefix of :func:`shared_client_name`. Private sockets do not use it, so a
#: ``/connz`` count can ignore them. B2-05 moved behaviour tests off
#: ``just test``; the filter stays so a private socket cannot flake the count.
SHARED_CLIENT_PREFIX = "mftik-pytest-"


def shared_client_name() -> str:
    """Client name ``/connz`` reports for this worker's shared connection."""
    return f"{SHARED_CLIENT_PREFIX}{xdist_worker_id()}"


def shared_client_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    """``/connz`` rows whose name is a shared pytest client.

    ``num_connections`` counts every socket, including the private ones
    tests still open with :func:`a_broker`. Those are not the per-worker
    shared connection, and under xdist they make that count flaky.
    """
    rows = report.get("connections") or []
    if not isinstance(rows, list):
        return []
    return [
        row
        for row in rows
        if isinstance(row, dict)
        and str(row.get("name", "")).startswith(SHARED_CLIENT_PREFIX)
    ]


def unique_key_prefix(stem: str = "test") -> str:
    """A subject root no other test, on any worker, is using."""
    return f"{stem}-{xdist_worker_id()}-{uuid.uuid4().hex[:10]}"


def test_config(key_prefix: str) -> BrokerConfig:
    """A broker config for one test."""
    return BrokerConfig(
        nats_url=os.getenv("NATS_URL", "nats://localhost:4222"),
        key_prefix=key_prefix,
    )


def subjects_under(connection: NatsClient, key_prefix: str) -> list[str]:
    """Subjects this connection is still subscribed to under ``key_prefix``.

    nats-py does not list live subscriptions. ``_subs`` is the map
    :meth:`nats.aio.subscription.Subscription.unsubscribe` removes from.
    """
    needle = f"{key_prefix}."
    return sorted(
        sub.subject
        for sub in connection._subs.values()  # noqa: SLF001
        if sub.subject.startswith(needle)
    )


async def unsubscribe_prefix(connection: NatsClient, key_prefix: str) -> None:
    """Drop every subscription under ``key_prefix`` and leave the connection up.

    A flush follows the UNSUBs so the server has dropped the interest before
    the next test publishes on a different prefix.
    """
    needle = f"{key_prefix}."
    for sub in list(connection._subs.values()):  # noqa: SLF001
        if not sub.subject.startswith(needle):
            continue
        with contextlib.suppress(Exception):
            await sub.unsubscribe()
    if connection.is_connected:
        with contextlib.suppress(Exception):
            await connection.flush(timeout=2)
    leftover = subjects_under(connection, key_prefix)
    if leftover:
        raise RuntimeError(
            f"subscriptions still open under {key_prefix!r}: {leftover}"
        )


@asynccontextmanager
async def prefixed_broker(
    connection: NatsClient,
    *,
    stem: str = "test",
) -> AsyncIterator[Broker]:
    """A broker on ``connection`` with its own prefix, unsubscribed on exit.

    ``close`` is not called. The transport does not own the connection, so
    a caller that does close the broker still leaves the socket up.
    """
    prefix = unique_key_prefix(stem)
    config = test_config(prefix)
    client = Broker(
        config,
        transport=NatsTransport(config, connection=connection),
    )
    await client.connect()
    try:
        yield client
    finally:
        await unsubscribe_prefix(connection, prefix)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def nats_connection() -> AsyncIterator[NatsClient]:
    """The one NATS connection this xdist worker opens.

    Torn down when the worker's session ends, not between tests.
    """
    config = test_config("shared")
    connection = await nats.connect(
        config.nats_url,
        name=shared_client_name(),
        **_CONNECT_KWARGS,
    )
    try:
        yield connection
    finally:
        if not connection.is_closed:
            with contextlib.suppress(Exception):
                await connection.close()


@pytest_asyncio.fixture(loop_scope="session")
async def broker(nats_connection: NatsClient) -> AsyncIterator[Broker]:
    """This test's broker: the shared connection, a prefix nobody else has."""
    async with prefixed_broker(nats_connection) as client:
        yield client


@asynccontextmanager
async def a_broker(key_prefix: str = "test") -> AsyncIterator[Broker]:
    """A private connection, closed on the way out.

    Tests that have not moved to :func:`broker` still call this. It opens
    its own socket on the caller's loop, which is not the shared one.
    """
    prefix = f"{key_prefix}-{uuid.uuid4().hex[:10]}"
    client = Broker(test_config(prefix))
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


async def inject_raw_request(broker: Broker, subject: str, raw: str) -> None:
    """Publish ``raw`` onto the core RPC subject, bypassing the broker.

    For the one thing ``request`` cannot express: bytes that will not parse.
    A serve loop must already be listening — core NATS stores nothing.
    """
    transport = broker.transport
    await transport.nc.publish(transport._rpc_subject(subject), raw.encode())  # noqa: SLF001


def server_address() -> tuple[str, int]:
    """Host and port of the NATS server this run needs, for a reachability check."""
    config = test_config("probe")
    parts = urlsplit(config.nats_url)
    return parts.hostname or "localhost", parts.port or 4222


def server_is_up(timeout: float = 1.0) -> bool:
    """Whether the NATS server is accepting connections."""
    host, port = server_address()
    with closing(socket.socket()) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


def monitor_url() -> str:
    """``/connz`` base URL: the NATS host, on the monitoring port."""
    host, _port = server_address()
    return f"http://{host}:{_MONITOR_PORT}"


def fetch_connz() -> dict[str, Any]:
    """The server's current ``/connz`` document.

    ``num_connections`` is the live count. CI and compose start NATS with
    ``-m 8222``; without that port there is nothing to assert against.
    """
    url = f"{monitor_url()}/connz"
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            payload = json.load(response)
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"NATS monitoring is not reachable at {url}. "
            f"Start the server with -m {_MONITOR_PORT}."
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected /connz payload from {url}: {payload!r}")
    return payload
