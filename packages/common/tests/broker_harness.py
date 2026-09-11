"""The broker every test runs against.

Sixty-odd test modules across six packages want the same thing: a connected
:class:`Broker` that nothing else is using, closed after.

This is not a fake. Isolation is a ``key_prefix`` nobody else has. Core
NATS stores nothing, so teardown is just ``close``.
"""

from __future__ import annotations

import os
import socket
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, closing
from urllib.parse import urlsplit

from mftik.broker import Broker, BrokerConfig


def test_config(key_prefix: str) -> BrokerConfig:
    """A broker config for one test."""
    return BrokerConfig(
        nats_url=os.getenv("NATS_URL", "nats://localhost:4222"),
        key_prefix=key_prefix,
    )


@asynccontextmanager
async def a_broker(key_prefix: str = "test") -> AsyncIterator[Broker]:
    """A connected broker nothing else shares, cleaned up on the way out."""
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
