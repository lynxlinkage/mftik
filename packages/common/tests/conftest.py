"""Shared fixtures for the venue adapter tests."""

from __future__ import annotations

import pytest
from binance_future_stub import FakeBinanceFutureApi, FakeBinanceFutureUser
from binance_stub import FakeBinanceApi, FakeBinanceStream, keypair
from bybit_stub import FakeBybit
from gate_stub import FakeGate
from websockets.asyncio.server import serve


@pytest.fixture
async def gate():
    """A FakeGate listening on an ephemeral port; ``gate.url`` points at it."""
    fake = FakeGate()
    server = await serve(fake.handler, "127.0.0.1", 0)
    fake.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    yield fake
    server.close()
    await server.wait_closed()


@pytest.fixture
def binance_key():
    """``(private_key, pem)`` — the credential a Binance user would paste in."""
    return keypair()


@pytest.fixture
async def binance_api(binance_key):
    """A FakeBinanceApi that verifies logon signatures against ``binance_key``."""
    private_key, _pem = binance_key
    fake = FakeBinanceApi(public_key=private_key.public_key())
    server = await serve(fake.handler, "127.0.0.1", 0)
    fake.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    yield fake
    server.close()
    await server.wait_closed()


@pytest.fixture
async def binance_stream():
    """A FakeBinanceStream on an ephemeral port; ``.url`` points at it."""
    fake = FakeBinanceStream()
    server = await serve(fake.handler, "127.0.0.1", 0)
    fake.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    yield fake
    server.close()
    await server.wait_closed()


@pytest.fixture
async def future_api(binance_key):
    """A FakeBinanceFutureApi verifying logon signatures against ``binance_key``."""
    private_key, _pem = binance_key
    fake = FakeBinanceFutureApi(public_key=private_key.public_key())
    server = await serve(fake.handler, "127.0.0.1", 0)
    fake.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    yield fake
    server.close()
    await server.wait_closed()


@pytest.fixture
async def future_user():
    """The futures user data socket — the listen-key connection."""
    fake = FakeBinanceFutureUser()
    server = await serve(fake.handler, "127.0.0.1", 0)
    fake.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    yield fake
    server.close()
    await server.wait_closed()


@pytest.fixture
async def future_public_stream():
    """The futures ``/public`` market socket — book feeds only."""
    fake = FakeBinanceStream()
    server = await serve(fake.handler, "127.0.0.1", 0)
    fake.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    yield fake
    server.close()
    await server.wait_closed()


@pytest.fixture
async def future_market_stream():
    """The futures ``/market`` socket — tape, candles, stats, liquidations.

    A second server rather than a second subscription on the first: that is
    what the venue is since the 2026 endpoint split, and a test sharing one
    stub would never catch a stream routed to the wrong host.
    """
    fake = FakeBinanceStream()
    server = await serve(fake.handler, "127.0.0.1", 0)
    fake.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    yield fake
    server.close()
    await server.wait_closed()


@pytest.fixture
async def bybit():
    """A FakeBybit that verifies auth signatures; ``.url`` points at it.

    One fixture for all three of Bybit's sockets: they speak the same envelope,
    so a test picks which it is by what it sends and what the stub pushes.
    """
    fake = FakeBybit()
    server = await serve(fake.handler, "127.0.0.1", 0)
    fake.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    yield fake
    server.close()
    await server.wait_closed()


@pytest.fixture
async def bybit_public():
    """A FakeBybit standing in for a public socket — no credential expected."""
    fake = FakeBybit(api_secret=None)
    server = await serve(fake.handler, "127.0.0.1", 0)
    fake.url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    yield fake
    server.close()
    await server.wait_closed()
