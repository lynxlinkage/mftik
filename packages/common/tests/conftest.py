"""Shared fixtures for the Gate adapter tests."""

from __future__ import annotations

import pytest
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
