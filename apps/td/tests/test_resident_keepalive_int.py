"""A warm pool reuses one TCP connection across an idle past httpx's default.

httpx drops an idle connection after 5s (``Limits.keepalive_expiry``).
Ten real minutes cannot run here. The interval and the expiry are
scaled down, and the idle between application requests is longer than
that default 5s:

* with the keepalive loop running, the requests reuse one connection
* with the loop off and a default client, the same spacing opens a new one

The 10-minute claim is those two facts at the adapter's real interval
and expiry. The venue's own idle limit is measured later (B4-09).
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from mftik.clock import SystemClock
from mftik_td.account.resident import ResidentLayer

pytestmark = pytest.mark.integration

#: Longer than httpx's default ``keepalive_expiry`` (5s), inside the
#: 10s integration cap. The two scenarios run together, so the wall
#: time is one gap, not two.
_GAP_S = 6.0
#: Scaled stand-in for the adapter interval. Well under the expiry.
_INTERVAL_S = 0.25
#: Scaled stand-in for the adapter expiry. Longer than the interval,
#: shorter than the gap, so the connection survives the gap only
#: because the loop keeps using it.
_EXPIRY_S = 1.0

# A body BybitPublicRest.server_time accepts. The assertion is the
# TCP accept count, and a parse failure would still return the
# connection, but a successful read is what the loop is for.
_BODY = (
    b'{"retCode":0,"retMsg":"OK","result":{"timeSecond":"1700000000",'
    b'"timeNano":"1700000000000000000"},"time":1700000000000}'
)


class _Server:
    """Loopback HTTP/1.1. Counts accepted TCP connections."""

    def __init__(self) -> None:
        self.accepts = 0
        self.requests = 0
        self._server: asyncio.Server | None = None

    @property
    def url(self) -> str:
        assert self._server is not None and self._server.sockets
        sock = self._server.sockets[0]
        host, port = sock.getsockname()[:2]
        return f"http://{host}:{port}"

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)

    async def close(self) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.accepts += 1
        try:
            while True:
                try:
                    head = await reader.readuntil(b"\r\n\r\n")
                except (
                    asyncio.IncompleteReadError,
                    ConnectionResetError,
                    BrokenPipeError,
                ):
                    break
                if not head:
                    break
                self.requests += 1
                writer.write(
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    b"Content-Length: " + str(len(_BODY)).encode() + b"\r\n"
                    b"Connection: keep-alive\r\n"
                    b"\r\n" + _BODY
                )
                await writer.drain()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                return


async def _warm(server: _Server) -> None:
    layer = ResidentLayer(1, venue="Bybit", clock=SystemClock())
    # One in-flight request holds the only socket. A second request
    # started while a keepalive is on that socket opens another TCP
    # connection, which would make the accept count lie. The lock
    # keeps the bookend reads off that socket until it is idle.
    gate = asyncio.Lock()
    send = layer.keepalive_once

    async def _send_alone() -> None:
        async with gate:
            await send()

    layer.keepalive_once = _send_alone  # type: ignore[method-assign]
    await layer.start(
        base_urls=(server.url,),
        interval_s=_INTERVAL_S,
        keepalive_expiry_s=_EXPIRY_S,
    )
    try:
        await asyncio.sleep(_GAP_S)
        client = layer.pool.client_for(server.url) if layer.pool is not None else None
        assert client is not None
        async with gate:
            await client.get("/v5/market/time")
            await client.get("/v5/market/time")
    finally:
        await layer.close()


async def _cold(server: _Server) -> None:
    async with httpx.AsyncClient(base_url=server.url, timeout=5.0) as client:
        await client.get("/v5/market/time")
        await asyncio.sleep(_GAP_S)
        await client.get("/v5/market/time")
        await client.get("/v5/market/time")


async def test_keepalive_reuses_one_connection_past_the_default_expiry() -> None:
    warm = _Server()
    cold = _Server()
    await warm.start()
    await cold.start()
    try:
        await asyncio.gather(_warm(warm), _cold(cold))
    finally:
        await warm.close()
        await cold.close()
    assert warm.accepts == 1
    assert warm.requests >= 4
    assert cold.accepts == 2
    assert cold.requests == 3
