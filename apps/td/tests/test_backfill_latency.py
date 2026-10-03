"""Order-path latency on a warm pool while backfill is using it (B6-05).

A loopback server answers every request after a fixed delay. The order
path is the pool client itself. Backfill is the resident layer's capped
client, the same object a history reader is given, sending many
requests at once. Real venue numbers are B4-09.

The bound: while backfill holds :data:`BACKFILL_MAX_CONNECTIONS`
requests, order-path p50 and p99 stay within 2.5× the same figures
taken on the pool alone.
"""

from __future__ import annotations

import asyncio
import time

import pytest
from mftik.clock import FakeClock
from mftik_td.account import AccountWorker
from mftik_td.account.resident import BACKFILL_MAX_CONNECTIONS

pytestmark = pytest.mark.integration

_DELAY_S = 0.02
_SAMPLES = 16
_HAMMER = 24
#: Order p50 and p99 during the hammer, relative to the idle sample.
_BOUND = 2.5


def _percentile(samples: list[float], p: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    rank = (len(ordered) - 1) * p
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] * (1 - (rank - lo)) + ordered[hi] * (rank - lo)


class _Server:
    def __init__(self, delay: float) -> None:
        self._delay = delay
        self.backfill_now = 0
        self.backfill_max = 0
        self._server: asyncio.Server | None = None

    @property
    def url(self) -> str:
        assert self._server is not None
        sockets = self._server.sockets
        assert sockets
        host, port = sockets[0].getsockname()[:2]
        return f"http://{host}:{port}"

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._connection, "127.0.0.1", 0
        )

    async def close(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()

    async def _connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        buffer = b""
        try:
            while True:
                while b"\r\n\r\n" not in buffer:
                    chunk = await reader.read(4096)
                    if not chunk:
                        return
                    buffer += chunk
                    if len(buffer) > 65536:
                        return
                head, _, buffer = buffer.partition(b"\r\n\r\n")
                first = head.split(b"\r\n", 1)[0].split()
                path = first[1].decode() if len(first) > 1 else "/"
                length = _content_length(head)
                while len(buffer) < length:
                    more = await reader.read(length - len(buffer))
                    if not more:
                        return
                    buffer += more
                buffer = buffer[length:]
                backfill = path.startswith("/backfill")
                if backfill:
                    self.backfill_now += 1
                    self.backfill_max = max(
                        self.backfill_max, self.backfill_now
                    )
                try:
                    await asyncio.sleep(self._delay)
                finally:
                    if backfill:
                        self.backfill_now -= 1
                writer.write(
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    b"Content-Length: 2\r\n"
                    b"Connection: keep-alive\r\n"
                    b"\r\n{}"
                )
                await writer.drain()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                return


def _content_length(head: bytes) -> int:
    for line in head.split(b"\r\n")[1:]:
        if line.lower().startswith(b"content-length:"):
            return int(line.split(b":", 1)[1].strip() or b"0")
    return 0


async def _sample(client, path: str, n: int) -> list[float]:
    samples: list[float] = []
    for _ in range(n):
        started = time.perf_counter()
        response = await client.get(path)
        response.raise_for_status()
        samples.append(time.perf_counter() - started)
    return samples


async def test_order_latency_stays_within_bound_while_backfill_hammers() -> None:
    server = _Server(_DELAY_S)
    await server.start()
    clock = FakeClock()
    worker = AccountWorker(7, venue="Bybit", clock=clock)
    await worker.resident.start(base_urls=(server.url,))
    try:
        pool = worker.resident.pool
        assert pool is not None
        orders = pool.client_for(server.url)
        backfill = worker.resident.backfill_client()
        assert backfill is not None
        assert backfill._pool_client is orders

        idle = await _sample(orders, "/order", _SAMPLES)

        async def hammer() -> None:
            await asyncio.gather(
                *[backfill.get("/backfill") for _ in range(_HAMMER)]
            )

        walk = asyncio.create_task(hammer())
        try:
            during = await _sample(orders, "/order", _SAMPLES)
            await walk
        finally:
            if not walk.done():
                walk.cancel()
                await asyncio.gather(walk, return_exceptions=True)
    finally:
        await worker.resident.close()
        await server.close()

    idle_p50 = _percentile(idle, 0.50)
    idle_p99 = _percentile(idle, 0.99)
    during_p50 = _percentile(during, 0.50)
    during_p99 = _percentile(during, 0.99)
    print(
        "backfill order latency: "
        f"idle p50={idle_p50 * 1000:.1f}ms p99={idle_p99 * 1000:.1f}ms; "
        f"during p50={during_p50 * 1000:.1f}ms p99={during_p99 * 1000:.1f}ms; "
        f"backfill_max_in_flight={server.backfill_max}; "
        f"n={_SAMPLES}; server_delay={_DELAY_S * 1000:.0f}ms; "
        f"bound={_BOUND}x"
    )
    assert server.backfill_max <= BACKFILL_MAX_CONNECTIONS
    assert server.backfill_max >= 1
    assert during_p50 <= idle_p50 * _BOUND
    assert during_p99 <= idle_p99 * _BOUND
