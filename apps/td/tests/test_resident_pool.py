"""R1–R5 for a non-paper resident layer (B6-01).

In-process. The venue is Bybit behind :class:`httpx.MockTransport`, and
the clock is a :class:`~mftik.clock.FakeClock`. Paper still has no pool:
the contract test that asks a paper worker for one stays ``xfail``
until B6-02, because that test also switches the trading layer.

A backfill that is not a ``TdBackfill`` is refused and does not replace
the pool. The pool still has more than one connection, which is what
leaves room beside :data:`BACKFILL_MAX_CONNECTIONS`.
"""

from __future__ import annotations

import asyncio
import logging

import httpx
import pytest
from mftik.clock import FakeClock
from mftik.exchange.bybit.protocol import BYBIT_REST_URL
from mftik.exchange.bybit.rest import BybitPublicRest, BybitRest
from mftik.exchange.keepalive import for_venue
from mftik.protocol import (
    TD_BACKFILL_RESULT,
    TD_ORDER_CANCEL_SESSION,
    Envelope,
    TdBackfillResult,
)
from mftik_td.account import AccountWorker
from mftik_td.account._ticket import TICKET
from mftik_td.oms import Ledger, Oms

_BODY = {
    "retCode": 0,
    "retMsg": "OK",
    "result": {"timeSecond": "1700000000", "timeNano": "1700000000000000000"},
    "time": 1_700_000_000_000,
}


class _Script:
    def __init__(self) -> None:
        self.fail = False
        self.calls = 0
        self.attempts = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        del request
        self.attempts += 1
        if self.fail:
            raise httpx.ConnectError("keepalive down")
        self.calls += 1
        return httpx.Response(200, json=_BODY)


class _Probe:
    def __init__(self) -> None:
        self.pings = 0

    async def __call__(self) -> None:
        self.pings += 1


class _Book:
    """Enough of a session for the trading layer to switch. No venue."""

    def __init__(self) -> None:
        self.oms = Oms()
        self.ledger = Ledger()
        self.private = object()
        self.starts = 0
        self.destroys = 0

    async def start(self) -> None:
        self.starts += 1

    async def destroy(self) -> None:
        self.destroys += 1


class _PaperConnector:
    def __init__(self) -> None:
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False


async def _spin(ready, turns: int = 8) -> None:
    for _ in range(turns):
        if ready():
            return
        await asyncio.sleep(0)
    assert ready()


def _worker(
    clock: FakeClock,
    *,
    keepalive: _Probe | None = None,
    session: _Book | None = None,
) -> AccountWorker:
    return AccountWorker(
        7,
        venue="Bybit",
        keepalive=keepalive,
        clock=clock,
        session=session,
    )


async def test_r1_an_account_with_no_session_still_warms_a_pool() -> None:
    clock = FakeClock()
    script = _Script()
    worker = _worker(clock)
    assert worker.trading.active is False
    await worker.resident.start(transport=httpx.MockTransport(script.handler))
    try:
        assert worker.resident.started
        assert worker.resident.pool is not None
        assert worker.trading.active is False
        assert script.calls == 1
    finally:
        await worker.resident.close()


async def test_r2_the_trading_switch_keeps_the_pool_and_the_hook() -> None:
    clock = FakeClock()
    script = _Script()
    probe = _Probe()
    book = _Book()
    worker = _worker(clock, keepalive=probe, session=book)
    await worker.resident.start(transport=httpx.MockTransport(script.handler))
    try:
        pool = worker.resident.pool
        assert pool is not None
        # The injected hook is the request. The warm call already ran it.
        assert probe.pings == 1
        assert script.calls == 0

        await worker.trading.activate()
        assert worker.trading.active
        assert book.starts == 1
        await worker.resident.keepalive_once()

        await worker.trading.deactivate()
        assert worker.trading.active is False
        assert book.destroys == 1
        assert worker.resident.started
        assert worker.resident.pool is pool
        assert worker.resident.keepalive is probe
        await worker.resident.keepalive_once()
        assert probe.pings == 3
    finally:
        await worker.resident.close()


async def test_r3_a_refused_backfill_leaves_the_pool_with_room() -> None:
    spec = for_venue("Bybit")
    assert spec.limits.max_keepalive_connections is not None
    assert spec.limits.max_keepalive_connections > 1
    assert spec.limits.max_connections is not None
    assert spec.limits.max_connections > 2
    clock = FakeClock()
    script = _Script()
    worker = _worker(clock)
    await worker.resident.start(transport=httpx.MockTransport(script.handler))
    try:
        message = Envelope[dict[str, object]].wrap(
            {}, type=TD_ORDER_CANCEL_SESSION, source="test"
        )
        reply = await worker.resident.handle_backfill(message)
        assert reply is not None
        assert reply.type == TD_BACKFILL_RESULT
        assert TdBackfillResult.model_validate(reply.payload).ok is False
        pool = worker.resident.pool
        assert pool is not None
        client = pool.client_for(BYBIT_REST_URL)
        before = script.calls
        await asyncio.gather(
            client.get("/v5/market/time"),
            client.get("/v5/market/time"),
        )
        assert script.calls == before + 2
        assert worker.resident.pool is pool
    finally:
        await worker.resident.close()


async def test_r4_a_venue_rest_client_takes_the_pool_client() -> None:
    clock = FakeClock()
    script = _Script()
    worker = _worker(clock)
    await worker.resident.start(transport=httpx.MockTransport(script.handler))
    try:
        pool = worker.resident.pool
        assert pool is not None
        client = pool.client_for(BYBIT_REST_URL)
        public = BybitPublicRest(client=client)
        signed = BybitRest(api_key="k", api_secret="s", client=client)
        assert public._client is client
        assert signed._client is client
        assert public._owns_client is False
        assert signed._owns_client is False
        await public.server_time()
        assert script.calls >= 2
        # A client the REST object does not own stays open when the
        # REST object is closed. The pool is what closes it.
        await public.close()
        await signed.close()
        assert public._client is client
        await public.server_time()
    finally:
        await worker.resident.close()


async def test_r5_the_loop_sends_the_adapter_read_on_its_interval() -> None:
    spec = for_venue("Bybit")
    clock = FakeClock()
    script = _Script()
    worker = _worker(clock)
    await worker.resident.start(transport=httpx.MockTransport(script.handler))
    try:
        await asyncio.sleep(0)
        assert script.calls == 1
        assert worker.resident.keepalive is None
        clock.advance(spec.interval_s - 1)
        await asyncio.sleep(0)
        assert script.calls == 1
        clock.advance(1)
        await _spin(lambda: script.calls == 2)
        assert script.calls == 2
    finally:
        await worker.resident.close()


async def test_a_keepalive_failure_is_retried_and_does_not_stop_the_worker() -> None:
    spec = for_venue("Bybit")
    clock = FakeClock()
    script = _Script()
    script.fail = True
    worker = _worker(clock)
    await worker.resident.start(transport=httpx.MockTransport(script.handler))
    try:
        assert worker.resident.started
        assert worker.resident.pool is not None
        assert worker.trading.active is False
        assert script.calls == 0
        await asyncio.sleep(0)
        script.fail = False
        clock.advance(spec.interval_s)
        await _spin(lambda: script.calls == 1)
        assert worker.resident.started
        assert worker.trading.active is False
    finally:
        await worker.resident.close()


async def test_a_later_keepalive_failure_is_logged_and_the_next_tick_runs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    spec = for_venue("Bybit")
    clock = FakeClock()
    script = _Script()
    worker = _worker(clock)
    await worker.resident.start(transport=httpx.MockTransport(script.handler))
    try:
        await asyncio.sleep(0)
        script.fail = True
        with caplog.at_level(logging.ERROR):
            clock.advance(spec.interval_s)
            await _spin(lambda: script.attempts >= 2)
        assert worker.resident.started
        assert worker.trading.active is False
        assert any("keepalive failed" in rec.message for rec in caplog.records)
        script.fail = False
        clock.advance(spec.interval_s)
        await _spin(lambda: script.calls == 2)
    finally:
        await worker.resident.close()


async def test_close_stops_the_loop_and_drops_the_pool() -> None:
    spec = for_venue("Bybit")
    clock = FakeClock()
    script = _Script()
    worker = _worker(clock)
    await worker.resident.start(transport=httpx.MockTransport(script.handler))
    pool = worker.resident.pool
    await asyncio.sleep(0)
    await worker.resident.close()
    assert worker.resident.started is False
    assert worker.resident.pool is None
    clock.advance(spec.interval_s * 3)
    await asyncio.sleep(0)
    assert script.calls == 1
    await worker.resident.start(transport=httpx.MockTransport(script.handler))
    try:
        assert worker.resident.pool is not None
        assert worker.resident.pool is not pool
    finally:
        await worker.resident.close()


async def test_paper_still_has_no_pool() -> None:
    connector = _PaperConnector()
    worker = AccountWorker(7, venue="Paper", private=connector)
    await worker.resident.start()
    try:
        assert worker.resident.started
        assert connector.connected
        assert worker.resident.pool is None
        with pytest.raises(NotImplementedError, match=TICKET):
            await worker.resident.keepalive_once()
    finally:
        await worker.resident.close()
    assert connector.connected is False
