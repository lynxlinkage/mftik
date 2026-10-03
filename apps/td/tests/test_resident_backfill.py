"""Backfill on the resident layer (B6-05, F35).

No broker and no database. The executor is a stand-in; the HTTP cap is
an in-process transport. The latency numbers are
``test_backfill_latency.py`` (integration).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
from mftik.broker import NoRespondersError, RequestTimeoutError
from mftik.broker.request import IncomingRequest
from mftik.clock import FakeClock
from mftik.exchange.bybit.protocol import BYBIT_REST_URL
from mftik.protocol import (
    TD_BACKFILL,
    TD_BACKFILL_RESULT,
    TD_INTENT_DELETE,
    TD_INTENT_PUT,
    Envelope,
    TdBackfill,
    TdBackfillResult,
    Topics,
)
from mftik_td.account import AccountWorker
from mftik_td.account.handlers import account_subject_handler
from mftik_td.account.resident import BACKFILL_MAX_CONNECTIONS
from mftik_td.backfill.executor import BackfillExecutor
from mftik_td.backfill.reader import HistoryReaderFactory, NoHistoryReaderError
from mftik_td.backfill.session import BackfillSession, in_flight_reason
from mftik_td.controller import TdIntentBook, intent_handler

API = 7


class _Run:
    def __init__(self) -> None:
        self.calls: list[tuple[int, tuple[str, ...], str, object]] = []
        self.gate: asyncio.Event | None = None
        self.started = asyncio.Event()

    async def run(self, api_id, *, tickers=(), reason="", client=None):
        self.calls.append((api_id, tuple(tickers), reason, client))
        self.started.set()
        if self.gate is not None:
            await self.gate.wait()


class _PaperConnector:
    def __init__(self) -> None:
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False


class _Broker:
    def __init__(self, *, reply=None, error: Exception | None = None) -> None:
        self.reply = reply
        self.error = error
        self.asked: list[tuple[str, object, float | None]] = []
        self.sent: list[tuple[str, object]] = []

    async def request(self, subject, envelope, timeout=None):
        self.asked.append((subject, envelope, timeout))
        if self.error is not None:
            raise self.error
        return self.reply

    async def _send_reply(self, reply_to, envelope) -> None:
        self.sent.append((reply_to, envelope))


def _ask(api_id: int = API, **over: object) -> Envelope[TdBackfill]:
    body: dict[str, object] = {"api_id": api_id, "reason": "cron"}
    body.update(over)
    return Envelope[TdBackfill].wrap(
        TdBackfill.model_validate(body),
        type=TD_BACKFILL,
        source="test",
    )


def _result(envelope: object) -> TdBackfillResult:
    payload = getattr(envelope, "payload", None)
    return TdBackfillResult.model_validate(payload)


async def _spin(ready, turns: int = 8) -> None:
    for _ in range(turns):
        if ready():
            return
        await asyncio.sleep(0)
    assert ready()


async def test_an_account_with_no_session_backfills_on_the_pool() -> None:
    """The trading layer stays off. The reader is handed the pool client."""
    clock = FakeClock()
    run = _Run()
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        del request
        calls += 1
        return httpx.Response(200, json={})

    worker = AccountWorker(API, venue="Bybit", clock=clock, backfill=run)
    assert worker.trading.session is None
    assert worker.trading.active is False
    await worker.resident.start(
        base_urls=(BYBIT_REST_URL,),
        transport=httpx.MockTransport(handler),
    )
    try:
        reply = await account_subject_handler(worker)(
            _ask(reason="detach", tickers=["Bybit_Spot_BTCUSDT"])
        )
        assert reply is not None
        assert reply.type == TD_BACKFILL_RESULT
        assert _result(reply).ok is True
        assert _result(reply).reason == "accepted"
        await _spin(lambda: bool(run.calls))
        api_id, tickers, reason, client = run.calls[0]
        assert (api_id, tickers, reason) == (
            API,
            ("Bybit_Spot_BTCUSDT",),
            "detach",
        )
        pool = worker.resident.pool
        assert pool is not None
        assert client is not None
        assert client._pool_client is pool.client_for(BYBIT_REST_URL)
        await client.get("/v5/market/time")
        await client.aclose()
        await pool.client_for(BYBIT_REST_URL).get("/v5/market/time")
        assert calls >= 2
        assert worker.trading.active is False
        assert worker.resident.pool is pool
    finally:
        await worker.resident.close()


async def test_a_second_request_reuses_the_in_flight_refusal() -> None:
    run = _Run()
    run.gate = asyncio.Event()
    connector = _PaperConnector()
    worker = AccountWorker(
        API, venue="Paper", private=connector, backfill=run
    )
    await worker.resident.start()
    try:
        first = await worker.resident.handle_backfill(_ask())
        assert _result(first).reason == "accepted"
        await _spin(run.started.is_set)
        second = await worker.resident.handle_backfill(_ask(reason="cron"))
        assert _result(second).ok is False
        assert _result(second).reason == in_flight_reason(1)
        assert "already in flight" in _result(second).reason
        assert len(run.calls) == 1
    finally:
        run.gate.set()
        await worker.resident.close()


async def test_close_cancels_a_walk_that_has_not_finished() -> None:
    run = _Run()
    run.gate = asyncio.Event()
    connector = _PaperConnector()
    worker = AccountWorker(
        API, venue="Paper", private=connector, backfill=run
    )
    await worker.resident.start()
    await worker.resident.handle_backfill(_ask())
    await _spin(run.started.is_set)
    await worker.resident.close()
    assert worker.resident.pool is None
    assert connector.connected is False


async def test_backfill_holds_at_most_two_http_requests() -> None:
    """Orders are not on this cap. The wrapper is."""
    gate = _HttpGate()
    clock = FakeClock()
    worker = AccountWorker(API, venue="Bybit", clock=clock)
    await worker.resident.start(
        base_urls=("http://pool.test",),
        transport=httpx.ASGITransport(app=gate),
    )
    client = worker.resident.backfill_client()
    assert client is not None
    tasks = [
        asyncio.create_task(client.get("/backfill"))
        for _ in range(BACKFILL_MAX_CONNECTIONS + 4)
    ]
    try:
        await gate.reached.wait()
        assert gate.entered == BACKFILL_MAX_CONNECTIONS
        assert gate.max_entered == BACKFILL_MAX_CONNECTIONS
    finally:
        gate.release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await worker.resident.close()


async def test_a_reader_uses_the_given_client_and_does_not_close_it() -> None:
    """A stand-in, not a live client: opening one is past the unit cap."""

    class _Client:
        def __init__(self) -> None:
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True

    client = _Client()
    factory = HistoryReaderFactory(symbols=None)  # type: ignore[arg-type]
    reader = await factory.create(
        "Bybit",
        SimpleNamespace(api_key="k", api_secret="s", passphrase=None),
        client=client,
    )
    assert reader.rest._client is client
    assert reader.rest._owns_client is False
    await reader.close()
    assert client.closed is False
    row = SimpleNamespace(api_key="k", api_secret="s", passphrase=None)
    for venue in ("Paper", "Deribit", "Bitget"):
        try:
            await factory.create(venue, row)
        except NoHistoryReaderError:
            continue
        raise AssertionError(f"{venue} should have no history reader")


async def test_the_executor_hands_the_pool_client_to_the_factory() -> None:
    seen: list[object] = []

    class _Factory:
        async def create(self, venue, row, client=None):
            del venue, row
            seen.append(client)
            raise NoHistoryReaderError("stop before the database")

    async def load_api(api_id: int):
        return SimpleNamespace(
            id=api_id, venue="Bybit", api_key="k", api_secret="s"
        )

    marker = object()
    executor = BackfillExecutor(
        broker=object(),  # type: ignore[arg-type]
        factory=_Factory(),
        load_api=load_api,
        page_pause=0,
    )
    outcome = await executor.run(API, reason="cron", client=marker)
    assert seen == [marker]
    assert outcome.ok is True
    assert "stop before the database" in outcome.reason


async def test_no_worker_falls_back_and_a_worker_reply_is_returned() -> None:
    executor = _Run()
    missing = _Broker(error=NoRespondersError("td.account.7", "id", 0.2))
    session = BackfillSession(missing, executor, forward_timeout=0.2)  # type: ignore[arg-type]
    await session._handle(_incoming(missing, _ask()))
    await _spin(lambda: bool(executor.calls))
    assert executor.calls[0][0] == API
    assert missing.asked[0][0] == Topics.td_account(API)
    accepted = _result(missing.sent[0][1])
    assert accepted.ok is True
    assert accepted.reason == "accepted"

    busy = Envelope[TdBackfillResult].wrap(
        TdBackfillResult(
            api_id=API, ok=False, reason=in_flight_reason(1)
        ),
        type=TD_BACKFILL_RESULT,
        source="td",
    )
    present = _Broker(reply=busy)
    other = _Run()
    session = BackfillSession(present, other, forward_timeout=0.2)  # type: ignore[arg-type]
    await session._handle(_incoming(present, _ask()))
    await asyncio.sleep(0)
    assert other.calls == []
    forwarded = _result(present.sent[0][1])
    assert forwarded.ok is False
    assert forwarded.reason == in_flight_reason(1)

    timed_out = _Broker(
        error=RequestTimeoutError("td.account.7", "id", 0.2)
    )
    session = BackfillSession(timed_out, _Run(), forward_timeout=0.2)  # type: ignore[arg-type]
    await session._handle(_incoming(timed_out, _ask()))
    await asyncio.sleep(0)
    refused = _result(timed_out.sent[0][1])
    assert refused.ok is False
    assert refused.reason == "account worker did not answer"


def _incoming(broker: _Broker, envelope: Envelope[TdBackfill]) -> IncomingRequest:
    addressed = envelope.model_copy(update={"reply_to": "inbox"})
    return IncomingRequest(broker, addressed)  # type: ignore[arg-type]


def _intent(type_: str, payload: dict[str, object]) -> Envelope[dict[str, object]]:
    session_id = payload.get("session_id")
    return Envelope[dict[str, object]].wrap(
        payload,
        type=type_,
        source="api",
        session_id=session_id if isinstance(session_id, str) else None,
    )


def _owner(session_id: str = "abc") -> dict[str, str]:
    return {"sts_instance": "sts", "session_id": session_id}


class _DetachBroker:
    def __init__(self) -> None:
        self.seen: list[TdBackfill] = []
        self.release = asyncio.Event()
        self.started = asyncio.Event()

    async def request(self, subject, envelope, timeout=None):
        del timeout
        assert subject == Topics.td_backfill("td")
        payload = TdBackfill.model_validate(envelope.payload)
        self.seen.append(payload)
        self.started.set()
        await self.release.wait()
        return Envelope[dict[str, object]].wrap(
            {"ok": True, "api_id": payload.api_id},
            type=TD_BACKFILL_RESULT,
            source="td",
        )


async def test_the_last_intent_asks_for_a_detach_backfill_without_blocking() -> None:
    book = TdIntentBook()
    broker = _DetachBroker()
    handler = intent_handler(book, broker=broker, instance="td")  # type: ignore[arg-type]
    await handler(
        _intent(
            TD_INTENT_PUT,
            {
                "session_id": "abc",
                "owner": _owner(),
                "api_ids": [API, 8],
            },
        )
    )
    reply = await handler(
        _intent(
            TD_INTENT_DELETE,
            {"session_id": "abc", "owner": _owner(), "api_ids": []},
        )
    )
    # The reply is already here. The ask is still waiting on ``release``.
    assert reply is not None
    assert reply.type == TD_INTENT_DELETE
    broker.release.set()
    await _spin(lambda: len(broker.seen) == 2)
    assert sorted((row.api_id, row.reason) for row in broker.seen) == [
        (API, "detach"),
        (8, "detach"),
    ]


async def test_a_delete_that_leaves_the_account_held_does_not_ask() -> None:
    book = TdIntentBook()
    broker = _DetachBroker()
    handler = intent_handler(book, broker=broker, instance="td")  # type: ignore[arg-type]
    await handler(
        _intent(
            TD_INTENT_PUT,
            {"session_id": "abc", "owner": _owner(), "api_ids": [API]},
        )
    )
    await handler(
        _intent(
            TD_INTENT_PUT,
            {
                "session_id": "def",
                "owner": _owner("def"),
                "api_ids": [API],
            },
        )
    )
    await handler(
        _intent(
            TD_INTENT_DELETE,
            {"session_id": "abc", "owner": _owner(), "api_ids": []},
        )
    )
    for _ in range(4):
        await asyncio.sleep(0)
    assert broker.seen == []


async def test_a_put_does_not_ask_even_when_it_drops_an_account() -> None:
    book = TdIntentBook()
    broker = _DetachBroker()
    handler = intent_handler(book, broker=broker, instance="td")  # type: ignore[arg-type]
    await handler(
        _intent(
            TD_INTENT_PUT,
            {"session_id": "abc", "owner": _owner(), "api_ids": [API, 8]},
        )
    )
    await handler(
        _intent(
            TD_INTENT_PUT,
            {"session_id": "abc", "owner": _owner(), "api_ids": [8]},
        )
    )
    for _ in range(4):
        await asyncio.sleep(0)
    assert broker.seen == []


class _HttpGate:
    def __init__(self) -> None:
        self.entered = 0
        self.max_entered = 0
        self.release = asyncio.Event()
        self.reached = asyncio.Event()

    async def __call__(self, scope, receive, send) -> None:
        del receive
        path = scope.get("path") or ""
        if path != "/backfill":
            await _http_ok(send)
            return
        self.entered += 1
        self.max_entered = max(self.max_entered, self.entered)
        if self.entered >= BACKFILL_MAX_CONNECTIONS:
            self.reached.set()
        try:
            await self.release.wait()
        finally:
            self.entered -= 1
        await _http_ok(send)


async def _http_ok(send) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": b"{}"})
