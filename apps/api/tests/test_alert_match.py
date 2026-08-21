"""Source resolution and the match worker. Judging is ALT-5."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from mftik.protocol import Envelope
from mftik_api import alert_match, log_persist
from mftik_api.alert_eval import Hit
from mftik_api.alert_match import (
    AlertRec,
    Graph,
    MatcherRec,
    MatchRuntime,
    SourceRec,
    TypeCache,
    ingest,
    run_alert_match,
)


def _graph(*, td: bool = False, sts: bool = False, wildcard_sts: bool = False) -> Graph:
    sources = []
    if td:
        sources.append(SourceRec(id=1, domain="td", selector="12"))
        sources.append(SourceRec(id=2, domain="td", selector="*"))
        sources.append(SourceRec(id=3, domain="td", selector="99"))
    if sts:
        sources.append(SourceRec(id=10, domain="sts", selector="private::Tiny"))
        sources.append(SourceRec(id=11, domain="sts", selector="CrossArb"))
    if wildcard_sts:
        sources.append(SourceRec(id=12, domain="sts", selector="*"))
    matchers = {
        100: MatcherRec(id=100, name="any", kind="level", spec={"levels": ["info"]}),
        101: MatcherRec(id=101, name="other", kind="level", spec={"levels": ["info"]}),
    }
    alerts = {
        1: AlertRec(id=1, name="ops", enabled=True),
        2: AlertRec(id=2, name="signals", enabled=True),
        3: AlertRec(id=3, name="wrong", enabled=True),
    }
    source_to_matchers = {}
    matcher_to_alerts = {100: [1], 101: [3]}
    if td:
        source_to_matchers[1] = [100]
        source_to_matchers[2] = [100]
        source_to_matchers[3] = [101]
    if sts:
        source_to_matchers[10] = [100]
        source_to_matchers[11] = [101]
    if wildcard_sts:
        source_to_matchers[12] = [100]
    return Graph(
        sources=sources,
        matchers=matchers,
        alerts=alerts,
        source_to_matchers=source_to_matchers,
        matcher_to_alerts=matcher_to_alerts,
    )


async def _accept_all(
    _line: dict[str, Any],
    candidates: list[MatcherRec],
    _runtime=None,
) -> list[Hit]:
    return [Hit(matcher) for matcher in candidates]


def _log(topic: str, message: str = "hello", **payload: object) -> Envelope[dict]:
    body = {"level": "info", "message": message, **payload}
    stream = topic.split(".", 2)[2]
    return Envelope[dict].wrap(body, type="log", source="test", session_id=stream)


async def test_td_line_injects_only_wired_alerts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(alert_match, "evaluate", _accept_all)
    runtime = MatchRuntime()
    runtime.graph = _graph(td=True)
    await ingest(runtime, "log.td.12", _log("log.td.12"))
    assert [e.message for e in runtime.pending_events(1)] == ["hello"]
    assert runtime.pending_events(3) == []


async def test_sts_payload_type_selects_the_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(alert_match, "evaluate", _accept_all)
    runtime = MatchRuntime()
    runtime.graph = _graph(sts=True, wildcard_sts=True)
    env = Envelope[dict].wrap(
        {"level": "info", "message": "hi", "type": "private::Tiny"},
        type="log",
        source="sts",
        session_id="deadbeef" * 4,
    )
    await ingest(runtime, f"log.sts.{env.session_id}", env)
    assert runtime.pending_events(1)
    assert runtime.pending_events(3) == []


async def test_sts_type_from_db_is_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(alert_match, "evaluate", _accept_all)
    calls = {"n": 0}

    async def lookup(session_id: str) -> str | None:
        calls["n"] += 1
        return "CrossArb"

    monkeypatch.setattr(alert_match, "lookup_session_type", lookup)
    runtime = MatchRuntime()
    runtime.graph = _graph(sts=True)
    env = Envelope[dict].wrap(
        {"level": "info", "message": "hi"},
        type="log",
        source="sts",
        session_id="sess-1",
    )
    await ingest(runtime, "log.sts.sess-1", env)
    await ingest(runtime, "log.sts.sess-1", env)
    assert calls["n"] == 1
    assert runtime.lookup_calls == 1
    assert runtime.pending_events(3)
    assert runtime.pending_events(1) == []


async def test_cached_null_is_not_a_miss(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(alert_match, "evaluate", _accept_all)
    calls = {"n": 0}

    async def lookup(session_id: str) -> str | None:
        calls["n"] += 1
        return None

    monkeypatch.setattr(alert_match, "lookup_session_type", lookup)
    runtime = MatchRuntime()
    runtime.graph = _graph(sts=True, wildcard_sts=True)
    env = Envelope[dict].wrap(
        {"level": "info", "message": "hi"},
        type="log",
        source="sts",
        session_id="sess-null",
    )
    await ingest(runtime, "log.sts.sess-null", env)
    await ingest(runtime, "log.sts.sess-null", env)
    assert calls["n"] == 1
    # Wildcard still applies; the type-bound Source does not.
    assert runtime.pending_events(1)
    assert runtime.pending_events(3) == []


async def test_unknown_kind_does_not_inject() -> None:
    runtime = MatchRuntime()
    runtime.graph = _graph(td=True)
    runtime.graph.matchers[100] = MatcherRec(
        id=100, name="owner", kind="python", spec={}
    )
    await ingest(runtime, "log.td.12", _log("log.td.12"))
    assert runtime.pending_events(1) == []
    assert runtime.pending_events(3) == []


async def test_worker_does_not_drain_the_ring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetched = {"n": 0}

    class FakeBroker:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def fetch_log_buffer(self, _topic: str):
            fetched["n"] += 1
            return []

        async def psubscribe(self, _pattern: str, *, stop: asyncio.Event):
            stop.set()
            if False:
                yield None

    async def no_graph() -> Graph:
        return Graph()

    monkeypatch.setattr(alert_match, "Broker", FakeBroker)
    monkeypatch.setattr(alert_match, "load_graph", no_graph)
    stop = asyncio.Event()
    await run_alert_match(stop)
    assert fetched["n"] == 0


async def test_killing_the_match_worker_does_not_block_persist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    flushed: list[list[dict[str, Any]]] = []

    async def fake_flush(rows: list[dict[str, Any]]) -> None:
        flushed.append(list(rows))

    monkeypatch.setattr(log_persist, "flush_rows", fake_flush)
    monkeypatch.setenv("LOG_PERSIST_BATCH_SIZE", "1")
    monkeypatch.setenv("LOG_PERSIST_FLUSH_INTERVAL", "30")

    class FakeBroker:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def psubscribe(self, _pattern: str, *, stop: asyncio.Event):
            env = Envelope[dict].wrap(
                {"level": "info", "message": "kept"},
                type="log",
                source="sts",
                session_id="s1",
            )
            yield "log.sts.s1", env
            stop.set()

    monkeypatch.setattr(log_persist, "Broker", FakeBroker)
    match_stop = asyncio.Event()
    match_stop.set()
    persist_stop = asyncio.Event()
    await log_persist.run_log_persist(persist_stop)
    assert flushed
    assert flushed[0][0]["message"] == "kept"


def test_type_cache_sweeps_expired_on_put() -> None:
    cache = TypeCache(ttl=0.01, max_size=2)
    cache.put("old", "gone")
    cache.put("keep", "private::Tiny")
    time.sleep(0.02)
    cache.put("next", "CrossArb")
    hit, value = cache.get("old")
    assert hit is False
    assert "old" not in cache._data
    assert cache.get("next") == (True, "CrossArb")
