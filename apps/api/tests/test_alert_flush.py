"""Quiesce window: one POST, then clear. The stub sees the URL, logs do not."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
from mftik.protocol import Envelope
from mftik_api import alert_match
from mftik_api.alert_discord import set_http_client
from mftik_api.alert_eval import Hit
from mftik_api.alert_match import (
    AlertRec,
    Graph,
    MatcherRec,
    MatchRuntime,
    SourceRec,
    flush_alert,
    ingest,
    render_embed,
)

HOOK = "https://discord.com/api/webhooks/1/super-secret-token"


def _alert(**overrides: object) -> AlertRec:
    payload: dict[str, object] = {
        "id": 1,
        "name": "ops",
        "enabled": True,
        "webhook_url": HOOK,
        "flush_interval_s": 30,
        "max_events_in_payload": 15,
        "max_buffer_events": 200,
        "dedupe": True,
    }
    payload.update(overrides)
    return AlertRec(**payload)  # type: ignore[arg-type]


def _graph(alert: AlertRec, *, matchers: int = 1) -> Graph:
    sources = [SourceRec(id=1, domain="td", selector="12")]
    recs = {
        i: MatcherRec(id=i, name=f"m{i}", kind="level", spec={"levels": ["info"]})
        for i in range(100, 100 + matchers)
    }
    return Graph(
        sources=sources,
        matchers=recs,
        alerts={alert.id: alert},
        source_to_matchers={1: list(recs)},
        matcher_to_alerts={mid: [alert.id] for mid in recs},
    )


async def _accept_all(
    _line: dict[str, Any],
    candidates: list[MatcherRec],
    _runtime=None,
) -> list[Hit]:
    return [Hit(matcher) for matcher in candidates]


def _log(message: str, envelope_id: str | None = None) -> Envelope[dict]:
    env = Envelope[dict].wrap(
        {"level": "info", "message": message},
        type="log",
        source="td",
        session_id="12",
    )
    if envelope_id is not None:
        object.__setattr__(env, "id", envelope_id)
    return env


@pytest.fixture
def deliveries() -> list[dict[str, Any]]:
    return []


@pytest.fixture
async def runtime(
    monkeypatch: pytest.MonkeyPatch, deliveries: list[dict[str, Any]]
) -> MatchRuntime:
    async def capture(**kwargs: object) -> None:
        deliveries.append(dict(kwargs))

    monkeypatch.setattr(alert_match, "evaluate", _accept_all)
    monkeypatch.setattr(alert_match, "record_delivery", capture)
    return MatchRuntime()


async def test_three_lines_one_post(
    runtime: MatchRuntime, deliveries: list[dict[str, Any]]
) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(204)

    runtime.graph = _graph(_alert(dedupe=False))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        set_http_client(client)
        try:
            for i in range(3):
                await ingest(runtime, "log.td.12", _log(f"line-{i}"))
            await flush_alert(runtime, 1)
        finally:
            set_http_client(None)

    assert seen == [HOOK]
    assert deliveries[0]["event_count"] == 3
    assert runtime.pending_events(1) == []


async def test_two_matchers_one_envelope_folds(
    runtime: MatchRuntime, deliveries: list[dict[str, Any]]
) -> None:
    runtime.graph = _graph(_alert(dedupe=False), matchers=2)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        set_http_client(client)
        try:
            await ingest(runtime, "log.td.12", _log("shared"))
            await flush_alert(runtime, 1)
        finally:
            set_http_client(None)

    assert len(runtime.pending_events(1)) == 0
    assert deliveries[0]["event_count"] == 1


async def test_dedupe_identical_messages(
    runtime: MatchRuntime, deliveries: list[dict[str, Any]]
) -> None:
    runtime.graph = _graph(_alert(dedupe=True))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(204))
    ) as client:
        set_http_client(client)
        try:
            for _ in range(3):
                await ingest(runtime, "log.td.12", _log("warn: disconnected"))
            await flush_alert(runtime, 1)
        finally:
            set_http_client(None)

    assert deliveries[0]["event_count"] == 1


async def test_nearby_values_do_not_dedupe(
    runtime: MatchRuntime, deliveries: list[dict[str, Any]]
) -> None:
    runtime.graph = _graph(_alert(dedupe=True))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(204))
    ) as client:
        set_http_client(client)
        try:
            await ingest(runtime, "log.td.12", _log("risk value = 0.995"))
            await ingest(runtime, "log.td.12", _log("risk value = 0.996"))
            await flush_alert(runtime, 1)
        finally:
            set_http_client(None)

    assert deliveries[0]["event_count"] == 2


async def test_plus_n_more_and_clear(
    runtime: MatchRuntime, deliveries: list[dict[str, Any]]
) -> None:
    bodies: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(204)

    runtime.graph = _graph(_alert(dedupe=False, max_events_in_payload=15))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        set_http_client(client)
        try:
            for i in range(16):
                await ingest(runtime, "log.td.12", _log(f"msg-{i}"))
            await flush_alert(runtime, 1)
        finally:
            set_http_client(None)

    description = bodies[0]["embeds"][0]["description"]
    assert "+1 more" in description
    assert runtime.pending_events(1) == []
    assert deliveries[0]["event_count"] == 16


async def test_buffer_cap_increments_dropped(
    runtime: MatchRuntime, deliveries: list[dict[str, Any]]
) -> None:
    runtime.graph = _graph(_alert(dedupe=False, max_buffer_events=2))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(204))
    ) as client:
        set_http_client(client)
        try:
            for i in range(5):
                await ingest(runtime, "log.td.12", _log(f"n-{i}"))
            await flush_alert(runtime, 1)
        finally:
            set_http_client(None)

    assert deliveries[0]["dropped_count"] == 3
    assert deliveries[0]["event_count"] == 2


async def test_disabled_alert_does_not_post(runtime: MatchRuntime) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(204)

    runtime.graph = _graph(_alert(enabled=False))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        set_http_client(client)
        try:
            await ingest(runtime, "log.td.12", _log("nope"))
            await flush_alert(runtime, 1)
        finally:
            set_http_client(None)

    assert seen == []
    assert runtime.pending_events(1) == []


async def test_429_is_recorded_and_matching_continues(
    runtime: MatchRuntime, deliveries: list[dict[str, Any]]
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429)

    runtime.graph = _graph(_alert(dedupe=False))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        set_http_client(client)
        try:
            await ingest(runtime, "log.td.12", _log("a"))
            await flush_alert(runtime, 1)
            await ingest(runtime, "log.td.12", _log("b"))
        finally:
            set_http_client(None)

    assert deliveries[0]["http_status"] == 429
    assert deliveries[0]["error"] == "discord returned 429"
    assert len(runtime.pending_events(1)) == 1


async def test_timer_flushes_once(
    monkeypatch: pytest.MonkeyPatch, deliveries: list[dict[str, Any]]
) -> None:
    async def capture(**kwargs: object) -> None:
        deliveries.append(dict(kwargs))

    monkeypatch.setattr(alert_match, "evaluate", _accept_all)
    monkeypatch.setattr(alert_match, "record_delivery", capture)
    runtime = MatchRuntime(arm_timers=True)
    runtime.graph = _graph(_alert(flush_interval_s=0.05, dedupe=False))
    posted = asyncio.Event()

    def handler(_request: httpx.Request) -> httpx.Response:
        posted.set()
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        set_http_client(client)
        try:
            await ingest(runtime, "log.td.12", _log("tick"))
            await asyncio.wait_for(posted.wait(), timeout=1)
        finally:
            set_http_client(None)

    assert deliveries[0]["event_count"] == 1
    assert runtime.pending_events(1) == []


def test_render_clips_to_2000() -> None:
    alert = _alert(max_events_in_payload=15)
    events = [
        alert_match.MatchEvent(
            domain="td",
            stream_id="12",
            session_id="12",
            type=None,
            source="td",
            level="info",
            message="x" * 400,
            envelope_id=str(i),
            ts=0.0,
            matcher_id=1,
        )
        for i in range(15)
    ]
    payload = render_embed(alert, events, 0)
    assert len(payload["embeds"][0]["description"]) <= 2000
