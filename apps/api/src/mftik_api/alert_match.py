"""Live ``log.*`` matcher. Resolves Sources; ALT-5 judges; ALT-6 fires."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from mftik.broker import Broker
from mftik.protocol import UntypedEnvelope
from mftik_db.repositories import (
    AlertMatcherRepository,
    AlertRepository,
    AlertSourceRepository,
    StsSessionRepository,
)
from mftik_db.session import session_scope

from mftik_api.alert_discord import post_webhook
from mftik_api.alert_eval import Hit, warm_patterns
from mftik_api.alert_eval import evaluate as judge
from mftik_api.log_persist import LOG_PATTERN, parse_log_topic

logger = logging.getLogger("mftik_api.alert_match")


@dataclass(frozen=True)
class SourceRec:
    id: int
    domain: str
    selector: str


@dataclass(frozen=True)
class MatcherRec:
    id: int
    name: str
    kind: str
    spec: dict[str, Any]


@dataclass(frozen=True)
class AlertRec:
    id: int
    name: str
    enabled: bool
    webhook_url: str = ""
    flush_interval_s: float = 30
    max_events_in_payload: int = 15
    max_buffer_events: int = 200
    dedupe: bool = True


@dataclass
class Graph:
    sources: list[SourceRec] = field(default_factory=list)
    matchers: dict[int, MatcherRec] = field(default_factory=dict)
    alerts: dict[int, AlertRec] = field(default_factory=dict)
    source_to_matchers: dict[int, list[int]] = field(default_factory=dict)
    matcher_to_alerts: dict[int, list[int]] = field(default_factory=dict)

    def sources_for(self, domain: str, keys: set[str]) -> list[SourceRec]:
        return [
            source
            for source in self.sources
            if source.domain == domain and source.selector in keys
        ]

    def successors(self, sources: list[SourceRec]) -> list[MatcherRec]:
        seen: set[int] = set()
        out: list[MatcherRec] = []
        for source in sources:
            for matcher_id in self.source_to_matchers.get(source.id, ()):
                if matcher_id in seen:
                    continue
                matcher = self.matchers.get(matcher_id)
                if matcher is None:
                    continue
                seen.add(matcher_id)
                out.append(matcher)
        return out


@dataclass
class MatchEvent:
    domain: str
    stream_id: str
    session_id: str | None
    type: str | None
    source: str
    level: str
    message: str
    envelope_id: str
    ts: float
    matcher_id: int
    captures: dict[str, Any] = field(default_factory=dict)


@dataclass
class Window:
    events: list[MatchEvent] = field(default_factory=list)
    dropped: int = 0
    started: float = 0.0
    task: asyncio.Task[None] | None = None


class TypeCache:
    """``session_id → type | None``. A cached null is a hit, not a miss."""

    def __init__(self, ttl: float = 30.0) -> None:
        self._ttl = ttl
        self._data: dict[str, tuple[str | None, float]] = {}

    def get(self, session_id: str) -> tuple[bool, str | None]:
        item = self._data.get(session_id)
        if item is None:
            return False, None
        value, expires = item
        if time.monotonic() >= expires:
            del self._data[session_id]
            return False, None
        return True, value

    def put(self, session_id: str, value: str | None) -> None:
        self._data[session_id] = (value, time.monotonic() + self._ttl)


class MatchRuntime:
    def __init__(self, *, arm_timers: bool = False) -> None:
        self.graph = Graph()
        self.cache = TypeCache(
            ttl=max(1.0, float(os.getenv("ALERT_TYPE_CACHE_TTL", "30")))
        )
        self.windows: dict[int, Window] = {}
        self.arm_timers = arm_timers
        self.lookup_calls = 0
        self.disabled: dict[int, str] = {}
        self.timeout_streak: dict[int, int] = {}

    def clear_disabled(self) -> None:
        self.disabled.clear()
        self.timeout_streak.clear()

    def pending_events(self, alert_id: int) -> list[MatchEvent]:
        window = self.windows.get(alert_id)
        return list(window.events) if window is not None else []


_runtime: MatchRuntime | None = None


def current_runtime() -> MatchRuntime | None:
    return _runtime


def _graph_poll_interval() -> float:
    return max(0.2, float(os.getenv("ALERT_GRAPH_POLL_INTERVAL", "2.0")))


async def load_graph() -> Graph:
    async with session_scope() as db:
        sources = [
            SourceRec(id=row.id, domain=row.domain, selector=row.selector)
            for row in await AlertSourceRepository(db).list_all()
        ]
        matchers = {
            row.id: MatcherRec(
                id=row.id, name=row.name, kind=row.kind, spec=dict(row.spec or {})
            )
            for row in await AlertMatcherRepository(db).list_all()
        }
        alerts = {
            row.id: AlertRec(
                id=row.id,
                name=row.name,
                enabled=row.enabled,
                webhook_url=row.webhook_url,
                flush_interval_s=float(row.flush_interval_s),
                max_events_in_payload=row.max_events_in_payload,
                max_buffer_events=row.max_buffer_events,
                dedupe=row.dedupe,
            )
            for row in await AlertRepository(db).list_all()
        }
        repo = AlertRepository(db)
        source_to_matchers: dict[int, list[int]] = defaultdict(list)
        for wire in await repo.list_source_matcher_wires():
            source_to_matchers[wire.source_id].append(wire.matcher_id)
        matcher_to_alerts: dict[int, list[int]] = defaultdict(list)
        for wire in await repo.list_matcher_alert_wires():
            matcher_to_alerts[wire.matcher_id].append(wire.alert_id)
    return Graph(
        sources=sources,
        matchers=matchers,
        alerts=alerts,
        source_to_matchers=dict(source_to_matchers),
        matcher_to_alerts=dict(matcher_to_alerts),
    )


async def lookup_session_type(session_id: str) -> str | None:
    async with session_scope() as db:
        row = await StsSessionRepository(db).get_by_session_id(session_id)
    return None if row is None else row.type


async def resolve_sts_type(
    runtime: MatchRuntime, session_id: str, payload_type: str | None
) -> str | None:
    if payload_type:
        runtime.cache.put(session_id, payload_type)
        return payload_type
    hit, cached = runtime.cache.get(session_id)
    if hit:
        return cached
    runtime.lookup_calls += 1
    value = await lookup_session_type(session_id)
    runtime.cache.put(session_id, value)
    return value


def line_from_envelope(
    topic: str, envelope: UntypedEnvelope
) -> dict[str, Any] | None:
    parsed = parse_log_topic(topic)
    if parsed is None:
        return None
    domain, stream_id = parsed
    payload = envelope.payload if isinstance(envelope.payload, dict) else {}
    message = payload.get("message")
    if message is None:
        return None
    raw_type = payload.get("type")
    return {
        "domain": domain,
        "stream_id": stream_id,
        "session_id": envelope.session_id or stream_id,
        "type": str(raw_type) if raw_type else None,
        "source": envelope.source or "unknown",
        "level": str(payload.get("level") or "info"),
        "message": str(message),
        "envelope_id": envelope.id,
        "ts": float(envelope.ts),
        "captures": {},
    }


async def evaluate(
    line: dict[str, Any],
    candidates: list[MatcherRec],
    runtime: MatchRuntime | None = None,
) -> list[Hit]:
    return await judge(line, candidates, runtime)


_LEVEL_RANK = {"info": 0, "warn": 1, "warning": 1, "error": 2}
_LEVEL_COLOR = {"error": 0xE74C3C, "warn": 0xF1C40F, "warning": 0xF1C40F}


def fold_events(events: list[MatchEvent], *, dedupe: bool) -> list[MatchEvent]:
    by_envelope: dict[str, MatchEvent] = {}
    order: list[str] = []
    for event in events:
        if event.envelope_id in by_envelope:
            continue
        by_envelope[event.envelope_id] = event
        order.append(event.envelope_id)
    folded = [by_envelope[key] for key in order]
    if not dedupe:
        return folded
    seen: set[str] = set()
    out: list[MatchEvent] = []
    for event in folded:
        if event.message in seen:
            continue
        seen.add(event.message)
        out.append(event)
    return out


def render_embed(
    alert: AlertRec, folded: list[MatchEvent], dropped: int
) -> dict[str, Any]:
    shown = folded[: alert.max_events_in_payload]
    extra = len(folded) - len(shown)
    lines: list[str] = []
    worst = "info"
    for event in shown:
        if _LEVEL_RANK.get(event.level.lower(), 0) > _LEVEL_RANK.get(worst, 0):
            worst = event.level.lower()
        loc = event.type or event.stream_id
        extras = ""
        if event.captures:
            extras = " " + ", ".join(f"{k}={v}" for k, v in event.captures.items())
        lines.append(f"{event.level} {loc} {event.message}{extras}")
    description = "\n".join(lines)
    if extra > 0:
        description += f"\n+{extra} more"
    if len(description) > 2000:
        description = description[:1997] + "..."
    return {
        "embeds": [
            {
                "title": alert.name,
                "description": description,
                "color": _LEVEL_COLOR.get(worst, 0x3498DB),
                "footer": {
                    "text": f"events={len(folded)} dropped={dropped}",
                },
            }
        ]
    }


async def record_delivery(
    *,
    alert_id: int,
    window_start: datetime,
    event_count: int,
    dropped_count: int,
    http_status: int | None,
    error: str | None,
    ts: datetime,
) -> None:
    async with session_scope() as db:
        await AlertRepository(db).record_delivery(
            alert_id=alert_id,
            window_start=window_start,
            event_count=event_count,
            dropped_count=dropped_count,
            http_status=http_status,
            error=error,
            ts=ts,
        )


def _arm(runtime: MatchRuntime, alert: AlertRec) -> None:
    if not runtime.arm_timers:
        return
    window = runtime.windows[alert.id]

    async def _wait() -> None:
        await asyncio.sleep(alert.flush_interval_s)
        await flush_alert(runtime, alert.id)

    window.task = asyncio.create_task(_wait())


def _buffer(runtime: MatchRuntime, alert: AlertRec, event: MatchEvent) -> None:
    window = runtime.windows.get(alert.id)
    if window is None:
        window = Window(started=time.time())
        runtime.windows[alert.id] = window
        _arm(runtime, alert)
    if len(window.events) >= alert.max_buffer_events:
        window.dropped += 1
        return
    window.events.append(event)


def _inject(runtime: MatchRuntime, line: dict[str, Any], hits: list[Hit]) -> None:
    for hit in hits:
        matcher = hit.matcher
        for alert_id in runtime.graph.matcher_to_alerts.get(matcher.id, ()):
            alert = runtime.graph.alerts.get(alert_id)
            if alert is None or not alert.enabled:
                continue
            event = MatchEvent(
                domain=line["domain"],
                stream_id=line["stream_id"],
                session_id=line.get("session_id"),
                type=line.get("type"),
                source=line["source"],
                level=line["level"],
                message=line["message"],
                envelope_id=line["envelope_id"],
                ts=line["ts"],
                matcher_id=matcher.id,
                captures=dict(hit.captures),
            )
            _buffer(runtime, alert, event)


async def flush_alert(runtime: MatchRuntime, alert_id: int) -> None:
    window = runtime.windows.pop(alert_id, None)
    if window is None:
        return
    if window.task is not None and not window.task.done():
        window.task.cancel()
    alert = runtime.graph.alerts.get(alert_id)
    if alert is None or not window.events:
        return
    folded = fold_events(window.events, dedupe=alert.dedupe)
    payload = render_embed(alert, folded, window.dropped)
    http_status: int | None = None
    error: str | None = None
    try:
        response = await post_webhook(alert.webhook_url, payload)
        http_status = response.status_code
        if response.status_code >= 400:
            error = f"discord returned {response.status_code}"
    except Exception as exc:
        error = exc.__class__.__name__
    now = datetime.now(UTC)
    started = datetime.fromtimestamp(window.started, tz=UTC)
    try:
        await record_delivery(
            alert_id=alert_id,
            window_start=started,
            event_count=len(folded),
            dropped_count=window.dropped,
            http_status=http_status,
            error=error,
            ts=now,
        )
    except Exception:
        logger.exception("alert delivery persist failed alert_id=%s", alert_id)


async def ingest(runtime: MatchRuntime, topic: str, envelope: UntypedEnvelope) -> None:
    line = line_from_envelope(topic, envelope)
    if line is None:
        return
    domain = line["domain"]
    if domain == "sts":
        line["type"] = await resolve_sts_type(
            runtime, line["session_id"], line["type"]
        )
        keys = {"*"}
        if line["type"]:
            keys.add(line["type"])
    else:
        keys = {line["stream_id"], "*"}
    sources = runtime.graph.sources_for(domain, keys)
    candidates = runtime.graph.successors(sources)
    if not candidates:
        return
    hits = await evaluate(line, candidates, runtime)
    _inject(runtime, line, hits)


async def run_alert_match(stop: asyncio.Event) -> None:
    """``psubscribe log.*`` and resolve Sources until stop.

    Does not drain ``fetch_log_buffer``. Does not import ``flush_rows``.
    Does not subscribe ``status.sts``.
    """
    global _runtime
    runtime = MatchRuntime(arm_timers=True)
    _runtime = runtime
    broker = Broker()
    await broker.connect()
    try:
        runtime.graph = await load_graph()
        warm_patterns(list(runtime.graph.matchers.values()))
        runtime.clear_disabled()
    except Exception:
        logger.exception("alert graph load failed; starting empty")
    logger.info("alert match worker started sources=%d", len(runtime.graph.sources))

    async def poll() -> None:
        interval = _graph_poll_interval()
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except TimeoutError:
                pass
            if stop.is_set():
                return
            try:
                runtime.graph = await load_graph()
                warm_patterns(list(runtime.graph.matchers.values()))
                runtime.clear_disabled()
            except Exception:
                logger.exception("alert graph refresh failed")

    poll_task = asyncio.create_task(poll())
    try:
        async for topic, envelope in broker.psubscribe(LOG_PATTERN, stop=stop):
            try:
                await ingest(runtime, topic, envelope)
            except Exception:
                logger.exception("alert match failed topic=%s", topic)
    finally:
        stop.set()
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass
        for window in runtime.windows.values():
            if window.task is not None:
                window.task.cancel()
        await broker.close()
        _runtime = None
        logger.info("alert match worker stopped")
