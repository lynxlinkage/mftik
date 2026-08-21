"""ALT-8: S1–S14 against the real Alert routes and match worker."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from auth_harness import a_client, an_api, use_database
from db_harness import a_database, an_owner
from fastapi import FastAPI
from mftik.protocol import Envelope
from mftik_api import alert_eval, alert_match, log_persist
from mftik_api.alert_discord import set_http_client
from mftik_api.alert_match import MatchRuntime, load_graph, run_alert_match
from mftik_api.auth import AuthMiddleware
from mftik_api.routes import alerts as alerts_routes
from mftik_api.routes.alerts import router as alerts_router
from mftik_db.repositories import SessionLogRepository, StsSessionRepository

HOOK = "https://discord.com/api/webhooks/111/super-secret-token"
SIGNAL = '"risk value = {%f}", 0.995'
EXTRACT = {
    "pattern": r'risk value = \{\%f\}", ([\d.]+)',
    "group": 1,
    "as": "float",
    "op": ">",
    "value": 0.99,
}


def _app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(alerts_router)
    return app


@pytest.fixture
async def world(monkeypatch, database_url):
    audits: list[dict[str, Any]] = []
    posts: list[dict[str, Any]] = []

    async def capture(**kwargs: object) -> None:
        audits.append(dict(kwargs))

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append({"url": str(request.url), "body": json.loads(request.content)})
        return httpx.Response(204)

    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            await session.commit()
        monkeypatch.setattr(alerts_routes, "session_scope", database.scope)
        monkeypatch.setattr(alerts_routes, "record_audit", capture)
        monkeypatch.setattr(alert_match, "session_scope", database.scope)
        monkeypatch.setattr(log_persist, "session_scope", database.scope)
        monkeypatch.setenv("ALERT_GRAPH_POLL_INTERVAL", "0.15")
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hook:
            set_http_client(hook)
            try:
                yield database.scope, audits, posts
            finally:
                set_http_client(None)


async def _source(client, domain: str, selector: str) -> dict[str, Any]:
    answered = await client.post(
        "/alerts/sources", json={"domain": domain, "selector": selector}
    )
    assert answered.status_code == 201, answered.text
    return answered.json()


async def _matcher(
    client, name: str, kind: str, spec: dict[str, Any]
) -> dict[str, Any]:
    answered = await client.post(
        "/alerts/matchers", json={"name": name, "kind": kind, "spec": spec}
    )
    assert answered.status_code == 201, answered.text
    return answered.json()


async def _alert(client, name: str = "ops", **extra: object) -> dict[str, Any]:
    answered = await client.post(
        "/alerts", json={"name": name, "webhook_url": HOOK, **extra}
    )
    assert answered.status_code == 201, answered.text
    assert "webhook_url" not in answered.json()
    return answered.json()


async def _wire(client, source_id: int, matcher_id: int, alert_id: int) -> None:
    a = await client.put(f"/alerts/sources/{source_id}/matchers/{matcher_id}")
    b = await client.put(f"/alerts/matchers/{matcher_id}/alerts/{alert_id}")
    assert a.status_code == 200, a.text
    assert b.status_code == 200, b.text


async def _put_session(scope, session_id: str, type_: str | None) -> None:
    async with scope() as db:
        await StsSessionRepository(db).create_live(
            session_id=session_id,
            created_by=1,
            strategy="tiny",
            type=type_,
        )


def _sts_log(
    session_id: str,
    message: str,
    *,
    level: str = "warn",
    type_: str | None = None,
) -> Envelope[dict]:
    payload: dict[str, object] = {"level": level, "message": message}
    if type_ is not None:
        payload["type"] = type_
    return Envelope[dict].wrap(payload, type="log", source="sts", session_id=session_id)


async def _runtime() -> MatchRuntime:
    runtime = MatchRuntime()
    runtime.graph = await load_graph()
    return runtime


def _secret_leaked(blob: object) -> bool:
    text = blob if isinstance(blob, str) else json.dumps(blob)
    return HOOK in text or "super-secret-token" in text


async def test_s1_redeploy_keeps_the_alert(world) -> None:
    scope, _, posts = world
    first = uuid4().hex
    second = uuid4().hex
    await _put_session(scope, first, "private::Tiny")
    await _put_session(scope, second, "private::Tiny")
    async with a_client(_app()) as client:
        source = await _source(client, "sts", "private::Tiny")
        matcher = await _matcher(client, "warn", "level", {"levels": ["warn", "error"]})
        alert = await _alert(client)
        await _wire(client, source["id"], matcher["id"], alert["id"])
        runtime = await _runtime()
        await alert_match.ingest(
            runtime,
            f"log.sts.{first}",
            _sts_log(first, "tick", type_="private::Tiny"),
        )
        await alert_match.flush_alert(runtime, alert["id"])
        await alert_match.ingest(
            runtime,
            f"log.sts.{second}",
            _sts_log(second, "tick", type_="private::Tiny"),
        )
        await alert_match.flush_alert(runtime, alert["id"])
        listed = await client.get(f"/alerts/{alert['id']}/deliveries")

    rows = listed.json()["deliveries"]
    assert [row["alert_id"] for row in rows] == [alert["id"], alert["id"]]
    assert len(posts) == 2


async def test_s2_session_id_is_not_a_source(world) -> None:
    scope, _, posts = world
    sid = uuid4().hex
    await _put_session(scope, sid, "private::Tiny")
    src = Path(alert_match.__file__).read_text()
    assert "SessionLog" not in src
    assert "get_by_session_id" in src  # type lookup, not Source matching
    async with a_client(_app()) as client:
        hex_source = await _source(client, "sts", sid)
        typed = await _source(client, "sts", "private::Tiny")
        star = await _source(client, "sts", "*")
        hex_m = await _matcher(client, "hex", "level", {"levels": ["warn"]})
        type_m = await _matcher(client, "type", "level", {"levels": ["warn"]})
        star_m = await _matcher(client, "star", "level", {"levels": ["warn"]})
        hex_a = await _alert(client, "hex")
        type_a = await _alert(client, "typed")
        star_a = await _alert(client, "star")
        await _wire(client, hex_source["id"], hex_m["id"], hex_a["id"])
        await _wire(client, typed["id"], type_m["id"], type_a["id"])
        await _wire(client, star["id"], star_m["id"], star_a["id"])
        runtime = await _runtime()
        await alert_match.ingest(
            runtime,
            f"log.sts.{sid}",
            _sts_log(sid, "live", type_="private::Tiny"),
        )
        await alert_match.flush_alert(runtime, hex_a["id"])
        await alert_match.flush_alert(runtime, type_a["id"])
        await alert_match.flush_alert(runtime, star_a["id"])
        hex_d = await client.get(f"/alerts/{hex_a['id']}/deliveries")
        type_d = await client.get(f"/alerts/{type_a['id']}/deliveries")
        star_d = await client.get(f"/alerts/{star_a['id']}/deliveries")

    assert hex_d.json()["deliveries"] == []
    assert len(type_d.json()["deliveries"]) == 1
    assert len(star_d.json()["deliveries"]) == 1
    assert {p["body"]["embeds"][0]["title"] for p in posts} == {"typed", "star"}


async def test_s3_short_name_is_not_a_source(world) -> None:
    async with a_client(_app()) as client:
        short = await _source(client, "sts", "tiny")
        priv = await _source(client, "sts", "private::Tiny")
        peer = await _source(client, "sts", "node1::Tiny")
        short_m = await _matcher(client, "short", "level", {"levels": ["warn"]})
        priv_m = await _matcher(client, "priv", "level", {"levels": ["warn"]})
        peer_m = await _matcher(client, "peer", "level", {"levels": ["warn"]})
        short_a = await _alert(client, "short")
        priv_a = await _alert(client, "priv")
        peer_a = await _alert(client, "peer")
        await _wire(client, short["id"], short_m["id"], short_a["id"])
        await _wire(client, priv["id"], priv_m["id"], priv_a["id"])
        await _wire(client, peer["id"], peer_m["id"], peer_a["id"])
        runtime = await _runtime()
        a = uuid4().hex
        b = uuid4().hex
        await alert_match.ingest(
            runtime,
            f"log.sts.{a}",
            _sts_log(a, "a", type_="private::Tiny"),
        )
        await alert_match.ingest(
            runtime,
            f"log.sts.{b}",
            _sts_log(b, "b", type_="node1::Tiny"),
        )
        for alert_id in (short_a["id"], priv_a["id"], peer_a["id"]):
            await alert_match.flush_alert(runtime, alert_id)
        short_d = await client.get(f"/alerts/{short_a['id']}/deliveries")
        priv_d = await client.get(f"/alerts/{priv_a['id']}/deliveries")
        peer_d = await client.get(f"/alerts/{peer_a['id']}/deliveries")

    assert short_d.json()["deliveries"] == []
    assert priv_d.json()["deliveries"][0]["event_count"] == 1
    assert peer_d.json()["deliveries"][0]["event_count"] == 1


async def test_s4_live_only_no_history(world, monkeypatch) -> None:
    scope, _, posts = world
    sid = uuid4().hex
    await _put_session(scope, sid, "private::Tiny")
    async with scope() as db:
        await SessionLogRepository(db).bulk_insert_ignore(
            [
                {
                    "envelope_id": f"hist-{i}",
                    "domain": "sts",
                    "stream_id": sid,
                    "source": "sts",
                    "level": "warn",
                    "message": "tuesday warn",
                    "ts": float(i),
                }
                for i in range(3)
            ]
        )
    async with a_client(_app()) as client:
        source = await _source(client, "sts", "private::Tiny")
        matcher = await _matcher(client, "hist", "regex", {"pattern": "tuesday warn"})
        alert = await _alert(client, flush_interval_s=1)
        await _wire(client, source["id"], matcher["id"], alert["id"])
        patched = await client.patch(
            f"/alerts/matchers/{matcher['id']}",
            json={"spec": {"pattern": "wednesday warn"}},
        )
        assert patched.status_code == 200, patched.text

    src = Path(alert_match.__file__).read_text()
    assert "SessionLog" not in src
    assert "import flush_rows" not in src

    ready = asyncio.Event()

    class IdleBroker:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def fetch_log_buffer(self, _topic: str):
            raise AssertionError("match worker must not drain the ring")

        async def psubscribe(self, _pattern: str, *, stop: asyncio.Event):
            ready.set()
            await stop.wait()
            if False:
                yield None

    monkeypatch.setattr(alert_match, "Broker", IdleBroker)
    stop = asyncio.Event()
    task = asyncio.create_task(run_alert_match(stop))
    try:
        await asyncio.wait_for(ready.wait(), timeout=2)
        await asyncio.sleep(0.3)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2)
    assert posts == []


async def test_s5_no_ring_buffer_replay(world, monkeypatch) -> None:
    _, _, posts = world
    live: asyncio.Queue[tuple[str, Envelope[dict]] | None] = asyncio.Queue()
    ready = asyncio.Event()

    class LiveOnlyBroker:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def fetch_log_buffer(self, _topic: str):
            raise AssertionError("match worker must not drain the ring")

        async def psubscribe(self, _pattern: str, *, stop: asyncio.Event):
            ready.set()
            while not stop.is_set():
                try:
                    item = await asyncio.wait_for(live.get(), timeout=0.05)
                except TimeoutError:
                    continue
                if item is None:
                    return
                yield item

    async with a_client(_app()) as client:
        source = await _source(client, "sts", "*")
        matcher = await _matcher(client, "warn", "level", {"levels": ["warn"]})
        alert = await _alert(client, flush_interval_s=30)
        await _wire(client, source["id"], matcher["id"], alert["id"])

    monkeypatch.setattr(alert_match, "Broker", LiveOnlyBroker)
    stop = asyncio.Event()
    task = asyncio.create_task(run_alert_match(stop))
    try:
        await asyncio.wait_for(ready.wait(), timeout=2)
        await asyncio.sleep(0.15)
        assert posts == []
        sid = uuid4().hex
        await live.put((f"log.sts.{sid}", _sts_log(sid, "fresh", type_="CrossArb")))
        runtime = await _wait_pending(alert["id"])
        await alert_match.flush_alert(runtime, alert["id"])
    finally:
        stop.set()
        await live.put(None)
        await asyncio.wait_for(task, timeout=2)
    assert len(posts) == 1


async def _wait_pending(alert_id: int) -> MatchRuntime:
    for _ in range(100):
        runtime = alert_match.current_runtime()
        if runtime is not None and runtime.pending_events(alert_id):
            return runtime
        await asyncio.sleep(0.02)
    raise AssertionError(f"match worker did not buffer alert {alert_id}")


async def test_s6_warn_error_level(world) -> None:
    async with a_client(_app()) as client:
        source = await _source(client, "sts", "*")
        matcher = await _matcher(
            client, "level", "level", {"levels": ["warn", "error"]}
        )
        alert = await _alert(client)
        await _wire(client, source["id"], matcher["id"], alert["id"])
        runtime = await _runtime()
        sid = uuid4().hex
        await alert_match.ingest(
            runtime,
            f"log.sts.{sid}",
            _sts_log(sid, "quiet", level="info", type_="CrossArb"),
        )
        await alert_match.flush_alert(runtime, alert["id"])
        empty = await client.get(f"/alerts/{alert['id']}/deliveries")
        await alert_match.ingest(
            runtime,
            f"log.sts.{sid}",
            _sts_log(sid, "boom", level="error", type_="CrossArb"),
        )
        await alert_match.flush_alert(runtime, alert["id"])
        listed = await client.get(f"/alerts/{alert['id']}/deliveries")

    assert empty.json()["deliveries"] == []
    assert listed.json()["deliveries"][0]["event_count"] == 1


async def test_s7_extract_signal(world) -> None:
    _, _, posts = world
    async with a_client(_app()) as client:
        source = await _source(client, "sts", "*")
        matcher = await _matcher(client, "sig", "extract", EXTRACT)
        alert = await _alert(client, "signals", dedupe=False)
        await _wire(client, source["id"], matcher["id"], alert["id"])
        runtime = await _runtime()
        sid = uuid4().hex
        await alert_match.ingest(
            runtime,
            f"log.sts.{sid}",
            _sts_log(sid, SIGNAL, level="info", type_="CrossArb"),
        )
        await alert_match.ingest(
            runtime,
            f"log.sts.{sid}",
            _sts_log(
                sid,
                '"risk value = {%f}", 0.50',
                level="info",
                type_="CrossArb",
            ),
        )
        await alert_match.flush_alert(runtime, alert["id"])

    assert len(posts) == 1
    description = posts[0]["body"]["embeds"][0]["description"]
    assert "0.995" in description
    assert "0.50" not in description


async def test_s8_shared_alert_buffer(world) -> None:
    _, _, posts = world
    async with a_client(_app()) as client:
        source = await _source(client, "sts", "*")
        left = await _matcher(client, "left", "level", {"levels": ["info"]})
        right = await _matcher(client, "right", "level", {"levels": ["info"]})
        alert = await _alert(client, dedupe=False)
        await _wire(client, source["id"], left["id"], alert["id"])
        await _wire(client, source["id"], right["id"], alert["id"])
        runtime = await _runtime()
        sid = uuid4().hex
        for i in range(10):
            await alert_match.ingest(
                runtime,
                f"log.sts.{sid}",
                _sts_log(sid, f"line-{i}", level="info", type_="CrossArb"),
            )
        await alert_match.flush_alert(runtime, alert["id"])
        listed = await client.get(f"/alerts/{alert['id']}/deliveries")

    assert len(posts) == 1
    assert listed.json()["deliveries"][0]["event_count"] == 10


async def test_s9_persist_isolation(world, monkeypatch) -> None:
    scope, _, _ = world
    src = Path(alert_eval.__file__).read_text()
    assert "concurrent=True" in src
    monkeypatch.setattr(alert_eval, "SEARCH_TIMEOUT", 0.3)

    async with a_client(_app()) as client:
        source = await _source(client, "sts", "*")
        matcher = await _matcher(
            client, "slow", "regex", {"pattern": r"(a+)+$"}
        )
        alert = await _alert(client)
        await _wire(client, source["id"], matcher["id"], alert["id"])

    runtime = await _runtime()
    sid = uuid4().hex
    row = log_persist.envelope_to_row(
        f"log.sts.{sid}",
        _sts_log(sid, "a" * 28 + "x", level="info", type_="CrossArb"),
    )
    assert row is not None
    persisted = asyncio.Event()
    ticks = 0

    async def persist_same_line() -> None:
        await log_persist.flush_rows([row])
        persisted.set()

    async def ticker() -> None:
        nonlocal ticks
        while not persisted.is_set():
            ticks += 1
            await asyncio.sleep(0)

    probe = asyncio.create_task(ticker())
    persist_task = asyncio.create_task(persist_same_line())
    await alert_match.ingest(
        runtime,
        f"log.sts.{sid}",
        _sts_log(sid, "a" * 28 + "x", level="info", type_="CrossArb"),
    )
    await persist_task
    probe.cancel()
    assert persisted.is_set()
    assert persist_task.done()
    assert ticks > 1
    async with scope() as db:
        rows = await SessionLogRepository(db).list_before("sts", sid, limit=10)
    assert [logged.message for logged in rows] == ["a" * 28 + "x"]
    # The matcher thread is free for the next line after timeout=.
    await alert_match.ingest(
        runtime,
        f"log.sts.{sid}",
        _sts_log(sid, "held", level="info", type_="CrossArb"),
    )


async def test_s10_secret_stays_secret(world, caplog) -> None:
    _, audits, posts = world
    async with a_client(_app()) as client:
        source = await _source(client, "sts", "*")
        matcher = await _matcher(client, "warn", "level", {"levels": ["warn"]})
        created = await _alert(client)
        await _wire(client, source["id"], matcher["id"], created["id"])
        listed = await client.get("/alerts")
        got = await client.get(f"/alerts/{created['id']}")
        fired = await client.post(f"/alerts/{created['id']}/test")
        runtime = await _runtime()
        sid = uuid4().hex
        await alert_match.ingest(
            runtime,
            f"log.sts.{sid}",
            _sts_log(sid, "secret-line", type_="CrossArb"),
        )
        await alert_match.flush_alert(runtime, created["id"])

    assert fired.status_code == 200, fired.text
    assert not _secret_leaked(created)
    assert not _secret_leaked(listed.json())
    assert not _secret_leaked(got.json())
    for audit in audits:
        assert not _secret_leaked(audit["result"])
    assert not _secret_leaked(caplog.text)
    assert posts
    assert all(p["url"] == HOOK for p in posts)


async def test_s11_wildcard_and_specific(world) -> None:
    async with a_client(_app()) as client:
        star = await _source(client, "sts", "*")
        arb = await _source(client, "sts", "CrossArb")
        star_m = await _matcher(client, "star", "level", {"levels": ["warn"]})
        arb_m = await _matcher(client, "arb", "level", {"levels": ["warn"]})
        star_a = await _alert(client, "star")
        arb_a = await _alert(client, "arb")
        await _wire(client, star["id"], star_m["id"], star_a["id"])
        await _wire(client, arb["id"], arb_m["id"], arb_a["id"])
        runtime = await _runtime()
        arb_sid = uuid4().hex
        tiny_sid = uuid4().hex
        await alert_match.ingest(
            runtime,
            f"log.sts.{arb_sid}",
            _sts_log(arb_sid, "x", type_="CrossArb"),
        )
        await alert_match.ingest(
            runtime,
            f"log.sts.{tiny_sid}",
            _sts_log(tiny_sid, "y", type_="private::Tiny"),
        )
        await alert_match.flush_alert(runtime, star_a["id"])
        await alert_match.flush_alert(runtime, arb_a["id"])
        star_d = await client.get(f"/alerts/{star_a['id']}/deliveries")
        arb_d = await client.get(f"/alerts/{arb_a['id']}/deliveries")

    assert star_d.json()["deliveries"][0]["event_count"] == 2
    assert arb_d.json()["deliveries"][0]["event_count"] == 1


async def test_s12_null_type(world) -> None:
    scope, _, _ = world
    sid = uuid4().hex
    await _put_session(scope, sid, None)
    async with a_client(_app()) as client:
        star = await _source(client, "sts", "*")
        arb = await _source(client, "sts", "CrossArb")
        star_m = await _matcher(client, "star", "level", {"levels": ["warn"]})
        arb_m = await _matcher(client, "arb", "level", {"levels": ["warn"]})
        star_a = await _alert(client, "star")
        arb_a = await _alert(client, "arb")
        await _wire(client, star["id"], star_m["id"], star_a["id"])
        await _wire(client, arb["id"], arb_m["id"], arb_a["id"])
        runtime = await _runtime()
        await alert_match.ingest(runtime, f"log.sts.{sid}", _sts_log(sid, "untyped"))
        await alert_match.flush_alert(runtime, star_a["id"])
        await alert_match.flush_alert(runtime, arb_a["id"])
        star_d = await client.get(f"/alerts/{star_a['id']}/deliveries")
        arb_d = await client.get(f"/alerts/{arb_a['id']}/deliveries")

    assert len(star_d.json()["deliveries"]) == 1
    assert arb_d.json()["deliveries"] == []


async def test_s13_registry_key(monkeypatch, database_url, tmp_path) -> None:
    from mftik_api.auth import routes as auth_routes
    from test_auth_registry_keys import an_owner_with_keys

    auth_routes._failures.clear()
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setattr(alerts_routes, "session_scope", database.scope)
        monkeypatch.setenv("MFTIK_AUTH_ENABLED", "1")
        monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
        app = an_api(registry=True)
        app.include_router(alerts_router)
        async with a_client(app) as client:
            peer, script = await an_owner_with_keys(client)

        async with a_client(app) as client:
            denied_get = await client.get(
                "/alerts", headers={"Authorization": f"Bearer {peer}"}
            )
            denied_post = await client.post(
                "/alerts",
                json={"name": "ops", "webhook_url": HOOK},
                headers={"Authorization": f"Bearer {peer}"},
            )
            allowed_get = await client.get(
                "/alerts", headers={"Authorization": f"Bearer {script}"}
            )
            allowed_post = await client.post(
                "/alerts",
                json={"name": "ops", "webhook_url": HOOK},
                headers={"Authorization": f"Bearer {script}"},
            )

    assert denied_get.status_code == 403
    assert denied_post.status_code == 403
    assert allowed_get.status_code == 200
    assert allowed_post.status_code == 201
    assert "webhook_url" not in allowed_post.json()


async def test_s14_test_fire_is_not_a_match(world) -> None:
    _, _, posts = world
    async with a_client(_app()) as client:
        alert = await _alert(client)
        fired = await client.post(f"/alerts/{alert['id']}/test")
        listed = await client.get(f"/alerts/{alert['id']}/deliveries")
        sources = await client.get("/alerts/sources")

    assert fired.status_code == 200, fired.text
    assert fired.json()["delivery"]["event_count"] == 0
    assert listed.json()["deliveries"][0]["event_count"] == 0
    assert sources.json()["sources"] == []
    assert len(posts) == 1
    assert posts[0]["body"]["embeds"][0]["title"] == "test fire from this node"
