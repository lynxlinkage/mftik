"""Alert CRUD: mask, audit, test fire, compile, registry-key 403."""

from __future__ import annotations

import httpx
import pytest
from auth_harness import a_client, an_api, use_database
from db_harness import a_database, an_owner
from fastapi import FastAPI
from mftik_api.alert_discord import set_http_client
from mftik_api.alert_spec import InvalidMatcherSpec, compile_matcher_spec
from mftik_api.auth import AuthMiddleware
from mftik_api.auth.middleware import REGISTRY_READ_PATHS, required_scope
from mftik_api.auth.principal import SCOPE_API
from mftik_api.routes import alerts as alerts_routes
from mftik_api.routes.alerts import router as alerts_router

HOOK = "https://discord.com/api/webhooks/1234567890/super-secret-token"
HOOK2 = "https://discord.com/api/webhooks/999/rotated-token"


@pytest.fixture
async def db(monkeypatch, database_url):
    audits: list[dict] = []

    async def capture(**kwargs: object) -> None:
        audits.append(dict(kwargs))

    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            await session.commit()
        monkeypatch.setattr(alerts_routes, "session_scope", database.scope)
        monkeypatch.setattr(alerts_routes, "record_audit", capture)
        yield database.scope, audits


def _app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(alerts_router)
    return app


async def test_create_lists_masked_url_and_audits_without_it(db) -> None:
    _, audits = db
    async with a_client(_app()) as client:
        created = await client.post(
            "/alerts", json={"name": "ops", "webhook_url": HOOK}
        )
        listed = await client.get("/alerts")
        got = await client.get(f"/alerts/{created.json()['id']}")

    assert created.status_code == 201, created.text
    body = created.json()
    assert "webhook_url" not in body
    assert body["webhook_masked"] == "https://discord.com/api/webhooks/…/***"
    assert body["name"] == "ops"
    assert "webhook_url" not in listed.json()["alerts"][0]
    assert "webhook_url" not in got.json()
    create_audit = next(a for a in audits if a["operation"] == "alert.create")
    assert "ops" in create_audit["result"]
    assert HOOK not in create_audit["result"]
    assert "super-secret-token" not in create_audit["result"]
    assert "discord.com/api/webhooks/123" not in create_audit["result"]


async def test_patch_name_leaves_the_url_and_patch_url_changes_the_mask(
    db,
) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(204)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as hook_client:
        set_http_client(hook_client)
        try:
            async with a_client(_app()) as client:
                created = await client.post(
                    "/alerts", json={"name": "ops", "webhook_url": HOOK}
                )
                alert_id = created.json()["id"]
                renamed = await client.patch(
                    f"/alerts/{alert_id}", json={"name": "signals"}
                )
                rotated = await client.patch(
                    f"/alerts/{alert_id}", json={"webhook_url": HOOK2}
                )
                fired = await client.post(f"/alerts/{alert_id}/test")
        finally:
            set_http_client(None)

    assert renamed.json()["name"] == "signals"
    assert renamed.json()["webhook_masked"].endswith("/api/webhooks/…/***")
    assert rotated.json()["webhook_masked"] == "https://discord.com/api/webhooks/…/***"
    assert fired.status_code == 200, fired.text
    delivery = fired.json()["delivery"]
    assert delivery["event_count"] == 0
    assert delivery["http_status"] == 204
    assert seen == [HOOK2]


async def test_test_fire_writes_a_zero_event_delivery(db) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as hook_client:
        set_http_client(hook_client)
        try:
            async with a_client(_app()) as client:
                created = await client.post(
                    "/alerts", json={"name": "ops", "webhook_url": HOOK}
                )
                alert_id = created.json()["id"]
                await client.post(f"/alerts/{alert_id}/test")
                listed = await client.get(f"/alerts/{alert_id}/deliveries")
        finally:
            set_http_client(None)

    rows = listed.json()["deliveries"]
    assert len(rows) == 1
    assert rows[0]["event_count"] == 0
    assert rows[0]["dropped_count"] == 0


async def test_put_source_matcher_is_idempotent_and_there_is_no_source_alert_route(
    db,
) -> None:
    async with a_client(_app()) as client:
        source = await client.post(
            "/alerts/sources",
            json={"domain": "sts", "selector": "private::Tiny"},
        )
        matcher = await client.post(
            "/alerts/matchers",
            json={
                "name": "warn",
                "kind": "level",
                "spec": {"levels": ["warn", "error"]},
            },
        )
        alert = await client.post(
            "/alerts", json={"name": "ops", "webhook_url": HOOK}
        )
        first = await client.put(
            f"/alerts/sources/{source.json()['id']}/matchers/{matcher.json()['id']}"
        )
        second = await client.put(
            f"/alerts/sources/{source.json()['id']}/matchers/{matcher.json()['id']}"
        )
        missing = await client.put(
            f"/alerts/sources/{source.json()['id']}/alerts/{alert.json()['id']}"
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert missing.status_code == 404


async def test_bad_regex_is_422_and_uses_the_regex_package() -> None:
    with pytest.raises(InvalidMatcherSpec):
        compile_matcher_spec("regex", {"pattern": "("})
    compile_matcher_spec("regex", {"pattern": "risk value"})
    compile_matcher_spec(
        "extract",
        {
            "pattern": r"risk value = \{\%f\}, ([\d.]+)",
            "group": 1,
            "as": "float",
            "op": ">",
            "value": 0.99,
        },
    )

    # No database: the handler must refuse before it writes.
    async with a_client(_app()) as client:
        answered = await client.post(
            "/alerts/matchers",
            json={"name": "bad", "kind": "regex", "spec": {"pattern": "("}},
        )
    assert answered.status_code == 422
    assert "invalid pattern" in answered.json()["detail"]


def test_alerts_are_not_a_registry_read_path() -> None:
    assert "/alerts" not in REGISTRY_READ_PATHS
    assert required_scope("GET", "/alerts") == SCOPE_API
    assert required_scope("GET", "/alerts/sources") == SCOPE_API
    assert required_scope("POST", "/alerts") == SCOPE_API


async def test_registry_key_is_403_when_the_gate_is_on(
    monkeypatch, database_url, tmp_path
) -> None:
    """Auth is off by default; without the flag there is no principal to refuse."""
    from mftik_api.auth import routes as auth_routes
    from test_auth_registry_keys import an_owner_with_keys as mint_keys

    auth_routes._failures.clear()
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setenv("MFTIK_AUTH_ENABLED", "1")
        monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
        app = an_api(registry=True)
        app.include_router(alerts_router)
        async with a_client(app) as client:
            peer, _ = await mint_keys(client)

        answered = None
        async with a_client(app) as client:
            answered = await client.get(
                "/alerts", headers={"Authorization": f"Bearer {peer}"}
            )

    assert answered is not None
    assert answered.status_code == 403
    assert "x-mftik-auth" not in answered.headers
