"""Settings → Update is a browser session talking to the updater, not a key."""

from __future__ import annotations

import httpx
import pytest
from auth_harness import a_client, an_api, use_database
from db_harness import a_database
from fastapi import FastAPI
from mftik_api.auth import routes as auth_routes
from mftik_api.routes.admin import router as admin_router

GOOD = "correct-horse-battery"

STATUS = {
    "state": "running",
    "step": "wait_md_next",
    "from_version": "v0.0.1",
    "to_version": "v0.0.2",
    "feeds_published": 3,
    "feeds_total": 5,
    "error": None,
    "updated_at": 1.0,
}


@pytest.fixture(autouse=True)
def _no_throttle() -> None:
    auth_routes._failures.clear()


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setenv("MFTIK_AUTH_ENABLED", "1")
        monkeypatch.delenv("MFTIK_UPDATER_URL", raising=False)
        monkeypatch.delenv("MFTIK_UPDATER_TOKEN", raising=False)
        yield database.scope


def an_admin_api() -> FastAPI:
    app = an_api()
    app.include_router(admin_router)
    return app


async def signed_in(client: httpx.AsyncClient) -> None:
    created = await client.post(
        "/auth/setup", json={"username": "yite", "password": GOOD}
    )
    assert created.status_code == 201


async def minted_key(client: httpx.AsyncClient) -> str:
    await signed_in(client)
    minted = await client.post("/auth/keys", json={"name": "ci"})
    assert minted.status_code == 201
    return minted.json()["token"]


async def test_without_an_updater_get_is_available_false(db) -> None:
    async with a_client(an_admin_api()) as client:
        await signed_in(client)
        answered = await client.get("/admin/update")
    assert answered.status_code == 200
    assert answered.json()["available"] is False
    assert answered.json()["state"] == "idle"


async def test_without_an_updater_post_is_404(db) -> None:
    async with a_client(an_admin_api()) as client:
        await signed_in(client)
        refused = await client.post("/admin/update")
    assert refused.status_code == 404


async def test_a_key_cannot_read_or_start_an_update(db) -> None:
    app = an_admin_api()
    async with a_client(app) as client:
        token = await minted_key(client)

    auth = {"Authorization": f"Bearer {token}"}
    async with a_client(app) as client:
        listed = await client.get("/admin/update", headers=auth)
        started = await client.post("/admin/update", headers=auth)

    assert listed.status_code == 403
    assert started.status_code == 403
    assert "x-mftik-auth" not in listed.headers


async def test_status_is_proxied_and_marked_available(db, monkeypatch) -> None:
    monkeypatch.setenv("MFTIK_UPDATER_URL", "http://updater:8080")
    monkeypatch.setenv("MFTIK_UPDATER_TOKEN", "secret")

    async def fake_call(method: str, path: str) -> httpx.Response:
        assert method == "GET"
        assert path == "/status"
        return httpx.Response(200, json=STATUS)

    monkeypatch.setattr("mftik_api.routes.admin.call_updater", fake_call)

    async with a_client(an_admin_api()) as client:
        await signed_in(client)
        answered = await client.get("/admin/update")

    assert answered.status_code == 200
    body = answered.json()
    assert body["available"] is True
    assert body["step"] == "wait_md_next"
    assert body["feeds_published"] == 3
    assert body["to_version"] == "v0.0.2"


async def test_post_returns_202_from_the_updater(db, monkeypatch) -> None:
    monkeypatch.setenv("MFTIK_UPDATER_URL", "http://updater:8080")
    monkeypatch.setenv("MFTIK_UPDATER_TOKEN", "secret")

    async def fake_call(method: str, path: str) -> httpx.Response:
        assert method == "POST"
        assert path == "/update"
        return httpx.Response(202, json={**STATUS, "step": "resolve"})

    monkeypatch.setattr("mftik_api.routes.admin.call_updater", fake_call)

    async with a_client(an_admin_api()) as client:
        await signed_in(client)
        started = await client.post("/admin/update")

    assert started.status_code == 202
    assert started.json()["available"] is True
    assert started.json()["step"] == "resolve"


async def test_a_running_update_is_409(db, monkeypatch) -> None:
    monkeypatch.setenv("MFTIK_UPDATER_URL", "http://updater:8080")
    monkeypatch.setenv("MFTIK_UPDATER_TOKEN", "secret")

    async def fake_call(method: str, path: str) -> httpx.Response:
        return httpx.Response(409, json={"error": "an update is already running"})

    monkeypatch.setattr("mftik_api.routes.admin.call_updater", fake_call)

    async with a_client(an_admin_api()) as client:
        await signed_in(client)
        refused = await client.post("/admin/update")

    assert refused.status_code == 409
    assert "already running" in refused.json()["detail"]


async def test_an_unreachable_updater_is_502(db, monkeypatch) -> None:
    monkeypatch.setenv("MFTIK_UPDATER_URL", "http://updater:8080")
    monkeypatch.setenv("MFTIK_UPDATER_TOKEN", "secret")

    async def fake_call(method: str, path: str) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr("mftik_api.routes.admin.call_updater", fake_call)

    async with a_client(an_admin_api()) as client:
        await signed_in(client)
        answered = await client.get("/admin/update")

    assert answered.status_code == 502
    assert answered.json()["detail"] == "updater unreachable"
