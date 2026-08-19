"""Audit rows name the proof, not the Owner.

See docs/AuditIdentity.md. The auth harness stubs ``record_audit`` so the
other files do not have to carry the audits table; this one puts it back
and reads ``GET /audits``.
"""

from __future__ import annotations

import pytest
from auth_harness import a_client, an_api, use_database
from db_harness import a_database
from mftik_api.audit_util import record_audit
from mftik_api.auth import routes as auth_routes
from mftik_api.routes import apis as apis_routes
from mftik_api.routes import audits as audits_routes
from mftik_api.routes.apis import router as apis_router
from mftik_api.routes.audits import router as audits_router

GOOD = "correct-horse-battery"


@pytest.fixture(autouse=True)
def _no_throttle() -> None:
    auth_routes._failures.clear()


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setattr(auth_routes, "record_audit", record_audit)
        monkeypatch.setattr("mftik_api.audit_util.session_scope", database.scope)
        monkeypatch.setattr(audits_routes, "session_scope", database.scope)
        monkeypatch.setattr(apis_routes, "session_scope", database.scope)
        monkeypatch.setenv("MFTIK_AUTH_ENABLED", "1")
        yield database.scope


def an_audit_api():
    app = an_api()
    app.include_router(apis_router)
    app.include_router(audits_router)
    return app


def _named(rows: list[dict], operation: str) -> dict:
    return next(row for row in rows if row["operation"] == operation)


async def test_password_login_writes_via_password(db) -> None:
    app = an_audit_api()
    async with a_client(app) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})

    async with a_client(app) as client:
        logged_in = await client.post(
            "/auth/login/password", json={"username": "yite", "password": GOOD}
        )
        listed = await client.get("/audits")

    assert logged_in.status_code == 200
    login = _named(listed.json()["audits"], "auth.login")
    assert login["via"] == "password"
    assert login["key_kind"] is None
    assert login["key_id"] is None
    assert login["user_id"] == 1
    assert "ip=" in login["result"]
    assert "via=" not in login["result"]


async def test_an_api_key_audit_names_the_key(db) -> None:
    app = an_audit_api()
    async with a_client(app) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})
        minted = await client.post("/auth/keys", json={"name": "ci"})
        token = minted.json()["token"]
        key_id = minted.json()["id"]

    auth = {"Authorization": f"Bearer {token}"}
    async with a_client(app) as client:
        created = await client.post(
            "/apis",
            json={
                "name": "paper trader",
                "venue": "Paper",
                "api_key": "paper-key-audit",
                "api_secret": "paper-secret-audit",
                "type": "HMAC",
            },
            headers=auth,
        )
        listed = await client.get("/audits", headers=auth)

    assert created.status_code == 201
    row = _named(listed.json()["audits"], "api.create")
    assert row["via"] == "key:ci"
    assert row["key_kind"] == "api"
    assert row["key_id"] == key_id
    assert row["user_id"] == 1
