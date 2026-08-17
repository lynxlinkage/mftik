"""The gate: what gets in without a credential, and what does not.

Default deny is the property under test. The allowlist is asserted against
the real app's route table rather than against itself, so a route added later
is gated by having been added — and a route deliberately made public shows up
here as a diff instead of as a quiet hole.
"""

from __future__ import annotations

import pytest
from auth_harness import a_client, an_api, use_database
from db_harness import a_database
from mftik_api.auth import routes as auth_routes
from mftik_api.auth import sessions
from mftik_api.auth.middleware import AuthMiddleware, is_public
from mftik_api.deps import DEFAULT_USER_ID

GOOD = "correct-horse-battery"

#: Every path the app answers without a credential. Anything not here is
#: gated; anything here was a decision.
EXPECTED_PUBLIC = {
    # Compose and CI probe this, so it cannot be the keepalive endpoint —
    # it answers 200 to an expired session because it never looks at one.
    "/health",
    "/auth/status",
    "/auth/setup",
    "/auth/login/password",
    # Versions only, so a peer learns it speaks the wrong protocol before it
    # goes looking for a key.
    "/registry/v1/info",
}


@pytest.fixture(autouse=True)
def _no_throttle() -> None:
    auth_routes._failures.clear()


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setenv("MFT_AUTH_ENABLED", "1")
        yield database.scope


async def an_owner_with_a_password(client) -> None:
    created = await client.post(
        "/auth/setup", json={"username": "yite", "password": GOOD}
    )
    assert created.status_code == 201


async def test_the_flag_off_is_exactly_todays_behaviour(
    monkeypatch, database_url
) -> None:
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setenv("MFT_AUTH_ENABLED", "0")

        async with a_client(an_api()) as client:
            answered = await client.get("/sts/sessions")
            # And says so, so the UI does not offer to sign in or out of a
            # gate that is not there. Both would be no-ops.
            status = await client.get("/auth/status")

    assert answered.status_code == 200
    assert answered.json() == {"owner": DEFAULT_USER_ID}
    assert status.json()["enabled"] is False


async def test_a_gated_route_without_a_cookie_is_401(db) -> None:
    async with a_client(an_api()) as client:
        refused = await client.get("/sts/sessions")

    assert refused.status_code == 401
    assert refused.json() == {"detail": "authentication required"}
    # Says which gate answered. Until the cutover the Traefik chain is also in
    # front of production and wants the opposite response from the browser —
    # a document navigation rather than a client-side route to /login.
    assert refused.headers["x-mft-auth"] == "login-required"


async def test_the_public_routes_answer_without_one(db) -> None:
    async with a_client(an_api()) as client:
        assert (await client.get("/health")).status_code == 200
        assert (await client.get("/auth/status")).status_code == 200


async def test_a_session_gets_through_and_carries_the_owner(db) -> None:
    async with a_client(an_api()) as client:
        await an_owner_with_a_password(client)
        answered = await client.get("/sts/sessions")

    assert answered.status_code == 200
    assert answered.json() == {"owner": 1}


async def test_a_forged_identity_header_is_removed(db) -> None:
    """The Traefik chain strips these today. The app has to keep doing it."""
    async with a_client(an_api()) as client:
        refused = await client.get(
            "/sts/sessions",
            headers={"X-Auth-User": "1", "X-Auth-Groups": "admin"},
        )

    assert refused.status_code == 401


async def test_a_database_that_cannot_answer_is_401_not_500(
    monkeypatch, database_url
) -> None:
    """An outage costs logins, not every route. /health still answers."""
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setenv("MFT_AUTH_ENABLED", "1")

        app = an_api()
        async with a_client(app) as client:
            await an_owner_with_a_password(client)

            from mftik_api.auth import middleware as auth_middleware

            def _broken():
                raise RuntimeError("database is gone")

            monkeypatch.setattr(auth_middleware, "session_scope", _broken)

            refused = await client.get("/sts/sessions")
            healthy = await client.get("/health")

    assert refused.status_code == 401
    assert healthy.status_code == 200


async def test_a_websocket_without_a_session_is_refused_before_accept(db) -> None:
    """A socket cannot report a 401 once accepted, so this happens first."""
    reached = False

    async def app(scope, receive, send) -> None:
        nonlocal reached
        reached = True

    sent: list[dict] = []

    async def _receive() -> dict:
        return {"type": "websocket.connect"}

    async def _send(message: dict) -> None:
        sent.append(message)

    await AuthMiddleware(app)(
        {"type": "websocket", "path": "/ws/board", "headers": []}, _receive, _send
    )

    assert reached is False
    assert sent == [{"type": "websocket.close", "code": 1008}]


async def test_a_websocket_with_a_session_reaches_the_endpoint(db) -> None:
    async with a_client(an_api()) as client:
        await an_owner_with_a_password(client)
        token = client.cookies[sessions.COOKIE_NAME]

    seen: dict = {}

    async def app(scope, receive, send) -> None:
        seen["principal"] = scope["state"]["principal"]

    async def _receive() -> dict:
        return {"type": "websocket.connect"}

    async def _send(message: dict) -> None:  # pragma: no cover - nothing is sent
        raise AssertionError(f"unexpected {message}")

    await AuthMiddleware(app)(
        {
            "type": "websocket",
            "path": "/ws/board",
            "headers": [(b"cookie", f"{sessions.COOKIE_NAME}={token}".encode())],
        },
        _receive,
        _send,
    )

    assert seen["principal"].user_id == 1
    assert seen["principal"].via == "password"


def test_the_public_surface_is_the_one_we_meant() -> None:
    """Read from the real app, so a new route cannot join the list quietly.

    HTTP paths come from the OpenAPI document rather than ``app.routes``:
    included routers are expanded lazily and do not list their own paths, and
    the document is what ``just check-contracts`` publishes anyway. WebSocket
    routes are not in it, and are checked alongside — none of them is public,
    and the assertion is what says so.
    """
    from fastapi.routing import APIWebSocketRoute
    from mftik_api.main import app

    paths = set(app.openapi()["paths"]) | {
        route.path for route in app.routes if isinstance(route, APIWebSocketRoute)
    }
    assert {path for path in paths if is_public(path)} == EXPECTED_PUBLIC
