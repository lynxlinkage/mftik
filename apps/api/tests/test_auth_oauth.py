"""Discord: linking an account to the Owner, and signing in with a linked one.

Every assertion here is really the same one. An OAuth callback may attach to
the Owner or be refused; it may never create one. Break that and "one
instance, one person" stops being true the first time a stranger clicks a
link.

The provider is stubbed. What is under test is this side of the flow — which
record a callback is answered from, and what it is allowed to do — not
Discord's.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from auth_harness import a_client, an_api, use_database
from db_harness import a_database
from mftik_api.auth import oauth
from mftik_api.auth import routes as auth_routes
from mftik_db.models.auth import AuthIdentity, AuthOAuthState
from mftik_db.models.user import User
from sqlalchemy import func, select

GOOD = "correct-horse-battery"


@pytest.fixture(autouse=True)
def _no_throttle() -> None:
    auth_routes._failures.clear()


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setattr(oauth, "session_scope", database.scope, raising=False)
        monkeypatch.setenv("MFTIK_AUTH_ENABLED", "1")
        monkeypatch.setenv("DISCORD_CLIENT_ID", "client-id")
        monkeypatch.setenv("DISCORD_CLIENT_SECRET", "client-secret")
        monkeypatch.setenv("MFTIK_OAUTH_REDIRECT_BASE", "http://localhost:5173/api")
        yield database.scope


def a_discord_account(monkeypatch, subject: str = "1234567890", label: str = "yite"):
    """Stand in for the two calls ``exchange`` makes, and record the verifier."""
    seen: dict[str, str] = {}

    async def _exchange(self, code: str, verifier: str, *, client=None):
        seen["code"] = code
        seen["verifier"] = verifier
        return oauth.Profile(subject=subject, label=label)

    monkeypatch.setattr(oauth.Provider, "exchange", _exchange)
    return seen


async def an_owner(client) -> None:
    created = await client.post(
        "/auth/setup", json={"username": "yite", "password": GOOD}
    )
    assert created.status_code == 201


async def a_started_flow(client, path: str) -> str:
    """Follow the redirect to the provider and pull `state` back out of it."""
    started = await client.get(path)
    assert started.status_code == 307, started.text
    location = httpx.URL(started.headers["location"])
    assert location.host == "discord.com"
    return location.params["state"]


# ------------------------------------------------------------------ start ---


async def test_the_authorize_url_asks_for_an_id_and_nothing_else(db) -> None:
    async with a_client(an_api()) as client:
        await an_owner(client)
        started = await client.get("/auth/login/discord")

    url = httpx.URL(started.headers["location"])
    assert url.params["scope"] == "identify", "no email: it is never matched on"
    assert url.params["redirect_uri"] == (
        "http://localhost:5173/api/auth/callback/discord"
    )
    assert url.params["code_challenge_method"] == "S256"


async def test_an_unconfigured_provider_is_not_offered(monkeypatch, db) -> None:
    monkeypatch.delenv("DISCORD_CLIENT_SECRET")
    async with a_client(an_api()) as client:
        await an_owner(client)
        status = await client.get("/auth/status")
        started = await client.get("/auth/login/discord")

    assert status.json()["providers"] == ["password"]
    assert started.status_code == 404


async def test_providers_lists_discord_once_it_is_configured(db) -> None:
    async with a_client(an_api()) as client:
        await an_owner(client)
        status = await client.get("/auth/status")
    assert status.json()["providers"] == ["password", "discord"]


# --------------------------------------------------------------- callback ---


async def test_connect_links_the_account_to_the_one_owner(monkeypatch, db) -> None:
    seen = a_discord_account(monkeypatch)
    async with a_client(an_api()) as client:
        await an_owner(client)
        state = await a_started_flow(client, "/auth/connect/discord")
        done = await client.get(f"/auth/callback/discord?state={state}&code=abc")

    assert done.status_code == 303
    assert done.headers["location"] == "/settings"
    assert seen["code"] == "abc"

    async with db() as session:
        rows = list((await session.execute(select(AuthIdentity))).scalars())
        users = await session.scalar(select(func.count()).select_from(User))
    assert len(rows) == 1
    assert (rows[0].provider, rows[0].subject, rows[0].user_id) == (
        "discord",
        "1234567890",
        1,
    )
    assert users == 1


async def test_connecting_the_same_account_twice_is_idempotent(
    monkeypatch, db
) -> None:
    a_discord_account(monkeypatch)
    async with a_client(an_api()) as client:
        await an_owner(client)
        for _ in range(2):
            state = await a_started_flow(client, "/auth/connect/discord")
            done = await client.get(f"/auth/callback/discord?state={state}&code=abc")
            assert done.status_code == 303

    async with db() as session:
        count = await session.scalar(select(func.count()).select_from(AuthIdentity))
    assert count == 1


async def test_a_linked_account_signs_in(monkeypatch, db) -> None:
    a_discord_account(monkeypatch)
    app = an_api()
    async with a_client(app) as client:
        await an_owner(client)
        state = await a_started_flow(client, "/auth/connect/discord")
        await client.get(f"/auth/callback/discord?state={state}&code=abc")

    async with a_client(app) as client:
        state = await a_started_flow(client, "/auth/login/discord")
        done = await client.get(f"/auth/callback/discord?state={state}&code=abc")
        me = await client.get("/auth/me")

    assert done.status_code == 303
    assert done.headers["location"] == "/"
    assert me.status_code == 200
    assert me.json()["via"] == "discord"
    assert me.json()["user_id"] == 1


async def test_an_unlinked_account_is_refused_and_creates_nothing(
    monkeypatch, db
) -> None:
    """The rule the whole document exists for."""
    a_discord_account(monkeypatch, subject="a-stranger")
    async with a_client(an_api()) as client:
        await an_owner(client)

    async with a_client(an_api()) as client:
        state = await a_started_flow(client, "/auth/login/discord")
        done = await client.get(f"/auth/callback/discord?state={state}&code=abc")

    assert done.status_code == 403
    async with db() as session:
        users = await session.scalar(select(func.count()).select_from(User))
        identities = await session.scalar(
            select(func.count()).select_from(AuthIdentity)
        )
    assert (users, identities) == (1, 0)


# ------------------------------------------------------------------ state ---


@pytest.mark.parametrize("state", ["", "never-issued"])
async def test_a_callback_we_did_not_start_is_refused(
    monkeypatch, db, state: str
) -> None:
    a_discord_account(monkeypatch)
    async with a_client(an_api()) as client:
        await an_owner(client)
        done = await client.get(f"/auth/callback/discord?state={state}&code=abc")

    assert done.status_code in {400, 403}
    async with db() as session:
        count = await session.scalar(select(func.count()).select_from(AuthIdentity))
    assert count == 0


async def test_a_state_works_once(monkeypatch, db) -> None:
    a_discord_account(monkeypatch)
    async with a_client(an_api()) as client:
        await an_owner(client)
        state = await a_started_flow(client, "/auth/connect/discord")
        first = await client.get(f"/auth/callback/discord?state={state}&code=abc")
        replay = await client.get(f"/auth/callback/discord?state={state}&code=abc")

    assert first.status_code == 303
    assert replay.status_code == 403


async def test_an_expired_state_is_refused(monkeypatch, db) -> None:
    a_discord_account(monkeypatch)
    async with a_client(an_api()) as client:
        await an_owner(client)
        state = await a_started_flow(client, "/auth/connect/discord")

        async with db() as session:
            row = await session.get(AuthOAuthState, state)
            row.expires_at = datetime.now(UTC) - timedelta(seconds=1)

        done = await client.get(f"/auth/callback/discord?state={state}&code=abc")

    assert done.status_code == 403


async def test_a_connect_cannot_be_finished_in_another_browser(
    monkeypatch, db
) -> None:
    """The account-linking CSRF this design exists to stop.

    The state is unguessable, so this is the residual case: someone who has
    the state but not the session that started it. Binding the two is what
    makes the callback useless to them.
    """
    a_discord_account(monkeypatch)
    app = an_api()
    async with a_client(app) as client:
        await an_owner(client)
        state = await a_started_flow(client, "/auth/connect/discord")

    async with a_client(app) as elsewhere:
        # A different browser, with no session at all.
        done = await elsewhere.get(f"/auth/callback/discord?state={state}&code=abc")

    assert done.status_code == 403
    async with db() as session:
        count = await session.scalar(select(func.count()).select_from(AuthIdentity))
    assert count == 0


async def test_starting_a_connect_needs_a_session(db) -> None:
    async with a_client(an_api()) as client:
        await an_owner(client)

    async with a_client(an_api()) as client:
        refused = await client.get("/auth/connect/discord")
    assert refused.status_code == 401


# ------------------------------------------------------------- identities ---


async def test_identities_list_password_alongside_the_rest(monkeypatch, db) -> None:
    a_discord_account(monkeypatch)
    async with a_client(an_api()) as client:
        await an_owner(client)
        state = await a_started_flow(client, "/auth/connect/discord")
        await client.get(f"/auth/callback/discord?state={state}&code=abc")
        listed = (await client.get("/auth/identities")).json()["identities"]

    by_provider = {row["provider"]: row for row in listed}
    assert set(by_provider) == {"password", "discord"}
    assert by_provider["password"]["removable"] is False
    assert by_provider["password"]["id"] is None
    # Which account, not merely that there is one.
    assert by_provider["discord"]["label"] == "yite"
    assert by_provider["discord"]["removable"] is True


async def test_unlinking_removes_only_the_oauth_one(monkeypatch, db) -> None:
    a_discord_account(monkeypatch)
    async with a_client(an_api()) as client:
        await an_owner(client)
        state = await a_started_flow(client, "/auth/connect/discord")
        await client.get(f"/auth/callback/discord?state={state}&code=abc")
        listed = (await client.get("/auth/identities")).json()["identities"]
        discord = next(r for r in listed if r["provider"] == "discord")

        gone = await client.delete(f"/auth/identities/{discord['id']}")
        after = (await client.get("/auth/identities")).json()["identities"]
        # The password is still the way in, which is the point of it being
        # the root identity rather than a row.
        me = await client.get("/auth/me")

    assert gone.status_code == 200
    assert [r["provider"] for r in after] == ["password"]
    assert me.status_code == 200


async def test_a_key_cannot_touch_identities(monkeypatch, db) -> None:
    app = an_api()
    async with a_client(app) as client:
        await an_owner(client)
        token = (
            await client.post("/auth/keys", json={"name": "ci"})
        ).json()["token"]

    auth = {"Authorization": f"Bearer {token}"}
    async with a_client(app) as client:
        listed = await client.get("/auth/identities", headers=auth)
        connect = await client.get("/auth/connect/discord", headers=auth)
        unlink = await client.delete("/auth/identities/1", headers=auth)

    assert listed.status_code == 403
    assert connect.status_code == 403
    assert unlink.status_code == 403
