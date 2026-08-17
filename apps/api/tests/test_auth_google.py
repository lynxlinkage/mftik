"""Google, and what having two providers has to keep true.

Adding a provider is meant to be a table entry, so most of the behaviour is
already covered by the Discord tests running over the same code. What is new
is plural: two ways in on one Owner, and two namespaces of subject that must
not be able to collide.
"""

from __future__ import annotations

import httpx
import pytest
from auth_harness import a_client, an_api, use_database
from db_harness import a_database
from mftik_api.auth import oauth
from mftik_api.auth import routes as auth_routes
from mftik_db.models.auth import AuthIdentity
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
        monkeypatch.setenv("MFT_AUTH_ENABLED", "1")
        monkeypatch.setenv("MFT_OAUTH_REDIRECT_BASE", "http://localhost:5173/api")
        for provider in ("DISCORD", "GOOGLE"):
            monkeypatch.setenv(f"{provider}_CLIENT_ID", "client-id")
            monkeypatch.setenv(f"{provider}_CLIENT_SECRET", "client-secret")
        yield database.scope


def an_account(monkeypatch, **by_provider: oauth.Profile):
    """Stub the exchange, answering differently per provider."""

    async def _exchange(self, code: str, verifier: str, *, client=None):
        return by_provider[self.name]

    monkeypatch.setattr(oauth.Provider, "exchange", _exchange)


async def an_owner(client) -> None:
    created = await client.post(
        "/auth/setup", json={"username": "yite", "password": GOOD}
    )
    assert created.status_code == 201


async def connect(client, provider: str) -> httpx.Response:
    started = await client.get(f"/auth/connect/{provider}")
    assert started.status_code == 307, started.text
    state = httpx.URL(started.headers["location"]).params["state"]
    return await client.get(f"/auth/callback/{provider}?state={state}&code=abc")


async def test_google_asks_for_the_address_it_will_only_ever_display(db) -> None:
    """``openid`` alone gives the ``sub`` and nothing a person recognises.

    Discord has a username; Google has only the address, so it is asked for —
    and it stays a label. The identity is still ``sub``.
    """
    async with a_client(an_api()) as client:
        await an_owner(client)
        started = await client.get("/auth/login/google")

    url = httpx.URL(started.headers["location"])
    assert url.host == "accounts.google.com"
    assert url.params["scope"] == "openid email"
    assert url.params["redirect_uri"] == (
        "http://localhost:5173/api/auth/callback/google"
    )
    # Not "consent": Google takes that as "show the consent screen again",
    # while what Connect needs to offer is a choice of account.
    assert url.params["prompt"] == "select_account"
    assert url.params["code_challenge_method"] == "S256"


async def test_both_providers_are_offered_when_both_are_configured(db) -> None:
    async with a_client(an_api()) as client:
        await an_owner(client)
        status = await client.get("/auth/status")
    assert status.json()["providers"] == ["password", "discord", "google"]


async def test_one_owner_can_hold_both(monkeypatch, db) -> None:
    an_account(
        monkeypatch,
        discord=oauth.Profile(subject="snowflake-1", label="yite"),
        google=oauth.Profile(subject="google-sub-1", email="yite@example.com"),
    )
    async with a_client(an_api()) as client:
        await an_owner(client)
        assert (await connect(client, "discord")).status_code == 303
        assert (await connect(client, "google")).status_code == 303
        listed = (await client.get("/auth/identities")).json()["identities"]

    async with db() as session:
        users = await session.scalar(select(func.count()).select_from(User))
    assert users == 1, "two ways in, still one person"
    by_provider = {row["provider"]: row for row in listed}
    assert set(by_provider) == {"password", "discord", "google"}
    # Each shows whichever identifier its provider actually gives.
    assert by_provider["discord"]["label"] == "yite"
    assert by_provider["google"]["email"] == "yite@example.com"


async def test_the_same_subject_under_two_providers_is_two_identities(
    monkeypatch, db
) -> None:
    """Subjects are only unique within a provider, so the key is the pair.

    Nothing stops a Google ``sub`` from reading like a Discord snowflake, and
    a single-column key would have quietly made one account the other.
    """
    same = "1234567890"
    an_account(
        monkeypatch,
        discord=oauth.Profile(subject=same, label="from-discord"),
        google=oauth.Profile(subject=same, email="from-google@example.com"),
    )
    async with a_client(an_api()) as client:
        await an_owner(client)
        assert (await connect(client, "discord")).status_code == 303
        assert (await connect(client, "google")).status_code == 303

    async with db() as session:
        rows = list((await session.execute(select(AuthIdentity))).scalars())
    assert {(r.provider, r.subject) for r in rows} == {
        ("discord", same),
        ("google", same),
    }


async def test_unlinking_one_leaves_the_other(monkeypatch, db) -> None:
    an_account(
        monkeypatch,
        discord=oauth.Profile(subject="snowflake-1", label="yite"),
        google=oauth.Profile(subject="google-sub-1", email="yite@example.com"),
    )
    async with a_client(an_api()) as client:
        await an_owner(client)
        await connect(client, "discord")
        await connect(client, "google")
        listed = (await client.get("/auth/identities")).json()["identities"]
        google = next(r for r in listed if r["provider"] == "google")

        await client.delete(f"/auth/identities/{google['id']}")
        after = (await client.get("/auth/identities")).json()["identities"]

    assert [r["provider"] for r in after] == ["password", "discord"]


async def test_a_google_account_nobody_connected_still_cannot_sign_in(
    monkeypatch, db
) -> None:
    """The rule does not weaken by being written twice."""
    an_account(
        monkeypatch, google=oauth.Profile(subject="a-stranger", email="who@example.com")
    )
    async with a_client(an_api()) as client:
        await an_owner(client)

    async with a_client(an_api()) as client:
        started = await client.get("/auth/login/google")
        state = httpx.URL(started.headers["location"]).params["state"]
        done = await client.get(f"/auth/callback/google?state={state}&code=abc")

    assert done.status_code == 403
    async with db() as session:
        users = await session.scalar(select(func.count()).select_from(User))
        identities = await session.scalar(
            select(func.count()).select_from(AuthIdentity)
        )
    assert (users, identities) == (1, 0)


async def test_a_state_from_one_provider_is_not_valid_at_the_other(
    monkeypatch, db
) -> None:
    """The record names its provider, and the callback checks it."""
    an_account(
        monkeypatch,
        discord=oauth.Profile(subject="snowflake-1", label="yite"),
        google=oauth.Profile(subject="google-sub-1"),
    )
    async with a_client(an_api()) as client:
        await an_owner(client)
        started = await client.get("/auth/connect/discord")
        state = httpx.URL(started.headers["location"]).params["state"]

        crossed = await client.get(f"/auth/callback/google?state={state}&code=abc")

    assert crossed.status_code == 403
    async with db() as session:
        count = await session.scalar(select(func.count()).select_from(AuthIdentity))
    assert count == 0


def test_adding_a_provider_is_a_table_entry() -> None:
    """If this ever stops being true, the abstraction has failed."""
    assert set(oauth.PROVIDERS) == {"discord", "google"}
    for provider in oauth.PROVIDERS.values():
        assert provider.authorize_url.startswith("https://")
        assert provider.token_url.startswith("https://")
        assert provider.userinfo_url.startswith("https://")
        assert "email" not in provider.scope or provider.name == "google"
