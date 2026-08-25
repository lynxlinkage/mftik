"""The sequence ``mftik connect`` performs, against the real auth routes.

The CLI lives in another package and cannot import this one, so its own tests
drive a fake node. That fake is only worth as much as its resemblance to this
app, and nothing in either package fails when the two drift. This is what
fails: the same calls in the same order, reading the same fields, against the
routes themselves.

Named for the client because that is what constrains it. Any of these fields
disappearing is a released CLI that stops being able to log in.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from auth_harness import a_client, an_api, use_database
from db_harness import a_database
from mftik_api.auth import routes as auth_routes

GOOD = "correct-horse-battery"


@pytest.fixture(autouse=True)
def _no_throttle() -> None:
    auth_routes._failures.clear()


@pytest.fixture
async def db(monkeypatch, database_url, tmp_path: Path):
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setenv("MFTIK_AUTH_ENABLED", "1")
        monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
        yield database.scope


async def test_status_carries_what_connect_branches_on(db) -> None:
    """Three fields decide which of three paths the client takes."""
    app = an_api()
    async with a_client(app) as client:
        body = (await client.get("/auth/status")).json()

    assert body["enabled"] is True
    # No owner with a password yet, so this is the "claim it" branch.
    assert body["setup_required"] is True
    # The client checks the password length before spending a second prompt.
    assert isinstance(body["min_password_length"], int)
    assert "username" in body


async def test_claim_then_mint_then_logout(db) -> None:
    """``mftik connect --setup`` end to end.

    The order is the substance: a key can only be minted by a session, and the
    session has to be given back afterwards or it is a second live credential
    that nothing on the client's machine knows exists.
    """
    app = an_api()
    async with a_client(app) as client:
        claimed = await client.post(
            "/auth/setup", json={"username": "yite", "password": GOOD}
        )
        assert claimed.status_code == 201
        assert claimed.json()["user_id"]

        created = await client.post(
            "/auth/keys", json={"name": "mftik-cli@laptop", "kind": "api"}
        )
        assert created.status_code == 201
        token = created.json()["token"]
        assert token.startswith("mftik_ak_")

        assert (await client.post("/auth/logout")).status_code == 200

        # The session is gone...
        assert (await client.get("/auth/me")).status_code == 401

    # ...and the key it minted still works, on a client that has no cookies.
    async with a_client(app) as fresh:
        me = await fresh.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert me.status_code == 200
        assert me.json()["username"] == "yite"
        # ``via`` names the key, not just its kind — which is what makes
        # ``mftik whoami`` able to say *which* credential got you in.
        assert me.json()["via"] == "key:mftik-cli@laptop"


async def test_login_then_mint_on_a_claimed_node(db) -> None:
    """``mftik connect`` against a node somebody already owns."""
    app = an_api()
    async with a_client(app) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})
        await client.post("/auth/logout")

    async with a_client(app) as client:
        status = (await client.get("/auth/status")).json()
        assert status["setup_required"] is False
        # Not to an anonymous caller, it does not (issue #20). The client
        # used to offer this as the default at the username prompt; the
        # prompt simply has no default now, which `_login_and_mint` already
        # handles because a node that has never been claimed has no Owner
        # name to offer either.
        assert status["username"] is None

        signed_in = await client.post(
            "/auth/login/password", json={"username": "yite", "password": GOOD}
        )
        assert signed_in.status_code == 200

        token = (
            await client.post("/auth/keys", json={"name": "cli", "kind": "api"})
        ).json()["token"]
        await client.post("/auth/logout")

    async with a_client(app) as fresh:
        me = await fresh.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert me.status_code == 200


async def test_a_wrong_password_is_a_401_with_a_readable_detail(db) -> None:
    """The client prints this verbatim instead of its own guess."""
    app = an_api()
    async with a_client(app) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})
        await client.post("/auth/logout")

    async with a_client(app) as client:
        refused = await client.post(
            "/auth/login/password", json={"username": "yite", "password": "wrong-one"}
        )

    assert refused.status_code == 401
    assert isinstance(refused.json()["detail"], str)


async def test_claiming_a_claimed_node_is_refused(db) -> None:
    """``--setup`` against somebody else's node must not take it from them."""
    app = an_api()
    async with a_client(app) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})
        await client.post("/auth/logout")

    async with a_client(app) as client:
        again = await client.post(
            "/auth/setup", json={"username": "someone", "password": "another-password"}
        )

    assert again.status_code == 409


async def test_a_minted_key_cannot_mint_another(db) -> None:
    """Why connect signs in at all, rather than reusing a key it already has.

    403 rather than 401, and the client must not turn it into "run connect":
    the credential is real, and presenting it again would fail identically.
    """
    app = an_api()
    async with a_client(app) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})
        token = (
            await client.post("/auth/keys", json={"name": "cli", "kind": "api"})
        ).json()["token"]
        await client.post("/auth/logout")

    async with a_client(app) as fresh:
        refused = await fresh.post(
            "/auth/keys",
            json={"name": "second", "kind": "api"},
            headers={"Authorization": f"Bearer {token}"},
        )

    assert refused.status_code == 403


async def test_health_is_reachable_without_a_credential(db) -> None:
    """What ``probe`` uses to find the API before there is anything to send."""
    app = an_api()
    async with a_client(app) as client:
        assert (await client.get("/health")).status_code == 200
