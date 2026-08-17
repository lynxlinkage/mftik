"""API keys: minted once, scoped below the Owner, revocable.

The two properties worth defending are that the secret leaves exactly once,
and that a key cannot promote itself. A key able to mint keys could not be
contained — revoking it would not remove whatever it issued first, and the
audit trail would call all of it the Owner.
"""

from __future__ import annotations

import pytest
from auth_harness import a_client, an_api, use_database
from db_harness import a_database
from mftik_api.auth import routes as auth_routes
from mftik_db.models.auth import AuthKey
from sqlalchemy import select

GOOD = "correct-horse-battery"


@pytest.fixture(autouse=True)
def _no_throttle() -> None:
    auth_routes._failures.clear()


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setenv("MFT_AUTH_ENABLED", "1")
        yield database.scope


async def an_owner_with_a_key(client, name: str = "ci") -> str:
    created = await client.post(
        "/auth/setup", json={"username": "yite", "password": GOOD}
    )
    assert created.status_code == 201
    minted = await client.post("/auth/keys", json={"name": name})
    assert minted.status_code == 201
    return minted.json()["token"]


async def test_a_key_is_returned_once_and_never_again(db) -> None:
    app = an_api()
    async with a_client(app) as client:
        token = await an_owner_with_a_key(client)
        listed = await client.get("/auth/keys")

    assert token.startswith("mft_ak_")
    body = listed.json()["keys"]
    assert len(body) == 1
    assert "token" not in body[0]
    assert token not in listed.text
    assert body[0]["prefix"].startswith("mft_ak_")
    assert body[0]["prefix"].endswith("…")


async def test_the_database_never_holds_the_secret(db) -> None:
    async with a_client(an_api()) as client:
        token = await an_owner_with_a_key(client)

    async with db() as session:
        row = (await session.execute(select(AuthKey))).scalar_one()
    assert token not in row.key_hash
    assert len(row.key_hash) == 64, "a sha-256 hex digest, not the token"


async def test_a_key_authenticates_a_domain_route(db) -> None:
    app = an_api()
    async with a_client(app) as client:
        token = await an_owner_with_a_key(client)

    async with a_client(app) as client:
        answered = await client.get(
            "/sts/sessions", headers={"Authorization": f"Bearer {token}"}
        )
        me = await client.get(
            "/auth/me", headers={"Authorization": f"Bearer {token}"}
        )

    assert answered.status_code == 200
    assert answered.json() == {"owner": 1}
    # Same Owner, and the trail can still say it was the key that acted.
    assert me.json()["user_id"] == 1
    assert me.json()["via"] == "key:ci"


async def test_a_key_cannot_mint_or_list_keys(db) -> None:
    app = an_api()
    async with a_client(app) as client:
        token = await an_owner_with_a_key(client)

    auth = {"Authorization": f"Bearer {token}"}
    async with a_client(app) as client:
        minted = await client.post("/auth/keys", json={"name": "second"}, headers=auth)
        listed = await client.get("/auth/keys", headers=auth)
        revoked = await client.delete("/auth/keys/1", headers=auth)

    assert minted.status_code == 403
    assert listed.status_code == 403
    assert revoked.status_code == 403
    # 403, not 401: the credential is real. Sending it to /login would be a
    # lie, and the SPA must not treat this as an expired session.
    assert "x-mft-auth" not in minted.headers


async def test_a_revoked_key_stops_working_but_stays_listed(db) -> None:
    app = an_api()
    async with a_client(app) as client:
        token = await an_owner_with_a_key(client)
        key_id = (await client.get("/auth/keys")).json()["keys"][0]["id"]
        gone = await client.delete(f"/auth/keys/{key_id}")
        listed = await client.get("/auth/keys")

    assert gone.status_code == 200
    assert gone.json()["revoked_at"] is not None
    assert len(listed.json()["keys"]) == 1, (
        "a key that stopped working and one that never existed are different"
    )

    async with a_client(app) as client:
        refused = await client.get(
            "/sts/sessions", headers={"Authorization": f"Bearer {token}"}
        )
    assert refused.status_code == 401


async def test_a_token_claiming_the_wrong_kind_is_refused(db) -> None:
    """The wire prefix is attacker-supplied, so it is checked, not trusted."""
    app = an_api()
    async with a_client(app) as client:
        token = await an_owner_with_a_key(client)

    lying = token.replace("mft_ak_", "mft_rk_", 1)
    async with a_client(app) as client:
        refused = await client.get(
            "/sts/sessions", headers={"Authorization": f"Bearer {lying}"}
        )
    assert refused.status_code == 401


@pytest.mark.parametrize(
    "header",
    [
        "Bearer not-a-key-at-all",
        "Bearer mft_ak_short",
        "Basic mft_ak_whatever",
        "Bearer ",
    ],
)
async def test_rubbish_in_authorization_is_just_unauthenticated(
    db, header: str
) -> None:
    async with a_client(an_api()) as client:
        refused = await client.get("/sts/sessions", headers={"Authorization": header})
    assert refused.status_code == 401


async def test_a_bearer_beats_the_cookie_on_the_same_request(db) -> None:
    """Otherwise a script driven from a browser could never act as its key."""
    app = an_api()
    async with a_client(app) as client:
        token = await an_owner_with_a_key(client)
        # The setup cookie is still in this client's jar.
        me = await client.get(
            "/auth/me", headers={"Authorization": f"Bearer {token}"}
        )

    assert me.json()["via"] == "key:ci"


async def test_using_a_key_records_that_it_was_used(db) -> None:
    app = an_api()
    async with a_client(app) as client:
        token = await an_owner_with_a_key(client)
        assert (await client.get("/auth/keys")).json()["keys"][0][
            "last_used_at"
        ] is None

    async with a_client(app) as client:
        await client.get("/sts/sessions", headers={"Authorization": f"Bearer {token}"})

    async with a_client(app) as client:
        await client.post(
            "/auth/login/password", json={"username": "yite", "password": GOOD}
        )
        listed = await client.get("/auth/keys")

    assert listed.json()["keys"][0]["last_used_at"] is not None


async def test_revoking_someone_elses_key_is_a_404(db) -> None:
    async with a_client(an_api()) as client:
        await an_owner_with_a_key(client)
        missing = await client.delete("/auth/keys/9999")
    assert missing.status_code == 404
