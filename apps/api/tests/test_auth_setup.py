"""Claiming the instance, and logging into it afterwards.

The case worth the most here is the second one: a ``users`` row that exists
with no password. That is not an edge case, it is what every deployment and
every local stack already looks like — ``seed`` creates the Owner so foreign
keys resolve, and local compose runs it before the API starts. Setup gated on
"users is empty" would 409 on a stack nobody has ever logged into.
"""

from __future__ import annotations

import pytest
from auth_harness import a_client, an_api, use_database
from db_harness import a_database, an_owner
from mftik_api.auth import passwords, sessions
from mftik_api.auth import routes as auth_routes
from mftik_db.models import Api, StsSessionRow
from mftik_db.models.api import ApiType
from mftik_db.models.user import User
from mftik_db.repositories import UserRepository
from sqlalchemy import func, select

GOOD = "correct-horse-battery"


@pytest.fixture(autouse=True)
def _no_throttle() -> None:
    """The failure counter is process-global; a test must not inherit one."""
    auth_routes._failures.clear()


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setenv("MFT_AUTH_ENABLED", "1")
        yield database.scope


async def a_seeded_owner(scope) -> int:
    """What ``seed`` leaves behind: a user row and things pointing at it."""
    async with scope() as session:
        owner = await an_owner(session)
        session.add(
            Api(
                owner_id=owner.id,
                venue="paper",
                api_key="paper-key-1",
                api_secret="paper-secret-1",
                type=ApiType.HMAC.value,
            )
        )
        return owner.id


async def test_setup_on_an_empty_database_creates_the_owner(db) -> None:
    async with a_client(an_api()) as client:
        status = await client.get("/auth/status")
        assert status.json()["setup_required"] is True

        created = await client.post(
            "/auth/setup", json={"username": "yite", "password": GOOD}
        )

    assert created.status_code == 201
    assert created.json()["username"] == "yite"
    assert sessions.COOKIE_NAME in created.cookies

    async with db() as session:
        owner = await UserRepository(session).get_owner()
        assert owner is not None and owner.password_hash is not None


async def test_setup_fills_in_the_row_seed_left_and_adds_no_second_owner(
    db,
) -> None:
    owner_id = await a_seeded_owner(db)

    async with a_client(an_api()) as client:
        status = await client.get("/auth/status")
        assert status.json()["setup_required"] is True, (
            "a passwordless owner is un-set-up, not already claimed"
        )

        created = await client.post(
            "/auth/setup", json={"username": "yite", "password": GOOD}
        )

    assert created.status_code == 201
    assert created.json()["user_id"] == owner_id, "setup adopted the seeded row"

    async with db() as session:
        assert await session.scalar(select(func.count()).select_from(Api)) == 1, (
            "the seeded API still belongs to somebody"
        )
        owner = await UserRepository(session).get_owner()
        assert owner is not None
        assert owner.id == owner_id
        assert owner.username == "yite"


async def test_a_second_setup_is_refused(db) -> None:
    async with a_client(an_api()) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})
        again = await client.post(
            "/auth/setup", json={"username": "someone", "password": GOOD}
        )

    assert again.status_code == 409
    async with db() as session:
        rows = await session.scalar(select(func.count()).select_from(User))
    assert rows == 1


async def test_status_stops_asking_for_setup_once_there_is_a_password(db) -> None:
    async with a_client(an_api()) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})
        status = await client.get("/auth/status")

    body = status.json()
    assert body["setup_required"] is False
    assert body["username"] == "yite"
    assert body["providers"] == ["password"]


async def test_the_minimum_password_length_is_where_it_says_it_is(db) -> None:
    """One character either side of the rule, and the rule as published.

    The login form reads `min_password_length` rather than hard-coding it, so
    a change that moved one without the other would show up here.
    """
    async with a_client(an_api()) as client:
        status = (await client.get("/auth/status")).json()
        published = status["min_password_length"]

        short = await client.post(
            "/auth/setup",
            json={"username": "yite", "password": "x" * (published - 1)},
        )
        exact = await client.post(
            "/auth/setup",
            json={"username": "yite", "password": "x" * published},
        )

    assert published == passwords.MIN_LENGTH == 8
    assert short.status_code == 422
    assert exact.status_code == 201


async def test_login_takes_the_password_and_refuses_the_wrong_one(db) -> None:
    await a_seeded_owner(db)
    app = an_api()

    async with a_client(app) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})

    async with a_client(app) as client:
        wrong = await client.post(
            "/auth/login/password", json={"username": "yite", "password": "wrong-one"}
        )
        assert wrong.status_code == 401
        assert sessions.COOKIE_NAME not in wrong.cookies

        right = await client.post(
            "/auth/login/password", json={"username": "yite", "password": GOOD}
        )
        assert right.status_code == 200
        assert sessions.COOKIE_NAME in right.cookies

        me = await client.get("/auth/me")
        assert me.status_code == 200
        assert me.json()["via"] == "password"


async def test_an_unknown_username_is_the_same_answer_as_a_wrong_password(
    db,
) -> None:
    async with a_client(an_api()) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})

    async with a_client(an_api()) as client:
        nobody = await client.post(
            "/auth/login/password", json={"username": "nobody", "password": GOOD}
        )

    assert nobody.status_code == 401
    assert nobody.json()["detail"] == "invalid credentials"


async def test_logout_ends_the_session_the_cookie_named(db) -> None:
    app = an_api()
    async with a_client(app) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})
        assert (await client.get("/sts/sessions")).status_code == 200

        await client.post("/auth/logout")

        # The cookie is cleared, but the row being gone is the part that
        # matters — a copy of the token taken beforehand is now worthless.
        assert (await client.get("/auth/me")).status_code == 401


async def test_a_revoked_session_does_not_come_back_with_the_cookie(db) -> None:
    app = an_api()
    async with a_client(app) as client:
        await client.post("/auth/setup", json={"username": "yite", "password": GOOD})
        token = client.cookies[sessions.COOKIE_NAME]
        await client.post("/auth/logout")

    async with a_client(app) as client:
        client.cookies.set(sessions.COOKIE_NAME, token, domain="mft.test")
        replayed = await client.get("/sts/sessions")

    assert replayed.status_code == 401


async def test_sessions_survive_a_row_the_owner_created(db) -> None:
    """A session is not a strategy session. The names collide; the tables must not."""
    owner_id = await a_seeded_owner(db)
    async with db() as session:
        session.add(
            StsSessionRow(
                session_id="s-1",
                created_by=owner_id,
                status="live",
                strategy="twap",
                td_api_ids=[],
                md_ids=[],
                st_paras={},
                st_facts={},
            )
        )

    async with a_client(an_api()) as client:
        created = await client.post(
            "/auth/setup", json={"username": "yite", "password": GOOD}
        )
    assert created.status_code == 201

    async with db() as session:
        still = await session.scalar(
            select(func.count()).select_from(StsSessionRow)
        )
    assert still == 1
