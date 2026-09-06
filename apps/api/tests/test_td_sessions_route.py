"""/td/sessions answers from the database, with no TD process anywhere.

It used to be an RPC on TD's shared subject, and the process that answered it
held no state the answer needed — `SessionManager.list_sessions` read
`td_sessions` and nothing else. Removing that round trip is what lets TD stop
serving an anycast subject at all: everything else that reaches TD carries an
`api_id`, and an `api_id` names the one instance allowed to use that
credential.

The app under test has no `state.broker`, which is the assertion: if this route
still needed a plane, it could not be built this way.
"""

from __future__ import annotations

import pytest
from auth_harness import a_client
from db_harness import a_database, an_instance, an_owner
from fastapi import FastAPI
from mftik_api.routes import td as td_routes
from mftik_api.routes.td import router as td_router
from mftik_db.models.api import Api
from mftik_db.models.session import SessionStatus, TdSessionRow
from mftik_db.repositories import AccountRepository


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.maker() as session:
            owner = await an_owner(session)
            instance = await an_instance(session)
            api = Api(
                owner_id=owner.id,
                venue="Paper",
                api_key="k1",
                api_secret="s1",
                type="HMAC",
                instance_id=instance.id,
            )
            session.add(api)
            await session.flush()
            await AccountRepository(session).create(
                name="paper trader", api_id=api.id, created_by=owner.id
            )
            session.add(
                TdSessionRow(
                    session_id="sess-1",
                    api_id=api.id,
                    created_by=owner.id,
                    status=SessionStatus.LIVE.value,
                )
            )
            session.add(
                TdSessionRow(
                    session_id="sess-2",
                    api_id=api.id,
                    created_by=owner.id,
                    status=SessionStatus.DONE.value,
                )
            )
            await session.commit()
        monkeypatch.setattr(td_routes, "session_scope", database.scope)
        monkeypatch.setenv("MFTIK_AUTH_ENABLED", "0")
        yield database.scope


def _app() -> FastAPI:
    """No ``state.broker``. A route that needed one would fail to answer."""
    app = FastAPI()
    app.include_router(td_router)
    return app


async def test_live_attaches_are_listed_without_a_td_process(db) -> None:
    async with a_client(_app()) as client:
        res = await client.get("/td/sessions")

    assert res.status_code == 200, res.text
    rows = res.json()["sessions"]
    assert [r["session_id"] for r in rows] == ["sess-1"]


async def test_the_row_carries_its_account_label(db) -> None:
    """What the RPC used to join in on the way back."""
    async with a_client(_app()) as client:
        rows = (await client.get("/td/sessions")).json()["sessions"]

    assert rows[0]["venue"] == "Paper"
    assert rows[0]["api_name"] == "paper trader"
    assert rows[0]["domain"] == "td"
    assert rows[0]["sts_session_id"] == "sess-1"


async def test_status_selects_which_rows_come_back(db) -> None:
    async with a_client(_app()) as client:
        done = (await client.get("/td/sessions?status=done")).json()["sessions"]

    assert [r["session_id"] for r in done] == ["sess-2"]
