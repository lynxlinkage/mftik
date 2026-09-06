"""/instances — declare, annotate, retire. There is no rename.

The route surface is small; what it has to get right is what it *refuses*. A
name cannot be edited, because a process reads its own from ``MFTIK_INSTANCE``
in an environment this service cannot write — a row renamed here would leave
the row and the process disagreeing with nothing able to reconcile them. And an
instance a credential still names cannot be retired.
"""

from __future__ import annotations

import pytest
from auth_harness import a_client
from db_harness import a_database, an_instance, an_owner
from fastapi import FastAPI
from mftik_api.auth import AuthMiddleware
from mftik_api.routes import apis as apis_routes
from mftik_api.routes import instances as instances_routes
from mftik_api.routes.apis import router as apis_router
from mftik_api.routes.instances import router as instances_router
from mftik_db.models.session import MdSessionRow, SessionStatus, StsSessionRow


@pytest.fixture
async def db(monkeypatch, database_url):
    async def _no_audit(**_kwargs: object) -> None:
        return None

    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            await an_instance(session)
            await session.commit()
        for module in (instances_routes, apis_routes):
            monkeypatch.setattr(module, "session_scope", database.scope)
            monkeypatch.setattr(module, "record_audit", _no_audit)
        monkeypatch.setenv("MFTIK_AUTH_ENABLED", "0")
        yield database.scope


def _app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(instances_router)
    app.include_router(apis_router)
    return app


async def test_the_seeded_td_instance_is_listed(db) -> None:
    async with a_client(_app()) as client:
        res = await client.get("/instances")

    assert res.status_code == 200
    names = [row["name"] for row in res.json()["instances"]]
    assert names == ["td"]


async def test_declaring_an_instance_does_not_start_anything(db) -> None:
    """A row is a statement of intent. Whether it answers is `/stats`'s job."""
    async with a_client(_app()) as client:
        made = await client.post(
            "/instances",
            json={"name": "md-jp-1", "domain": "md", "region": "ap-northeast-1"},
        )
        listed = await client.get("/instances?domain=md")

    assert made.status_code == 201, made.text
    body = made.json()
    assert body["name"] == "md-jp-1"
    assert body["domain"] == "md"
    assert body["region"] == "ap-northeast-1"
    assert body["enabled"] is True
    assert [row["name"] for row in listed.json()["instances"]] == ["md-jp-1"]


async def test_a_duplicate_name_is_refused(db) -> None:
    async with a_client(_app()) as client:
        again = await client.post(
            "/instances", json={"name": "td", "domain": "td"}
        )

    assert again.status_code == 409
    assert "td" in again.json()["detail"]


@pytest.mark.parametrize("domain", ["sym", "paper", "nonsense"])
async def test_only_td_md_sts_may_be_instanced(db, domain: str) -> None:
    """SYM is cached off the hot path and paper exists to be one shared book."""
    async with a_client(_app()) as client:
        res = await client.post(
            "/instances", json={"name": f"x-{domain}", "domain": domain}
        )

    assert res.status_code == 400
    assert domain in res.json()["detail"]


async def test_patch_edits_the_label_and_cannot_rename(db) -> None:
    """``region`` and ``enabled`` move. ``name`` is not a field at all.

    ``extra`` is ignored by the model rather than rejected, so the assertion
    that matters is the one on the row afterwards: a client that sends a name
    does not get one.
    """
    async with a_client(_app()) as client:
        listed = await client.get("/instances")
        instance_id = listed.json()["instances"][0]["id"]

        renamed = await client.patch(
            f"/instances/{instance_id}",
            json={"name": "td-jp-1", "region": "ap-northeast-1"},
        )

    assert renamed.status_code == 200, renamed.text
    body = renamed.json()
    assert body["region"] == "ap-northeast-1", "the label moves"
    assert body["name"] == "td", "the address does not"


async def test_patch_can_drain_without_evicting(db) -> None:
    async with a_client(_app()) as client:
        listed = await client.get("/instances")
        instance_id = listed.json()["instances"][0]["id"]
        res = await client.patch(
            f"/instances/{instance_id}", json={"enabled": False}
        )

    assert res.status_code == 200
    assert res.json()["enabled"] is False


async def test_patching_an_unknown_instance_is_404(db) -> None:
    async with a_client(_app()) as client:
        res = await client.patch("/instances/9999", json={"region": "x"})

    assert res.status_code == 404


async def test_an_unused_instance_can_be_retired(db) -> None:
    async with a_client(_app()) as client:
        made = await client.post(
            "/instances", json={"name": "td-jp-1", "domain": "td"}
        )
        gone = await client.delete(f"/instances/{made.json()['id']}")
        listed = await client.get("/instances")

    assert gone.status_code == 200
    assert gone.json()["deleted"] is True
    assert [row["name"] for row in listed.json()["instances"]] == ["td"]


async def test_a_credential_names_its_td_instance(db) -> None:
    """``apis.instance_id`` is NOT NULL and defaults to ``td``."""
    async with a_client(_app()) as client:
        made = await client.post(
            "/apis",
            json={
                "name": "paper trader",
                "venue": "Paper",
                "api_key": "k1",
                "api_secret": "s1",
            },
        )

    assert made.status_code == 201, made.text
    assert made.json()["instance"] == "td"


async def test_a_credential_cannot_name_an_md_instance(db) -> None:
    async with a_client(_app()) as client:
        await client.post("/instances", json={"name": "md-jp-1", "domain": "md"})
        made = await client.post(
            "/apis",
            json={
                "name": "paper trader",
                "venue": "Paper",
                "api_key": "k1",
                "api_secret": "s1",
                "instance": "md-jp-1",
            },
        )

    assert made.status_code == 400
    assert "td" in made.json()["detail"]


async def test_a_credential_cannot_name_an_instance_that_is_not_declared(
    db,
) -> None:
    async with a_client(_app()) as client:
        made = await client.post(
            "/apis",
            json={
                "name": "paper trader",
                "venue": "Paper",
                "api_key": "k1",
                "api_secret": "s1",
                "instance": "td-jp-9",
            },
        )

    assert made.status_code == 400
    assert "td-jp-9" in made.json()["detail"]


async def test_an_instance_a_live_md_session_names_cannot_be_retired(
    db,
) -> None:
    """The half the database cannot enforce.

    ``md_sessions.instance`` is a plain string on purpose — it is history, and
    retiring an instance must not break the record of what it did. But a
    session that is *still running* is not history, and retiring the instance
    it is attached to would leave a live feed with no declared owner.
    """
    async with a_client(_app()) as client:
        made = await client.post(
            "/instances", json={"name": "md-jp-1", "domain": "md"}
        )

    async with db() as session:
        session.add(
            MdSessionRow(
                instance="md-jp-1",
                venue="Bybit",
                session_id="running",
                created_by=1,
                status=SessionStatus.LIVE.value,
            )
        )

    async with a_client(_app()) as client:
        refused = await client.delete(f"/instances/{made.json()['id']}")

    assert refused.status_code == 409
    assert "live" in refused.json()["detail"]
    assert "md-jp-1" in refused.json()["detail"]


async def test_a_finished_session_does_not_block_retirement(db) -> None:
    """History is exactly what must not block it.

    A row naming a retired instance is the record of what that instance did,
    and keeping it is why the column is a string rather than a foreign key.
    """
    async with a_client(_app()) as client:
        made = await client.post(
            "/instances", json={"name": "md-jp-2", "domain": "md"}
        )

    async with db() as session:
        session.add(
            MdSessionRow(
                instance="md-jp-2",
                venue="Bybit",
                session_id="finished",
                created_by=1,
                status=SessionStatus.DONE.value,
            )
        )

    async with a_client(_app()) as client:
        gone = await client.delete(f"/instances/{made.json()['id']}")

    assert gone.status_code == 200


async def test_an_sts_instance_is_checked_against_sts_sessions(db) -> None:
    """A name belongs to one plane, so the wrong table is not consulted.

    Counting ``md_sessions`` against an STS instance would refuse a delete for
    a reason that is not true.
    """
    async with a_client(_app()) as client:
        made = await client.post(
            "/instances", json={"name": "sts-tw", "domain": "sts"}
        )

    async with db() as session:
        session.add(
            StsSessionRow(
                session_id="pinned-run",
                created_by=1,
                instance="sts-tw",
                status=SessionStatus.LIVE.value,
            )
        )

    async with a_client(_app()) as client:
        refused = await client.delete(f"/instances/{made.json()['id']}")

    assert refused.status_code == 409
    assert "sts session" in refused.json()["detail"]


async def test_the_wrong_table_does_not_refuse_a_delete(db) -> None:
    """A name belongs to one plane, and only that plane's rows count.

    ``md-lonely`` is an MD instance with no live MD attach. The live
    ``sts_sessions`` row below names the same string, but it describes some
    STS that happened to be called that — refusing on it would block a
    retirement for a reason that is not true.

    Written this way on purpose: a version that queries both tables passes
    every other test in this file and fails only here.
    """
    async with a_client(_app()) as client:
        made = await client.post(
            "/instances", json={"name": "md-lonely", "domain": "md"}
        )

    async with db() as session:
        session.add(
            StsSessionRow(
                session_id="a-different-plane",
                created_by=1,
                instance="md-lonely",
                status=SessionStatus.LIVE.value,
            )
        )

    async with a_client(_app()) as client:
        gone = await client.delete(f"/instances/{made.json()['id']}")

    assert gone.status_code == 200


async def test_a_td_instance_is_left_to_the_foreign_key(db) -> None:
    """TD has no session row of its own naming it.

    An attach is named by ``apis.instance_id``, which is ``RESTRICT`` — so an
    unused TD instance retires cleanly and a used one is refused by the
    database, not by the count above.
    """
    async with a_client(_app()) as client:
        made = await client.post(
            "/instances", json={"name": "td-jp-9", "domain": "td"}
        )
        gone = await client.delete(f"/instances/{made.json()['id']}")

    assert gone.status_code == 200
