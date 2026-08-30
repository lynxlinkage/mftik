"""GET /sts/strategies — the list is every STS session, not just successful deploys.

Attach failures persist a session and never record ``type`` / ``yaml_text``.
The list used to be driven by a sidecar table written only after deploy
succeeded, so those rows vanished. Pinning the endpoint is what keeps a
later ``WHERE type IS NOT NULL`` from bringing that back silently.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from db_harness import a_database, an_owner
from fastapi import HTTPException
from mftik_api.routes import sts as sts_routes
from mftik_db.models.session import SessionStatus
from mftik_db.repositories import StsSessionRepository


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.scope() as session:
            await an_owner(session)
        monkeypatch.setattr(sts_routes, "session_scope", database.scope)
        yield database.scope


async def test_an_attach_failure_still_appears_on_the_list(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(
            session_id="s-ok",
            created_by=1,
            strategy="tiny",
            type="private::Tiny",
            yaml_text="sts: {}\n",
        )
        await repo.create_live(session_id="s-orphan", created_by=1, strategy="tiny")
        await repo.mark_failed(
            "s-orphan", "attach failed — rolled back during deploy"
        )

    result = await sts_routes.list_strategies()
    by_id = {row.session_id: row for row in result.strategies}

    assert set(by_id) == {"s-ok", "s-orphan"}
    assert by_id["s-ok"].type == "private::Tiny"
    assert by_id["s-orphan"].type is None
    assert by_id["s-orphan"].status == "failed"
    assert by_id["s-ok"].td_api_ids == []
    assert by_id["s-ok"].md_ids == []
    assert result.has_more is False
    assert all("paused" not in row.model_dump() for row in result.strategies)


async def test_the_list_carries_attaches_from_the_row(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(
            session_id="s-att",
            created_by=1,
            type="NoopStrategy",
            td={"a": {"api_id": 3}, "b": {"api_id": 7}},
            md_ids=["orderbook.Paper_Spot_BTCUSDT"],
        )

    result = await sts_routes.list_strategies()
    row = result.strategies[0]
    assert row.session_id == "s-att"
    assert row.td_api_ids == [3, 7]
    assert row.md_ids == ["orderbook.Paper_Spot_BTCUSDT"]


async def test_one_session_is_the_database_row(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(
            session_id="s-one",
            created_by=1,
            type="NoopStrategy",
            td={"a": {"api_id": 2}},
            md_ids=["ticker.Paper_Spot_ETHUSDT"],
        )

    row = await sts_routes.get_strategy("s-one")
    assert row.session_id == "s-one"
    assert row.type == "NoopStrategy"
    assert row.td_api_ids == [2]
    assert row.md_ids == ["ticker.Paper_Spot_ETHUSDT"]


async def test_a_missing_session_is_a_404(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.get_strategy("nope")
    assert caught.value.status_code == 404
    assert "nope" in str(caught.value.detail)


async def test_attention_is_only_failed_and_interrupted(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(session_id="s-live", created_by=1)
        await repo.create_live(session_id="s-fail", created_by=1)
        await repo.mark_failed("s-fail", "boom")
        await repo.create_live(session_id="s-int", created_by=1)
        await repo.mark_finished(
            "s-int", status=SessionStatus.INTERRUPTED.value, reason="cut"
        )
        await repo.create_live(session_id="s-done", created_by=1)
        await repo.mark_done("s-done")

    result = await sts_routes.list_strategies(status="failed,interrupted")
    assert {row.session_id for row in result.strategies} == {"s-fail", "s-int"}
    assert result.has_more is False


async def test_live_is_the_database_alone(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(session_id="s-live", created_by=1)
        await repo.create_live(session_id="s-done", created_by=1)
        await repo.mark_done("s-done")

    result = await sts_routes.list_strategies(status="live")
    assert [row.session_id for row in result.strategies] == ["s-live"]
    assert result.has_more is False


async def test_the_list_pages_on_a_session_cursor(db) -> None:
    origin = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    async with db() as session:
        repo = StsSessionRepository(session)
        for offset, session_id in enumerate(("s-old", "s-mid", "s-new")):
            await repo.create_live(session_id=session_id, created_by=1)
            row = await repo.get_by_session_id(session_id)
            assert row is not None
            row.created_at = origin + timedelta(minutes=offset)
            await repo.mark_done(session_id)
            # mark_done does not touch created_at; keep the stamp we just set.
            row.created_at = origin + timedelta(minutes=offset)

    first = await sts_routes.list_strategies(status="done,ack", limit=2)
    assert [row.session_id for row in first.strategies] == ["s-new", "s-mid"]
    assert first.has_more is True

    second = await sts_routes.list_strategies(
        status="done,ack", before="s-mid", limit=2
    )
    assert [row.session_id for row in second.strategies] == ["s-old"]
    assert second.has_more is False


async def test_an_unknown_cursor_is_a_422(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.list_strategies(before="nope")
    assert caught.value.status_code == 422
    assert "nope" in str(caught.value.detail)


async def test_an_unknown_status_is_a_422(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.list_strategies(status="faild")
    assert caught.value.status_code == 422
    assert "faild" in str(caught.value.detail)


async def test_a_status_of_only_commas_is_a_422(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.list_strategies(status=" , ")
    assert caught.value.status_code == 422
