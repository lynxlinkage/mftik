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
from mftik.protocol import (
    STS_SESSION_LIST,
    ListSessionsResult,
    ListSessionsResultEnvelope,
)
from mftik_api.routes import sts as sts_routes
from mftik_db.models.session import SessionStatus
from mftik_db.repositories import StsSessionRepository


class QuietBroker:
    """Pause-state probe: no live sessions, so the list is the database alone."""

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        return ListSessionsResultEnvelope.wrap(
            ListSessionsResult(sessions=[]),
            type=STS_SESSION_LIST,
            source="sts",
        )


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

    result = await sts_routes.list_strategies(QuietBroker())
    by_id = {row.session_id: row for row in result.strategies}

    assert set(by_id) == {"s-ok", "s-orphan"}
    assert by_id["s-ok"].type == "private::Tiny"
    assert by_id["s-orphan"].type is None
    assert by_id["s-orphan"].status == "failed"
    assert result.has_more is False


class DeadBroker:
    """STS is not answering. History and Attention must still list."""

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        raise AssertionError("STS must not be probed")


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

    result = await sts_routes.list_strategies(
        DeadBroker(), status="failed,interrupted"
    )
    assert {row.session_id for row in result.strategies} == {"s-fail", "s-int"}
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

    first = await sts_routes.list_strategies(
        DeadBroker(), status="done,ack", limit=2
    )
    assert [row.session_id for row in first.strategies] == ["s-new", "s-mid"]
    assert first.has_more is True

    second = await sts_routes.list_strategies(
        DeadBroker(), status="done,ack", before="s-mid", limit=2
    )
    assert [row.session_id for row in second.strategies] == ["s-old"]
    assert second.has_more is False


async def test_history_does_not_touch_the_broker(db) -> None:
    async with db() as session:
        repo = StsSessionRepository(session)
        await repo.create_live(session_id="s-done", created_by=1)
        await repo.mark_done("s-done")

    result = await sts_routes.list_strategies(DeadBroker(), status="done,ack")
    assert [row.session_id for row in result.strategies] == ["s-done"]


async def test_an_unknown_cursor_is_a_422(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.list_strategies(QuietBroker(), before="nope")
    assert caught.value.status_code == 422
    assert "nope" in str(caught.value.detail)


async def test_an_unknown_status_is_a_422(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.list_strategies(QuietBroker(), status="faild")
    assert caught.value.status_code == 422
    assert "faild" in str(caught.value.detail)


async def test_a_status_of_only_commas_is_a_422(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.list_strategies(QuietBroker(), status=" , ")
    assert caught.value.status_code == 422
