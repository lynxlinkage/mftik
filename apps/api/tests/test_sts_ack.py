"""POST /sts/sessions/{id}/ack — operator acknowledgement of an abnormal stop."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from db_harness import a_database, an_owner
from fastapi import HTTPException
from mftik_api.routes import sts as sts_routes
from mftik_db.models import StsSessionRow
from mftik_db.models.session import SessionStatus

START = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)


class FakeBroker:
    def __init__(self) -> None:
        self.published: list[tuple[str, object]] = []

    async def publish_log(self, topic: str, envelope: object, **_kwargs: object) -> int:
        self.published.append((topic, envelope))
        return 1


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.scope() as session:
            await an_owner(session)

        async def _audit(**_kwargs: object) -> None:
            return None

        monkeypatch.setattr(sts_routes, "session_scope", database.scope)
        monkeypatch.setattr(sts_routes, "record_audit", _audit)
        yield database.scope


async def a_session(
    scope, session_id: str, *, status: str, reason: str | None = None
) -> None:
    async with scope() as session:
        session.add(
            StsSessionRow(
                session_id=session_id,
                created_by=1,
                created_at=START,
                finished_at=START + timedelta(minutes=10),
                status=status,
                reason=reason,
                strategy="twap",
                td_api_ids=[],
                md_ids=[],
                st_paras={},
                st_facts={},
            )
        )


async def test_ack_turns_failed_into_ack(db) -> None:
    await a_session(db, "s-fail", status="failed", reason="boom")
    broker = FakeBroker()

    result = await sts_routes.ack_session("s-fail", broker)

    assert result.status == SessionStatus.ACK.value
    assert result.reason == "boom"
    assert result.session_id == "s-fail"
    assert broker.published


async def test_ack_of_a_live_session_is_a_409(db) -> None:
    await a_session(db, "s-live", status="live")
    with pytest.raises(HTTPException) as caught:
        await sts_routes.ack_session("s-live", FakeBroker())
    assert caught.value.status_code == 409


async def test_ack_of_a_missing_session_is_a_404(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await sts_routes.ack_session("nope", FakeBroker())
    assert caught.value.status_code == 404
