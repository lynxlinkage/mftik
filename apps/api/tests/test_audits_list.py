"""GET /audits — numbered pages over the append-only log."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from db_harness import a_database, an_owner
from mftik_api.routes import audits as audits_routes
from mftik_db.repositories import AuditRepository


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.scope() as session:
            await an_owner(session)
        monkeypatch.setattr(audits_routes, "session_scope", database.scope)
        yield database.scope


async def _write(scope, *, operation: str, at: datetime) -> int:
    async with scope() as session:
        row = await AuditRepository(session).record(
            user_id=1, operation=operation, result="ok"
        )
        row.created_at = at
        return row.id


async def test_the_list_pages_on_offset(db) -> None:
    origin = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    await _write(db, operation="op.old", at=origin)
    await _write(db, operation="op.mid", at=origin + timedelta(minutes=1))
    await _write(db, operation="op.new", at=origin + timedelta(minutes=2))

    first = await audits_routes.list_audits(limit=2)
    assert [row.operation for row in first.audits] == ["op.new", "op.mid"]
    assert first.total == 3
    assert first.has_more is True

    second = await audits_routes.list_audits(offset=2, limit=2)
    assert [row.operation for row in second.audits] == ["op.old"]
    assert second.total == 3
    assert second.has_more is False


async def test_a_short_list_is_not_paged(db) -> None:
    await _write(
        db,
        operation="op.one",
        at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    )

    result = await audits_routes.list_audits(limit=2)
    assert [row.operation for row in result.audits] == ["op.one"]
    assert result.total == 1
    assert result.has_more is False
