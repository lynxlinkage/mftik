"""Audit log — newest first, then older than an id cursor."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from db_harness import a_database, an_owner
from mftik_db.repositories import AuditRepository


@pytest.fixture
async def db(database_url):
    async with a_database(database_url) as database, database.maker() as session:
        await an_owner(session)
        await session.commit()
        yield session


async def test_list_recent_pages_on_offset(db) -> None:
    repo = AuditRepository(db)
    origin = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    for minutes, operation in enumerate(("old", "mid", "new")):
        row = await repo.record(user_id=1, operation=operation, result="ok")
        row.created_at = origin + timedelta(minutes=minutes)

    first = await repo.list_recent(limit=2)
    assert [row.operation for row in first] == ["new", "mid"]
    assert await repo.count() == 3

    rest = await repo.list_recent(limit=2, offset=2)
    assert [row.operation for row in rest] == ["old"]
