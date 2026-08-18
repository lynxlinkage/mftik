"""Live sessions for a registry origin — used to refuse a disconnect."""

from __future__ import annotations

import pytest
from db_harness import a_database, an_owner
from mftik_db.models.session import SessionStatus
from mftik_db.repositories import StsSessionRepository


@pytest.fixture
async def db(database_url):
    async with a_database(database_url) as database, database.maker() as session:
        await an_owner(session)
        await session.commit()
        yield session


async def _session(
    db,
    *,
    session_id: str,
    type: str,
    status: str = SessionStatus.LIVE.value,
) -> None:
    sts = StsSessionRepository(db)
    await sts.create_live(
        session_id=session_id, created_by=1, strategy=type, type=type
    )
    if status != SessionStatus.LIVE.value:
        await sts.mark_finished(session_id, status=status)


async def test_live_rows_match_the_origin_not_a_prefix(db) -> None:
    await _session(db, session_id="s-a", type="node1::Tiny")
    await _session(db, session_id="s-b", type="node10::Tiny")
    await _session(db, session_id="s-done", type="node1::Other", status="done")

    repo = StsSessionRepository(db)
    assert [row.session_id for row in await repo.list_live_for_origin("node1")] == [
        "s-a"
    ]
    assert [row.session_id for row in await repo.list_live_for_origin("node10")] == [
        "s-b"
    ]
    assert await repo.list_live_for_origin("missing") == []
