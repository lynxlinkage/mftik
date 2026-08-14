"""Session log persistence — bulk insert ignore + cursor pagination."""

from __future__ import annotations

import pytest
from db_harness import a_database
from mft_db.repositories import SessionLogRepository


@pytest.fixture
async def db():
    async with a_database() as database, database.maker() as session:
        yield session


def _row(
    envelope_id: str,
    *,
    ts: float,
    message: str = "hi",
    domain: str = "sts",
    stream_id: str = "sess-1",
) -> dict:
    return {
        "envelope_id": envelope_id,
        "domain": domain,
        "stream_id": stream_id,
        "source": "sts",
        "level": "info",
        "message": message,
        "ts": ts,
    }


async def test_bulk_insert_ignore_dedupes_envelope_id(db) -> None:
    repo = SessionLogRepository(db)
    await repo.bulk_insert_ignore(
        [
            _row("e1", ts=1.0, message="a"),
            _row("e2", ts=2.0, message="b"),
        ]
    )
    await repo.bulk_insert_ignore(
        [
            _row("e1", ts=1.0, message="a-dup"),
            _row("e3", ts=3.0, message="c"),
        ]
    )
    await db.commit()

    rows = await repo.list_before("sts", "sess-1", limit=10)
    assert [r.envelope_id for r in rows] == ["e3", "e2", "e1"]
    assert rows[2].message == "a"


async def test_list_before_cursor_pagination(db) -> None:
    repo = SessionLogRepository(db)
    await repo.bulk_insert_ignore(
        [
            _row("e1", ts=1.0),
            _row("e2", ts=2.0),
            _row("e3", ts=2.0),
            _row("e4", ts=3.0),
        ]
    )
    await db.commit()

    newest = await repo.list_before("sts", "sess-1", limit=2)
    assert [r.envelope_id for r in newest] == ["e4", "e3"]

    page = await repo.list_before(
        "sts",
        "sess-1",
        before_ts=newest[-1].ts,
        before_id=newest[-1].id,
        limit=2,
    )
    assert [r.envelope_id for r in page] == ["e2", "e1"]

    older = await repo.list_before(
        "sts",
        "sess-1",
        before_ts=page[-1].ts,
        before_id=page[-1].id,
        limit=2,
    )
    assert older == []
