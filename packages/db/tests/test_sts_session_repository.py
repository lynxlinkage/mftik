"""STS session terminal statuses — done vs failed, and the failure reason."""

from __future__ import annotations

import pytest
from mft_db.models import Base
from mft_db.models.session import SessionStatus
from mft_db.repositories import StsSessionRepository
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest.fixture
async def db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        yield session
    await engine.dispose()


async def _live(repo: StsSessionRepository, session_id: str) -> None:
    await repo.create_live(
        session_id=session_id, created_by=1, strategy="NoopStrategy"
    )


async def test_a_new_session_starts_live_with_no_reason(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-live")

    row = await repo.get_by_session_id("s-live")
    assert row is not None
    assert row.status == SessionStatus.LIVE.value
    assert row.reason is None
    assert row.finished_at is None


async def test_mark_done_records_no_reason(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-done")

    row = await repo.mark_done("s-done")
    assert row is not None
    assert row.status == SessionStatus.DONE.value
    assert row.reason is None
    assert row.finished_at is not None


async def test_mark_failed_keeps_the_reason(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-failed")

    row = await repo.mark_failed("s-failed", "oco_insufficient_balance")
    assert row is not None
    assert row.status == SessionStatus.FAILED.value
    assert row.reason == "oco_insufficient_balance"
    assert row.finished_at is not None


async def test_a_long_reason_is_truncated_to_the_column_width(db) -> None:
    """SQLite would happily store an over-length string; Postgres would not."""
    repo = StsSessionRepository(db)
    await _live(repo, "s-long")

    row = await repo.mark_failed("s-long", "x" * 500)
    assert row is not None
    assert len(row.reason or "") == 256


async def test_marking_an_unknown_session_is_a_no_op(db) -> None:
    repo = StsSessionRepository(db)
    assert await repo.mark_failed("nope", "gone") is None


async def test_failed_sessions_are_listed_under_their_own_status(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-a")
    await _live(repo, "s-b")
    await repo.mark_done("s-a")
    await repo.mark_failed("s-b", "boom")

    live = await repo.list_sessions(status=SessionStatus.LIVE.value)
    done = await repo.list_sessions(status=SessionStatus.DONE.value)
    failed = await repo.list_sessions(status=SessionStatus.FAILED.value)

    assert [r.session_id for r in live] == []
    assert [r.session_id for r in done] == ["s-a"]
    assert [r.session_id for r in failed] == ["s-b"]
