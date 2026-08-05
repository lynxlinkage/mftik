"""STS session terminal statuses — done vs failed, and the failure reason."""

from __future__ import annotations

import pytest
from mft_db.models import Base
from mft_db.models.session import SessionStatus
from mft_db.repositories import (
    MdSessionRepository,
    StsSessionRepository,
    TdSessionRepository,
)
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


async def test_the_cid_slot_is_kept(db) -> None:
    """A rebuilt session has to mint order ids in the same slot.

    `Strategy.owns()` matches orders by slot, so a session that came back with
    a new one would not recognise the orders it placed before the restart.
    """
    repo = StsSessionRepository(db)
    await repo.create_live(
        session_id="s-slot", created_by=1, strategy="oco", cid_slot=4242
    )

    row = await repo.get_by_session_id("s-slot")
    assert row is not None
    assert row.cid_slot == 4242
    assert row.st_facts == {}


async def test_mark_live_undoes_the_ending(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-back")
    await repo.mark_finished(
        "s-back",
        status=SessionStatus.INTERRUPTED.value,
        reason="STS shut down while this was running",
    )

    row = await repo.mark_live("s-back")
    assert row is not None
    assert row.status == SessionStatus.LIVE.value
    # A session that is running again has no end and no reason for one.
    assert row.finished_at is None
    assert row.reason is None


async def test_remember_accumulates_facts(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-facts")

    await repo.remember("s-facts", "ref_start", "50000")
    await repo.remember("s-facts", "started_ms", "1785000000000")
    await repo.remember("s-facts", "ref_start", "50001")

    # Read it back from the database, not from the object we just wrote
    # through: a plain JSON column does not track in-place mutation, so an
    # implementation that updated the dict in place would pass any assertion
    # made against the live instance and still persist nothing.
    db.expire_all()
    row = await repo.get_by_session_id("s-facts")
    assert row is not None
    assert row.st_facts == {"ref_start": "50001", "started_ms": "1785000000000"}


async def test_remembering_for_an_unknown_session_is_a_no_op(db) -> None:
    repo = StsSessionRepository(db)
    assert await repo.remember("nope", "k", "v") is None


async def test_td_attach_survives_a_detach_and_reattach(db) -> None:
    """Rebuilding a session re-attaches the same (session_id, api_id) pair.

    The pair is unique and a detach only marks the row done, so the second
    attach has to revive that row rather than insert beside it.
    """
    repo = TdSessionRepository(db)
    first = await repo.attach_live(session_id="s-td", created_by=1, api_id=9)
    await repo.mark_done(session_id="s-td", api_id=9)

    again = await repo.attach_live(session_id="s-td", created_by=1, api_id=9)

    assert again.id == first.id
    assert again.status == SessionStatus.LIVE.value
    assert again.finished_at is None


async def test_md_attach_survives_a_detach_and_reattach(db) -> None:
    repo = MdSessionRepository(db)
    first = await repo.attach_live(
        venue="paper", session_id="s-md", created_by=1
    )
    await repo.mark_done(venue="paper", session_id="s-md")

    again = await repo.attach_live(
        venue="paper", session_id="s-md", created_by=1
    )

    assert again.id == first.id
    assert again.status == SessionStatus.LIVE.value
    assert again.finished_at is None
