"""STS session terminal statuses — done vs failed, and the failure reason."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from db_harness import a_database, an_owner
from mftik_db.models.session import SessionStatus
from mftik_db.repositories import (
    MdSessionRepository,
    StsSessionRepository,
    TdSessionRepository,
)


@pytest.fixture
async def db(database_url):
    async with a_database(database_url) as database, database.maker() as session:
        # Every session row names a creator, and that column is a foreign key.
        await an_owner(session)
        await session.commit()
        yield session


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


async def test_list_sessions_accepts_several_statuses(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-done")
    await _live(repo, "s-ack")
    await _live(repo, "s-fail")
    await repo.mark_done("s-done")
    await repo.mark_failed("s-fail", "boom")
    await repo.mark_finished("s-ack", status=SessionStatus.INTERRUPTED.value)
    await repo.mark_ack("s-ack")

    rows = await repo.list_sessions(
        status=[SessionStatus.DONE.value, SessionStatus.ACK.value]
    )
    assert {r.session_id for r in rows} == {"s-done", "s-ack"}


async def test_type_and_yaml_text_are_kept(db) -> None:
    repo = StsSessionRepository(db)
    await repo.create_live(
        session_id="s-doc",
        created_by=1,
        strategy="tiny",
        type="node1::Tiny",
        yaml_text="sts: {}\n",
    )

    row = await repo.get_by_session_id("s-doc")
    assert row is not None
    assert row.type == "node1::Tiny"
    assert row.yaml_text == "sts: {}\n"


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
        venue="Paper", session_id="s-md", created_by=1
    )
    await repo.mark_done(venue="Paper", session_id="s-md")

    again = await repo.attach_live(
        venue="Paper", session_id="s-md", created_by=1
    )

    assert again.id == first.id
    assert again.status == SessionStatus.LIVE.value
    assert again.finished_at is None


async def test_rebuild_count_accumulates(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-count")

    assert await repo.bump_rebuild_count("s-count") == 1
    assert await repo.bump_rebuild_count("s-count") == 2

    db.expire_all()
    row = await repo.get_by_session_id("s-count")
    assert row is not None
    assert row.rebuild_count == 2
    # Deploys say whether they want to come back; the default is that they do.
    assert row.restart == "always"


async def test_rebuild_count_can_be_forgiven(db) -> None:
    """A rebuild that turned out to work has answered what the count asked.

    Leaving the total standing would retire a healthy session on some later
    restart it had nothing to do with.
    """
    repo = StsSessionRepository(db)
    await _live(repo, "s-forgive")
    await repo.bump_rebuild_count("s-forgive")
    await repo.bump_rebuild_count("s-forgive")

    row = await repo.reset_rebuild_count("s-forgive")
    assert row is not None
    assert row.rebuild_count == 0

    db.expire_all()
    again = await repo.get_by_session_id("s-forgive")
    assert again is not None
    assert again.rebuild_count == 0
    # Only the count is forgiven — the row is otherwise untouched.
    assert again.status == SessionStatus.LIVE.value


async def test_resetting_an_unknown_session_is_not_an_error(db) -> None:
    repo = StsSessionRepository(db)
    assert await repo.reset_rebuild_count("s-nobody") is None


async def test_mark_ack_keeps_the_reason_and_the_end(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-ack")
    failed = await repo.mark_failed("s-ack", "oco_insufficient_balance")
    assert failed is not None
    ended = failed.finished_at

    row = await repo.mark_ack("s-ack")
    assert row is not None
    assert row.status == SessionStatus.ACK.value
    assert row.reason == "oco_insufficient_balance"
    assert row.finished_at == ended


async def test_mark_ack_accepts_interrupted(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-int")
    await repo.mark_finished(
        "s-int",
        status=SessionStatus.INTERRUPTED.value,
        reason="STS shut down while this was running",
    )

    row = await repo.mark_ack("s-int")
    assert row is not None
    assert row.status == SessionStatus.ACK.value
    assert row.reason == "STS shut down while this was running"


async def _stamp(
    repo: StsSessionRepository, session_id: str, when: datetime
) -> None:
    row = await repo.get_by_session_id(session_id)
    assert row is not None
    row.created_at = when
    await repo.session.flush()


async def test_list_sessions_pages_on_a_session_cursor(db) -> None:
    """Newest first; the cursor of the last row is the rest, with no overlap."""
    repo = StsSessionRepository(db)
    origin = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    for offset, session_id in enumerate(("s-old", "s-mid", "s-new")):
        await _live(repo, session_id)
        await _stamp(repo, session_id, origin + timedelta(minutes=offset))

    first = await repo.list_sessions(status=None, limit=2)
    assert [r.session_id for r in first] == ["s-new", "s-mid"]

    rest = await repo.list_sessions(
        status=None, before_session="s-mid", limit=2
    )
    assert [r.session_id for r in rest] == ["s-old"]


async def test_list_sessions_breaks_a_tied_created_at_on_session_id(db) -> None:
    repo = StsSessionRepository(db)
    when = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    await _live(repo, "s-a")
    await _live(repo, "s-b")
    await _stamp(repo, "s-a", when)
    await _stamp(repo, "s-b", when)

    first = await repo.list_sessions(status=None, limit=1)
    assert [r.session_id for r in first] == ["s-b"]

    rest = await repo.list_sessions(
        status=None, before_session="s-b", limit=1
    )
    assert [r.session_id for r in rest] == ["s-a"]


async def test_an_empty_status_list_matches_nothing(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-live")

    assert await repo.list_sessions(status=[]) == []
    assert [r.session_id for r in await repo.list_sessions(status=None)] == [
        "s-live"
    ]


async def test_an_unknown_cursor_returns_nothing_not_the_first_page(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-live")

    rows = await repo.list_sessions(status=None, before_session="nope")
    assert rows == []
    still = await repo.list_sessions(status=None)
    assert [r.session_id for r in still] == ["s-live"]


async def test_mark_ack_refuses_live_and_done(db) -> None:
    repo = StsSessionRepository(db)
    await _live(repo, "s-live")
    await _live(repo, "s-done")
    await repo.mark_done("s-done")

    assert await repo.mark_ack("s-live") is None
    assert await repo.mark_ack("s-done") is None
    assert await repo.mark_ack("nope") is None
