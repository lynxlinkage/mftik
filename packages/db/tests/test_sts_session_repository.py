"""STS session terminal statuses — done vs failed, and the failure reason."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from db_harness import a_database, an_instance, an_owner
from mftik_db.models.api import Api, ApiType
from mftik_db.models.session import MdSessionRow, SessionStatus, TdSessionRow
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
        session_id=session_id, created_by=1, type="NoopStrategy"
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
        type="node1::Tiny",
        yaml_text="sts: {}\n",
    )

    row = await repo.get_by_session_id("s-doc")
    assert row is not None
    assert row.type == "node1::Tiny"
    assert row.yaml_text == "sts: {}\n"


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


async def test_list_sessions_pages_on_offset(db) -> None:
    repo = StsSessionRepository(db)
    origin = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    for minutes, session_id in enumerate(("s-old", "s-mid", "s-new")):
        await _live(repo, session_id)
        await _stamp(repo, session_id, origin + timedelta(minutes=minutes))

    first = await repo.list_sessions(status=None, limit=2)
    assert [r.session_id for r in first] == ["s-new", "s-mid"]
    assert await repo.count_sessions(status=None) == 3

    rest = await repo.list_sessions(status=None, offset=2, limit=2)
    assert [r.session_id for r in rest] == ["s-old"]


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


async def test_sts_count_by_instance_omits_unpinned_and_splits(db) -> None:
    repo = StsSessionRepository(db)
    await repo.create_live(
        session_id="s-fail-tw", created_by=1, instance="sts-tw"
    )
    await repo.mark_failed("s-fail-tw", "boom")
    await repo.create_live(
        session_id="s-live-jp", created_by=1, instance="sts-jp"
    )
    await repo.create_live(session_id="s-unpinned", created_by=1, instance=None)

    assert await repo.count_by_instance() == {
        "sts-tw": {SessionStatus.FAILED.value: 1},
        "sts-jp": {SessionStatus.LIVE.value: 1},
    }


async def test_md_count_by_instance_splits(db) -> None:
    db.add(
        MdSessionRow(
            instance="md-jp-1",
            venue="Bybit",
            session_id="s1",
            created_by=1,
            status=SessionStatus.DONE.value,
            finished_at=datetime.now(UTC),
        )
    )
    db.add(
        MdSessionRow(
            instance="md-jp-2",
            venue="Bybit",
            session_id="s2",
            created_by=1,
            status=SessionStatus.LIVE.value,
        )
    )
    await db.flush()

    repo = MdSessionRepository(db)
    assert await repo.count_by_instance() == {
        "md-jp-1": {SessionStatus.DONE.value: 1},
        "md-jp-2": {SessionStatus.LIVE.value: 1},
    }


async def test_td_count_by_instance_follows_the_credential(db) -> None:
    tw = await an_instance(db, "td-tw", "td")
    jp = await an_instance(db, "td-jp", "td")
    api_tw = Api(
        owner_id=1,
        venue="Paper",
        api_key="tw",
        api_secret="s",
        type=ApiType.HMAC.value,
        instance_id=tw.id,
    )
    api_jp = Api(
        owner_id=1,
        venue="Deribit",
        api_key="jp",
        api_secret="s",
        type=ApiType.HMAC.value,
        instance_id=jp.id,
    )
    db.add(api_tw)
    db.add(api_jp)
    await db.flush()

    db.add(
        TdSessionRow(
            session_id="s-tw",
            created_by=1,
            api_id=api_tw.id,
            status=SessionStatus.LIVE.value,
        )
    )
    db.add(
        TdSessionRow(
            session_id="s-jp",
            created_by=1,
            api_id=api_jp.id,
            status=SessionStatus.DONE.value,
            finished_at=datetime.now(UTC),
        )
    )
    await db.flush()

    repo = TdSessionRepository(db)
    assert await repo.count_by_instance() == {
        "td-tw": {SessionStatus.LIVE.value: 1},
        "td-jp": {SessionStatus.DONE.value: 1},
    }
