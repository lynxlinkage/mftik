"""Intent rows: wholesale put, release without delete (F38, IF-13)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from db_harness import a_database
from mftik.protocol import (
    IntentOwner,
    MdIntentDelete,
    MdIntentPatch,
    MdIntentPut,
    RollingFutureSelect,
    TdIntentDelete,
    TdIntentPut,
)
from mftik_db.models.intent import MdIntent, TdIntent
from mftik_db.repositories import IntentRepository

SID = "sess-1"
OTHER = "sess-2"
AT = datetime(2026, 10, 1, tzinfo=UTC)
AT_LATER = datetime(2026, 10, 2, tzinfo=UTC)
AT_LAST = datetime(2026, 10, 3, tzinfo=UTC)


def _at(value: datetime | None) -> datetime | None:
    """The moment as UTC.

    sqlite hands a timezone-aware write back without ``tzinfo``. The
    instant is the same one.
    """
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


def _owner(session_id: str = SID, sts: str = "sts-jp") -> IntentOwner:
    return IntentOwner(sts_instance=sts, session_id=session_id)


def _td(api_ids: list[int], session_id: str = SID) -> TdIntentPut:
    return TdIntentPut(
        session_id=session_id, owner=_owner(session_id), api_ids=api_ids
    )


def _md(
    feeds: dict[str, list[str]],
    *,
    session_id: str = SID,
    selects: list[RollingFutureSelect] | None = None,
) -> MdIntentPut:
    return MdIntentPut(
        session_id=session_id,
        owner=_owner(session_id),
        feeds=feeds,
        selects=list(selects or []),
    )


@pytest.fixture
async def db(database_url):
    async with a_database(database_url) as database, database.maker() as session:
        yield session


async def test_unreleased_td_is_the_live_rows_for_those_accounts(db) -> None:
    """Restart seed reads this. Released rows and other accounts stay out."""
    repo = IntentRepository(db)
    await repo.put(_td([1, 2]), at=AT)
    await repo.put(_td([1], session_id=OTHER), at=AT)
    await repo.put(_td([3], session_id="sess-3"), at=AT)
    await repo.delete(
        TdIntentDelete(session_id=SID, owner=_owner(), api_ids=[1]),
        at=AT_LATER,
    )

    rows = await repo.unreleased_td([2, 1, 1])
    assert [(row.session_id, row.api_id) for row in rows] == [
        (SID, 2),
        (OTHER, 1),
    ]
    assert all(row.released_at is None for row in rows)
    assert await repo.unreleased_td(()) == ()


async def test_unreleased_td_rejects_an_api_id_that_is_not_a_positive_int(
    db,
) -> None:
    with pytest.raises(ValueError):
        await IntentRepository(db).unreleased_td([0])


async def test_td_put_inserts_one_row_per_api_id(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_td([1, 2]), at=AT)

    rows = await repo.td_for(SID)
    assert [row.api_id for row in rows] == [1, 2]
    assert all(row.released_at is None for row in rows)


async def test_td_put_replaces_the_set_and_does_not_insert_a_second_row(
    db,
) -> None:
    repo = IntentRepository(db)
    await repo.put(_td([1, 2]), at=AT)
    await repo.put(_td([2, 3]), at=AT_LATER)

    rows = {row.api_id: row for row in await repo.td_for(SID)}
    assert set(rows) == {1, 2, 3}
    assert _at(rows[1].released_at) == AT_LATER
    assert rows[2].released_at is None
    assert rows[3].released_at is None
    assert await db.get(TdIntent, (SID, 2)) is rows[2]


async def test_td_put_of_a_released_id_clears_released_at_on_the_same_row(
    db,
) -> None:
    repo = IntentRepository(db)
    await repo.put(_td([1, 2]), at=AT)
    await repo.put(_td([2]), at=AT_LATER)
    await repo.put(_td([1]), at=AT)

    rows = {row.api_id: row for row in await repo.td_for(SID)}
    assert rows[1].released_at is None
    # The third put is what drops 2, so the timestamp is that put's.
    assert _at(rows[2].released_at) == AT
    assert len(rows) == 2


async def test_an_already_released_td_row_keeps_its_first_timestamp(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_td([1]), at=AT)
    await repo.put(_td([]), at=AT_LATER)
    await repo.put(_td([]), at=AT_LAST)

    row = await db.get(TdIntent, (SID, 1))
    assert row is not None
    assert _at(row.released_at) == AT_LATER


async def test_an_empty_td_put_releases_every_account(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_td([1, 2]), at=AT)
    await repo.put(_td([]), at=AT_LATER)

    rows = await repo.td_for(SID)
    assert len(rows) == 2
    assert all(_at(row.released_at) == AT_LATER for row in rows)


async def test_duplicate_api_ids_are_membership_not_a_refcount(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_td([1, 1, 1]), at=AT)

    rows = await repo.td_for(SID)
    assert [row.api_id for row in rows] == [1]
    assert rows[0].released_at is None


async def test_td_delete_releases_the_named_accounts_and_leaves_the_rest(
    db,
) -> None:
    repo = IntentRepository(db)
    await repo.put(_td([1, 2]), at=AT)
    await repo.delete(
        TdIntentDelete(session_id=SID, owner=_owner(), api_ids=[1]),
        at=AT_LATER,
    )

    rows = {row.api_id: row for row in await repo.td_for(SID)}
    assert _at(rows[1].released_at) == AT_LATER
    assert rows[2].released_at is None


async def test_an_empty_td_delete_releases_every_account(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_td([1, 2]), at=AT)
    await repo.delete(
        TdIntentDelete(session_id=SID, owner=_owner(), api_ids=[]),
        at=AT_LATER,
    )

    rows = await repo.td_for(SID)
    assert len(rows) == 2
    assert all(_at(row.released_at) == AT_LATER for row in rows)


async def test_deleting_an_account_the_session_does_not_hold_is_a_no_op(
    db,
) -> None:
    repo = IntentRepository(db)
    await repo.put(_td([1]), at=AT)
    await repo.delete(
        TdIntentDelete(session_id=SID, owner=_owner(), api_ids=[9]),
        at=AT_LATER,
    )

    row = await db.get(TdIntent, (SID, 1))
    assert row is not None
    assert row.released_at is None
    assert await db.get(TdIntent, (SID, 9)) is None


async def test_a_td_delete_does_not_take_an_instance(db) -> None:
    repo = IntentRepository(db)
    with pytest.raises(TypeError):
        await repo.delete(
            TdIntentDelete(session_id=SID, owner=_owner(), api_ids=[1]),
            instance="td-jp",
        )


async def test_release_keeps_the_row_and_a_second_release_keeps_the_timestamp(
    db,
) -> None:
    repo = IntentRepository(db)
    await repo.put(_td([1]), at=AT)
    await repo.put(_md({"md-a": ["book"]}))
    await repo.release(SID, at=AT)
    await repo.release(SID, at=AT_LATER)

    td = await db.get(TdIntent, (SID, 1))
    md = await db.get(MdIntent, (SID, "md-a"))
    assert td is not None and md is not None
    assert _at(td.released_at) == AT
    assert _at(md.released_at) == AT


async def test_two_sessions_do_not_share_an_intent_row(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_td([1], SID), at=AT)
    await repo.put(_td([1], OTHER), at=AT)
    await repo.release(SID, at=AT_LATER)

    kept = await db.get(TdIntent, (OTHER, 1))
    released = await db.get(TdIntent, (SID, 1))
    assert kept is not None and kept.released_at is None
    assert released is not None and _at(released.released_at) == AT_LATER


async def test_an_api_id_must_be_a_positive_int(db) -> None:
    repo = IntentRepository(db)
    # The wire model accepts ``True`` as ``1``. The repository does not:
    # membership is an account id, and a bool is not one.
    coerced = TdIntentPut.model_construct(
        session_id=SID, owner=_owner(), api_ids=[True]
    )
    with pytest.raises(ValueError):
        await repo.put(coerced)
    with pytest.raises(ValueError):
        await repo.put(_td([0]))


async def test_the_owner_session_must_match_the_intent(db) -> None:
    repo = IntentRepository(db)
    with pytest.raises(ValueError):
        await repo.put(
            TdIntentPut(
                session_id=SID,
                owner=_owner(OTHER),
                api_ids=[1],
            )
        )


async def test_a_naive_timestamp_is_refused(db) -> None:
    repo = IntentRepository(db)
    with pytest.raises(ValueError):
        await repo.put(_td([1]), at=datetime(2026, 10, 1))


async def test_an_md_put_for_one_instance_does_not_release_another(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_md({"md-a": ["book"], "md-b": ["tape"]}))
    await repo.put(_md({"md-a": ["book-2"]}))

    rows = {row.instance: row for row in await repo.md_for(SID)}
    assert rows["md-a"].feeds == ["book-2"]
    assert rows["md-a"].generation == 2
    assert rows["md-a"].released_at is None
    assert rows["md-b"].feeds == ["tape"]
    assert rows["md-b"].generation == 1
    assert rows["md-b"].released_at is None


async def test_an_unchanged_md_declaration_keeps_atoms_and_generation(
    db,
) -> None:
    repo = IntentRepository(db)
    await repo.put(_md({"md-a": ["book"]}))
    row = await db.get(MdIntent, (SID, "md-a"))
    assert row is not None
    row.atoms = {"book": ["atom-1"]}
    await db.flush()

    await repo.put(_md({"md-a": ["book"]}))
    assert row.atoms == {"book": ["atom-1"]}
    assert row.generation == 1
    assert row.released_at is None


async def test_a_changed_md_declaration_clears_atoms_and_bumps_generation(
    db,
) -> None:
    repo = IntentRepository(db)
    await repo.put(_md({"md-a": ["book"]}))
    row = await db.get(MdIntent, (SID, "md-a"))
    assert row is not None
    row.atoms = {"book": ["atom-1"]}
    await db.flush()

    await repo.put(_md({"md-a": ["quote"]}))
    assert row.feeds == ["quote"]
    assert row.atoms == {}
    assert row.generation == 2


async def test_putting_a_released_instance_back_clears_released_at(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_md({"md-a": ["book"]}))
    await repo.release(SID, at=AT)
    await repo.put(_md({"md-a": ["book"]}))

    row = await db.get(MdIntent, (SID, "md-a"))
    assert row is not None
    assert row.released_at is None
    assert row.generation == 1
    assert await db.get(MdIntent, (SID, "md-a")) is row


async def test_a_select_block_is_stored_on_the_instance_the_put_names(
    db,
) -> None:
    select = RollingFutureSelect(
        name="near",
        venue="Paper",
        underlying="BTC",
        tenor="quarterly",
        topics=("orderbook",),
    )
    repo = IntentRepository(db)
    await repo.put(_md({"md-a": ["book"]}, selects=[select]))

    row = await db.get(MdIntent, (SID, "md-a"))
    assert row is not None
    assert row.feeds == ["book", select.model_dump(mode="json")]
    assert row.atoms == {}
    assert row.generation == 1


async def test_md_delete_releases_one_instance_and_leaves_the_other(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_md({"md-a": ["book"], "md-b": ["tape"]}))
    await repo.delete(
        MdIntentDelete(session_id=SID, owner=_owner()),
        instance="md-a",
        at=AT,
    )

    rows = {row.instance: row for row in await repo.md_for(SID)}
    assert _at(rows["md-a"].released_at) == AT
    assert rows["md-b"].released_at is None


async def test_md_delete_without_an_instance_releases_every_row(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_md({"md-a": ["book"], "md-b": ["tape"]}))
    await repo.delete(
        MdIntentDelete(session_id=SID, owner=_owner()),
        at=AT,
    )

    rows = await repo.md_for(SID)
    assert len(rows) == 2
    assert all(_at(row.released_at) == AT for row in rows)


async def test_patch_removes_then_adds_and_keeps_select_blocks(db) -> None:
    select = RollingFutureSelect(
        name="near",
        venue="Paper",
        underlying="BTC",
        tenor="quarterly",
        topics=("orderbook",),
    )
    repo = IntentRepository(db)
    await repo.put(_md({"md-a": ["a", "b"]}, selects=[select]))
    row = await db.get(MdIntent, (SID, "md-a"))
    assert row is not None
    row.atoms = {"a": ["atom-1"]}
    await db.flush()

    await repo.patch(
        MdIntentPatch(
            session_id=SID,
            owner=_owner(),
            remove=["a"],
            add=["a", "c"],
        ),
        instance="md-a",
    )

    assert row.feeds == ["b", "a", "c", select.model_dump(mode="json")]
    assert row.generation == 2
    assert row.atoms == {}
    assert row.released_at is None


async def test_a_no_op_patch_does_not_resurrect_a_released_row(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_md({"md-a": ["book"]}))
    await repo.release(SID, at=AT)
    await repo.patch(
        MdIntentPatch(session_id=SID, owner=_owner()),
        instance="md-a",
    )

    row = await db.get(MdIntent, (SID, "md-a"))
    assert row is not None
    assert _at(row.released_at) == AT
    assert row.generation == 1


async def test_a_patch_that_changes_feeds_unmarks_a_released_row(db) -> None:
    repo = IntentRepository(db)
    await repo.put(_md({"md-a": ["book"]}))
    row = await db.get(MdIntent, (SID, "md-a"))
    assert row is not None
    row.atoms = {"book": ["atom-1"]}
    await db.flush()
    await repo.release(SID, at=AT)
    await repo.patch(
        MdIntentPatch(session_id=SID, owner=_owner(), add=["quote"]),
        instance="md-a",
    )

    assert row.released_at is None
    assert row.feeds == ["book", "quote"]
    assert row.generation == 2
    assert row.atoms == {}


async def test_a_patch_of_a_missing_instance_inserts_when_add_names_a_feed(
    db,
) -> None:
    repo = IntentRepository(db)
    await repo.patch(
        MdIntentPatch(session_id=SID, owner=_owner(), add=["book"]),
        instance="md-a",
    )

    row = await db.get(MdIntent, (SID, "md-a"))
    assert row is not None
    assert row.feeds == ["book"]
    assert row.atoms == {}
    assert row.generation == 1
    assert row.released_at is None


async def test_an_empty_patch_of_a_missing_instance_inserts_nothing(db) -> None:
    repo = IntentRepository(db)
    await repo.patch(
        MdIntentPatch(session_id=SID, owner=_owner()),
        instance="md-a",
    )

    assert await db.get(MdIntent, (SID, "md-a")) is None
