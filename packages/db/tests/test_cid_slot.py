"""Global cid_slot sequence — not a process-local counter."""

from __future__ import annotations

import pytest
from db_harness import a_database
from mftik_db.models.cid_slot import SLOT_SPACE
from mftik_db.repositories import StsSessionRepository


@pytest.fixture
async def db(database_url):
    async with a_database(database_url) as database, database.maker() as session:
        yield session


async def test_slots_increment_and_are_distinct(db) -> None:
    repo = StsSessionRepository(db)
    first = await repo.next_cid_slot()
    second = await repo.next_cid_slot()
    assert first != second
    assert 0 <= first < SLOT_SPACE
    assert 0 <= second < SLOT_SPACE


async def test_the_counter_survives_a_missing_seed_row(db) -> None:
    """create_all does not insert the sequence row; the first call does."""
    from mftik_db.models.cid_slot import CidSlotSeq
    from sqlalchemy import delete

    await db.execute(delete(CidSlotSeq))
    await db.flush()
    repo = StsSessionRepository(db)
    slot = await repo.next_cid_slot()
    assert slot == 1
