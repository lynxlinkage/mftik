"""Venue API credentials — uniqueness is (venue, api_key)."""

from __future__ import annotations

import pytest
from db_harness import a_database, an_owner
from mftik_db.models.api import Api, ApiType
from mftik_db.repositories import ApiRepository
from sqlalchemy.exc import IntegrityError


@pytest.fixture
async def db(database_url):
    async with a_database(database_url) as database, database.maker() as session:
        await an_owner(session)
        await session.commit()
        yield session


def _row(*, venue: str, api_key: str = "shared-key") -> Api:
    return Api(
        owner_id=1,
        venue=venue,
        api_key=api_key,
        api_secret="secret",
        type=ApiType.ED25519.value,
    )


async def test_same_key_on_two_venues_is_allowed(db) -> None:
    """Binance issues one key for USD-M and COIN-M; both rows must store."""
    apis = ApiRepository(db)
    um = await apis.add(_row(venue="BinanceUM"))
    cm = await apis.add(_row(venue="BinanceCM"))
    await db.flush()

    assert um.id != cm.id
    assert await apis.get_by_venue_and_api_key(
        "BinanceUM", "shared-key"
    ) is um
    assert await apis.get_by_venue_and_api_key(
        "BinanceCM", "shared-key"
    ) is cm


async def test_same_key_on_the_same_venue_is_refused(db) -> None:
    apis = ApiRepository(db)
    await apis.add(_row(venue="BinanceUM"))
    with pytest.raises(IntegrityError):
        await apis.add(_row(venue="BinanceUM"))


async def test_lookup_finds_a_non_canonical_venue_spelling(db) -> None:
    """Rows written by older code may hold e.g. ``binanceum``.

    Venue identity is case-insensitive everywhere else, so the pre-check
    has to see such a row — the exact-match constraint will not.
    """
    apis = ApiRepository(db)
    legacy = await apis.add(_row(venue="binanceum"))
    await db.flush()

    assert await apis.get_by_venue_and_api_key(
        "BinanceUM", "shared-key"
    ) is legacy
    assert await apis.get_by_venue_and_api_key(
        " BinanceUM ", "shared-key"
    ) is legacy
    assert (
        await apis.get_by_venue_and_api_key("BinanceCM", "shared-key")
        is None
    )
