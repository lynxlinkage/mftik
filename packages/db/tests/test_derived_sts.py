"""STS placement from a row's TD region — unique or refuse."""

from __future__ import annotations

import pytest
from db_harness import a_database, an_instance, an_owner
from mftik_db.models.api import Api, ApiType
from mftik_db.repositories import InstanceRepository


@pytest.fixture
async def db(database_url):
    async with a_database(database_url) as database, database.maker() as session:
        await an_owner(session)
        await session.commit()
        yield session


async def _api(session, instance_id: int, key: str = "k") -> Api:
    api = Api(
        owner_id=1,
        venue="Paper",
        api_key=key,
        api_secret="s",
        type=ApiType.HMAC.value,
        instance_id=instance_id,
    )
    session.add(api)
    await session.flush()
    return api


async def test_one_region_one_sts(db) -> None:
    td = await an_instance(db, "td-tw", "td", region="tw")
    await an_instance(db, "sts-tw", "sts", region="tw")
    api = await _api(db, td.id)

    assert await InstanceRepository(db).derived_sts([api.id]) == "sts-tw"


async def test_no_accounts_is_not_unique(db) -> None:
    await an_instance(db, "sts-tw", "sts", region="tw")
    assert await InstanceRepository(db).derived_sts([]) is None


async def test_mixed_regions_are_not_unique(db) -> None:
    tw = await an_instance(db, "td-tw", "td", region="tw")
    jp = await an_instance(db, "td-jp", "td", region="jp")
    await an_instance(db, "sts-tw", "sts", region="tw")
    a = await _api(db, tw.id, "a")
    b = await _api(db, jp.id, "b")

    assert await InstanceRepository(db).derived_sts([a.id, b.id]) is None


async def test_two_sts_in_one_region_are_not_unique(db) -> None:
    td = await an_instance(db, "td-tw", "td", region="tw")
    await an_instance(db, "sts-tw-1", "sts", region="tw")
    await an_instance(db, "sts-tw-2", "sts", region="tw")
    api = await _api(db, td.id)

    assert await InstanceRepository(db).derived_sts([api.id]) is None


async def test_a_disabled_sts_does_not_count(db) -> None:
    td = await an_instance(db, "td-tw", "td", region="tw")
    await an_instance(db, "sts-tw", "sts", region="tw", enabled=False)
    api = await _api(db, td.id)

    assert await InstanceRepository(db).derived_sts([api.id]) is None


async def test_a_missing_credential_is_not_unique(db) -> None:
    await an_instance(db, "sts-tw", "sts", region="tw")
    assert await InstanceRepository(db).derived_sts([99]) is None
