"""Alert graph: six tables, two join FKs, selector grammar, SQLite cascade."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from db_harness import a_database, an_owner
from mftik_db.models import (
    Alert,
    AlertDelivery,
    AlertMatcher,
    AlertMatcherAlert,
    AlertSource,
    AlertSourceMatcher,
    Base,
)
from mftik_db.repositories import (
    AlertMatcherRepository,
    AlertRepository,
    AlertSourceRepository,
    InvalidAlertSelector,
    validate_source_selector,
)
from mftik_db.session import build_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker


@pytest.fixture
async def db(database_url):
    async with a_database(database_url) as database, database.maker() as session:
        await an_owner(session)
        await session.commit()
        yield session


def test_selector_grammar() -> None:
    validate_source_selector("sts", "private::Tiny")
    validate_source_selector("sts", "*")
    validate_source_selector("sts", "CrossArb")
    validate_source_selector("sts", "a" * 32)  # hex session id: legal, never matches
    validate_source_selector("td", "12")
    validate_source_selector("md", "Gate")
    validate_source_selector("md", "*")

    with pytest.raises(InvalidAlertSelector):
        validate_source_selector("foo", "*")
    with pytest.raises(InvalidAlertSelector):
        validate_source_selector("sts", "")
    with pytest.raises(InvalidAlertSelector):
        validate_source_selector("sts", "private::Tiny::extra")
    with pytest.raises(InvalidAlertSelector):
        validate_source_selector("sts", "::Tiny")
    with pytest.raises(InvalidAlertSelector):
        validate_source_selector("sts", "private::")


def test_join_tables_cannot_express_matcher_to_matcher() -> None:
    source_matcher = set(AlertSourceMatcher.__table__.c.keys())
    matcher_alert = set(AlertMatcherAlert.__table__.c.keys())
    assert source_matcher == {"source_id", "matcher_id"}
    assert matcher_alert == {"matcher_id", "alert_id"}
    assert "alert_edges" not in Base.metadata.tables


def test_alerts_created_by_is_a_required_user_fk() -> None:
    col = Alert.__table__.c.created_by
    assert not col.nullable
    assert "users.id" in {str(fk.column) for fk in col.foreign_keys}


async def _graph(db):
    sources = AlertSourceRepository(db)
    matchers = AlertMatcherRepository(db)
    alerts = AlertRepository(db)
    source = await sources.create(
        created_by=1, domain="sts", selector="private::Tiny"
    )
    matcher = await matchers.create(
        created_by=1,
        name="warn",
        kind="level",
        spec={"levels": ["warn", "error"]},
    )
    alert = await alerts.create(
        created_by=1,
        name="ops",
        webhook_url="https://discord.com/api/webhooks/1/secret",
    )
    await alerts.wire_source_matcher(source.id, matcher.id)
    await alerts.wire_matcher_alert(matcher.id, alert.id)
    return source, matcher, alert, sources, matchers, alerts


async def test_duplicate_source_violates_unique(db) -> None:
    sources = AlertSourceRepository(db)
    await sources.create(created_by=1, domain="sts", selector="private::Tiny")
    with pytest.raises(IntegrityError):
        await sources.create(
            created_by=1, domain="sts", selector="private::Tiny"
        )


async def test_delete_matcher_cascades_joins_and_keeps_the_alert(db) -> None:
    source, matcher, alert, _sources, matchers, _alerts = await _graph(db)
    await matchers.delete(matcher.id)
    await db.flush()

    assert await db.get(AlertSourceMatcher, (source.id, matcher.id)) is None
    assert await db.get(AlertMatcherAlert, (matcher.id, alert.id)) is None
    assert await db.get(Alert, alert.id) is not None
    assert await db.get(AlertSource, source.id) is not None


async def test_delete_alert_cascades_deliveries_and_wires(db) -> None:
    source, matcher, alert, _sources, _matchers, alerts = await _graph(db)
    await alerts.record_delivery(
        alert_id=alert.id,
        window_start=datetime(2026, 8, 21, tzinfo=UTC),
        event_count=2,
        dropped_count=0,
        http_status=204,
    )
    await alerts.delete(alert.id)
    await db.flush()

    assert await db.get(Alert, alert.id) is None
    leftover = (
        await db.execute(
            AlertDelivery.__table__.select().where(
                AlertDelivery.__table__.c.alert_id == alert.id
            )
        )
    ).first()
    assert leftover is None
    assert await db.get(AlertMatcherAlert, (matcher.id, alert.id)) is None
    assert await db.get(AlertMatcher, matcher.id) is not None
    assert await db.get(AlertSourceMatcher, (source.id, matcher.id)) is not None


async def test_spec_is_json(db) -> None:
    matchers = AlertMatcherRepository(db)
    row = await matchers.create(
        created_by=1,
        name="extract-risk",
        kind="extract",
        spec={"pattern": r"(\d+)", "group": 1, "as": "float", "op": ">", "value": 0.99},
    )
    loaded = await matchers.get(row.id)
    assert loaded is not None
    assert loaded.spec["value"] == 0.99
    assert loaded.spec["as"] == "float"


async def test_hex_session_id_is_a_legal_sts_selector(db) -> None:
    sources = AlertSourceRepository(db)
    row = await sources.create(
        created_by=1, domain="sts", selector="a" * 32
    )
    assert row.selector == "a" * 32


async def test_runtime_sqlite_engine_cascades_matcher_delete(
    tmp_path,
) -> None:
    """The listener on ``session.build_engine``, not the harness pragma."""
    url = f"sqlite+aiosqlite:///{tmp_path / 'alerts.db'}"
    engine = build_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)

    async with maker() as session:
        await an_owner(session)
        sources = AlertSourceRepository(session)
        matchers = AlertMatcherRepository(session)
        alerts = AlertRepository(session)
        source = await sources.create(
            created_by=1, domain="sts", selector="*"
        )
        matcher = await matchers.create(
            created_by=1, name="any", kind="level", spec={"levels": ["error"]}
        )
        alert = await alerts.create(
            created_by=1,
            name="ops",
            webhook_url="https://example.invalid/hook",
        )
        await alerts.wire_source_matcher(source.id, matcher.id)
        await alerts.wire_matcher_alert(matcher.id, alert.id)
        source_id, matcher_id, alert_id = source.id, matcher.id, alert.id
        await session.commit()

    async with maker() as session:
        matchers = AlertMatcherRepository(session)
        await matchers.delete(matcher_id)
        await session.commit()

    async with maker() as session:
        assert (
            await session.get(AlertSourceMatcher, (source_id, matcher_id))
        ) is None
        assert (
            await session.get(AlertMatcherAlert, (matcher_id, alert_id))
        ) is None
        assert await session.get(Alert, alert_id) is not None
        assert await session.get(AlertSource, source_id) is not None

    await engine.dispose()
