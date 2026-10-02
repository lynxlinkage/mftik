"""The boot-time read that refuses a schema older than the build.

A process pointed at a database below ``MIN_STS_REVISION`` does not fail on
connect. It fails on the first row that needs a column the migration has
not added yet, which is why this is a question asked once at boot.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa
from mftik_db.schema import (
    MIN_STS_REVISION,
    SchemaState,
    SchemaTooOld,
    describe_too_old,
    read_schema_state,
    require_sts_schema,
)
from mftik_db.session import build_engine
from sqlalchemy.ext.asyncio import AsyncEngine

_AFTER = frozenset({"session_id", "status", "type"})
_BEFORE = _AFTER | {"strategy"}


def test_the_dropped_column_is_what_says_the_migration_ran() -> None:
    assert describe_too_old(SchemaState(MIN_STS_REVISION, _AFTER)) is None
    too_old = describe_too_old(SchemaState("0033_option_strike", _BEFORE))
    assert too_old is not None
    assert "0033_option_strike" in too_old
    assert "0034_strategy_type_key" in too_old
    assert MIN_STS_REVISION in too_old


def test_0034_dropped_strategy_and_is_still_behind_spec_status() -> None:
    """0034 is no longer enough: this build selects the pins from 0036."""
    behind = describe_too_old(SchemaState("0034_strategy_type_key", _AFTER))
    assert behind is not None
    assert "below" in behind
    assert MIN_STS_REVISION in behind


def test_a_schema_built_from_the_models_serves() -> None:
    """No ``alembic_version`` at all. That is the test suite, not a node."""
    assert describe_too_old(SchemaState(None, _AFTER)) is None


def test_a_revision_behind_the_floor_is_refused_on_its_number() -> None:
    behind = describe_too_old(SchemaState("0031_instances", _AFTER))
    assert behind is not None and "below" in behind


def test_a_later_revision_serves() -> None:
    assert describe_too_old(SchemaState("0041_something", _AFTER)) is None


def test_a_database_with_no_sts_sessions_table_is_not_servable() -> None:
    """Nothing has migrated it yet, and this read cannot tell why.

    A cold start passes through it on the way to a migrated schema, so the
    caller waits — but it is not a state to serve, and this read does not
    pretend it is one.
    """
    for state in (
        SchemaState("0033_option_strike", frozenset()),
        SchemaState(None, frozenset()),
    ):
        problem = describe_too_old(state)
        assert problem is not None and "no sts_sessions table" in problem


async def _engine(tmp_path: Path, columns: str, revision: str | None) -> AsyncEngine:
    engine = build_engine(f"sqlite+aiosqlite:///{tmp_path / 'node.db'}")
    async with engine.begin() as conn:
        await conn.execute(sa.text(f"CREATE TABLE sts_sessions ({columns})"))
        if revision is not None:
            await conn.execute(
                sa.text("CREATE TABLE alembic_version (version_num VARCHAR(32))")
            )
            await conn.execute(
                sa.text("INSERT INTO alembic_version VALUES (:rev)"),
                {"rev": revision},
            )
    return engine


# sqlite schema read; over the 50 ms unit call cap
@pytest.mark.component
async def test_a_pre_0034_database_refuses_to_serve_sts(tmp_path: Path) -> None:
    engine = await _engine(
        tmp_path,
        "session_id VARCHAR(64) PRIMARY KEY, strategy VARCHAR(128), "
        "type VARCHAR(128)",
        "0033_option_strike",
    )
    try:
        state = await read_schema_state(engine)
        assert state.revision == "0033_option_strike"
        assert "strategy" in state.sts_columns
        with pytest.raises(SchemaTooOld, match=MIN_STS_REVISION):
            await require_sts_schema(engine)
    finally:
        await engine.dispose()


# sqlite schema read; over the 50 ms unit call cap
@pytest.mark.component
async def test_a_migrated_database_serves(tmp_path: Path) -> None:
    engine = await _engine(
        tmp_path,
        "session_id VARCHAR(64) PRIMARY KEY, type VARCHAR(128)",
        MIN_STS_REVISION,
    )
    try:
        await require_sts_schema(engine)
    finally:
        await engine.dispose()
