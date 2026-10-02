"""0035 is additive, on sqlite and on Postgres.

The deploy path is ``alembic upgrade``. These tests run that revision's
``upgrade`` and ``downgrade`` against a schema that still looks like 0034,
then check the rows it had to backfill. The model tests next to them are
the shape ``create_all`` builds, which is what the suite itself uses; CI's
*Migrations match the models* step is what proves the two stayed the same.
"""

from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from db_harness import a_database, an_instance, an_owner
from mftik_db.models import (
    Api,
    ApiType,
    Base,
    MdIntent,
    MdSelectorState,
    MdStandingSubscription,
    StsSessionRow,
    TdIntent,
)
from mftik_db.repositories import StsSessionRepository
from sqlalchemy.exc import IntegrityError

_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "mftik_db"
    / "migrations"
    / "versions"
    / "0035_plane_schema.py"
)


def _migration():
    spec = importlib.util.spec_from_file_location("m0035_up", _PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _apply(conn: sa.Connection, direction: str) -> None:
    ops = Operations(MigrationContext.configure(conn))
    with Operations.context(ops.migration_context):
        getattr(_migration(), direction)()


def _sync_url(url: str, path: Path) -> str:
    if url.startswith("sqlite"):
        return f"sqlite:///{path / 'pre.db'}"
    return url.replace("postgresql+asyncpg", "postgresql+psycopg", 1)


def _json(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _prepare(conn: sa.Connection) -> None:
    meta = sa.MetaData()
    sa.Table(
        "sts_sessions",
        meta,
        sa.Column("session_id", sa.String(64), primary_key=True),
        sa.Column(
            "restart",
            sa.String(8),
            nullable=False,
            server_default="always",
        ),
        sa.Column(
            "rebuild_count", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("st_facts", sa.JSON(), nullable=False, server_default="{}"),
    )
    sa.Table(
        "apis",
        meta,
        sa.Column("id", sa.Integer(), primary_key=True),
    )
    meta.create_all(conn)
    conn.execute(
        sa.text(
            "INSERT INTO sts_sessions (session_id, restart) "
            "VALUES ('s-old', 'always')"
        )
    )
    conn.execute(sa.text("INSERT INTO apis (id) VALUES (1)"))


def _columns(conn: sa.Connection, table: str, schema: str | None) -> set[str]:
    return {
        col["name"]
        for col in sa.inspect(conn).get_columns(table, schema=schema)
    }


def _restart_length(conn: sa.Connection, schema: str | None) -> int | None:
    for col in sa.inspect(conn).get_columns("sts_sessions", schema=schema):
        if col["name"] == "restart":
            return getattr(col["type"], "length", None)
    raise AssertionError("restart column missing")


def test_0035_upgrades_and_downgrades(database_url: str, tmp_path: Path) -> None:
    """A 0034-shaped database gains the new columns and can give them back.

    Existing session and account rows are rewritten only where a default
    has to fill a ``NOT NULL`` column. ``restart`` stays ``always`` on the
    row that already said so. ``rebuild_count`` and ``st_facts`` stay.
    ``strategy_digest`` does not appear.
    """
    postgres = database_url.startswith("postgresql")
    engine = sa.create_engine(_sync_url(database_url, tmp_path))
    try:
        with engine.begin() as conn:
            schema = "mig0035" if postgres else None
            if postgres:
                conn.execute(sa.text("CREATE SCHEMA mig0035"))
                conn.execute(sa.text("SET LOCAL search_path TO mig0035"))
                path = conn.execute(sa.text("SHOW search_path")).scalar_one()
                assert "public" not in path
            _prepare(conn)
            _apply(conn, "upgrade")

            cols = _columns(conn, "sts_sessions", schema)
            assert {
                "generation",
                "observed_generation",
                "worker_incarnation",
                "conditions",
                "restart_count",
                "rebuild_count",
                "st_facts",
            } <= cols
            assert "strategy_digest" not in cols
            assert "env_generation" not in cols
            assert _columns(conn, "md_intents", schema) == {
                "session_id",
                "instance",
                "feeds",
                "atoms",
                "generation",
                "created_at",
                "released_at",
            }
            assert _columns(conn, "td_intents", schema) == {
                "session_id",
                "api_id",
                "created_at",
                "released_at",
            }
            assert _columns(conn, "md_selector_state", schema) == {
                "spec_hash",
                "universe",
                "epoch",
                "center",
                "updated_at",
            }
            standing = _columns(conn, "md_standing_subscriptions", schema)
            assert "session_id" not in standing
            assert "released_at" not in standing
            if postgres:
                assert _restart_length(conn, schema) == 16

            old = conn.execute(
                sa.text(
                    "SELECT restart, generation, observed_generation, "
                    "worker_incarnation, conditions, restart_count, "
                    "rebuild_count FROM sts_sessions WHERE session_id = 's-old'"
                )
            ).one()
            assert old.restart == "always"
            assert old.generation == 1
            assert old.observed_generation is None
            assert old.worker_incarnation is None
            assert _json(old.conditions) == {}
            assert old.restart_count == 0
            assert old.rebuild_count == 0

            cod = conn.execute(
                sa.text("SELECT cancel_on_disconnect FROM apis WHERE id = 1")
            ).scalar_one()
            assert cod in (False, 0)

            conn.execute(
                sa.text(
                    "INSERT INTO sts_sessions (session_id) VALUES ('s-new')"
                )
            )
            conn.execute(
                sa.text(
                    "UPDATE sts_sessions SET restart = 'on_failure' "
                    "WHERE session_id = 's-new'"
                )
            )
            fresh = conn.execute(
                sa.text(
                    "SELECT restart, generation FROM sts_sessions "
                    "WHERE session_id = 's-new'"
                )
            ).one()
            assert fresh.restart == "on_failure"
            assert fresh.generation == 1

            # ``on_failure`` does not fit in the varchar(8) downgrade
            # restores. The value is a new-schema value; put the row back
            # before reversing the widen.
            conn.execute(
                sa.text(
                    "UPDATE sts_sessions SET restart = 'never' "
                    "WHERE session_id = 's-new'"
                )
            )
            _apply(conn, "downgrade")

            after = _columns(conn, "sts_sessions", schema)
            assert "generation" not in after
            assert "restart_count" not in after
            assert "conditions" not in after
            assert "rebuild_count" in after
            assert "st_facts" in after
            assert "cancel_on_disconnect" not in _columns(conn, "apis", schema)
            insp = sa.inspect(conn)
            for table in (
                "md_intents",
                "td_intents",
                "md_standing_subscriptions",
                "md_selector_state",
            ):
                assert not insp.has_table(table, schema=schema)
            if postgres:
                assert _restart_length(conn, schema) == 8
    finally:
        if postgres:
            with engine.begin() as conn:
                conn.execute(sa.text("DROP SCHEMA IF EXISTS mig0035 CASCADE"))
        engine.dispose()


def test_planned_columns_and_nothing_on_hold() -> None:
    """§8.4's tuples, and none of the on-hold tables.

    Selector state is exactly ``(spec_hash, universe, epoch, center,
    updated_at)``. Registry and artifact tables are not in the schema
    (F40). ``strategy_digest`` and ``env_generation`` are nullable Spec
    columns added by 0036 (F39, IF-16), not by this revision.
    """
    assert set(MdIntent.__table__.c.keys()) == {
        "session_id",
        "instance",
        "feeds",
        "atoms",
        "generation",
        "created_at",
        "released_at",
    }
    assert [c.name for c in MdIntent.__table__.primary_key] == [
        "session_id",
        "instance",
    ]
    assert MdIntent.__table__.c.released_at.nullable

    assert set(TdIntent.__table__.c.keys()) == {
        "session_id",
        "api_id",
        "created_at",
        "released_at",
    }
    assert [c.name for c in TdIntent.__table__.primary_key] == [
        "session_id",
        "api_id",
    ]

    assert set(MdSelectorState.__table__.c.keys()) == {
        "spec_hash",
        "universe",
        "epoch",
        "center",
        "updated_at",
    }
    assert [c.name for c in MdSelectorState.__table__.primary_key] == [
        "spec_hash"
    ]
    assert MdSelectorState.__table__.c.center.nullable
    assert MdSelectorState.__table__.c.universe.nullable is False

    standing = set(MdStandingSubscription.__table__.c.keys())
    assert "session_id" not in standing
    assert "released_at" not in standing
    assert {"id", "instance", "declaration"} <= standing

    names = set(Base.metadata.tables)
    assert {
        "strategy_registry",
        "code_versions",
        "artifacts",
        "sts_artifacts",
    }.isdisjoint(names)
    digest = StsSessionRow.__table__.c.strategy_digest
    env_generation = StsSessionRow.__table__.c.env_generation
    assert digest.nullable
    assert digest.type.length == 71
    assert env_generation.nullable


async def test_a_new_session_row_uses_the_f11_defaults(database_url: str) -> None:
    async with a_database(database_url) as database, database.maker() as db:
        await an_owner(db)
        repo = StsSessionRepository(db)
        await repo.create_live(session_id="s", created_by=1, type="NoopStrategy")
        row = await repo.get_by_session_id("s")
        assert row is not None
        assert row.restart == "never"
        assert row.generation == 1
        assert row.restart_count == 0
        assert row.observed_generation is None
        assert row.worker_incarnation is None
        assert row.conditions == {}
        assert row.rebuild_count == 0
        assert row.st_facts == {}

        row.restart = "on_failure"
        await db.flush()
        await db.refresh(row)
        assert row.restart == "on_failure"


async def test_cancel_on_disconnect_defaults_off(database_url: str) -> None:
    async with a_database(database_url) as database, database.maker() as db:
        await an_owner(db)
        instance = await an_instance(db)
        api = Api(
            owner_id=1,
            venue="Paper",
            api_key="k",
            api_secret="s",
            type=ApiType.HMAC.value,
            instance_id=instance.id,
        )
        db.add(api)
        await db.flush()
        assert api.cancel_on_disconnect is False


async def test_intent_identity_is_one_row_and_release_keeps_it(
    database_url: str,
) -> None:
    """F38: ending an intent timestamps the row. It does not delete it
    and it does not insert another.
    """
    async with a_database(database_url) as database, database.maker() as db:
        db.add(MdIntent(session_id="s", instance="md-jp", feeds=["ticker.X"]))
        db.add(TdIntent(session_id="s", api_id=7))
        await db.flush()

        md = await db.get(MdIntent, ("s", "md-jp"))
        td = await db.get(TdIntent, ("s", 7))
        assert md is not None and td is not None
        assert md.released_at is None
        assert md.atoms == {}
        assert md.generation == 1
        assert td.released_at is None

        when = datetime(2026, 10, 1, tzinfo=UTC)
        md.released_at = when
        td.released_at = when
        await db.flush()
        assert (await db.get(MdIntent, ("s", "md-jp"))).released_at == when
        assert (await db.get(TdIntent, ("s", 7))).released_at == when

        with pytest.raises(IntegrityError, match="UNIQUE|duplicate key"):
            await db.execute(
                sa.insert(MdIntent).values(
                    session_id="s",
                    instance="md-jp",
                    feeds=[],
                    atoms={},
                    generation=1,
                )
            )
        await db.rollback()


async def test_selector_state_is_one_row_per_spec_hash(database_url: str) -> None:
    async with a_database(database_url) as database, database.maker() as db:
        db.add(
            MdSelectorState(
                spec_hash="h",
                universe=["BTC-1"],
                epoch=3,
                center={"strike": "100"},
            )
        )
        await db.flush()
        with pytest.raises(IntegrityError, match="UNIQUE|duplicate key"):
            await db.execute(
                sa.insert(MdSelectorState).values(
                    spec_hash="h", universe=[], epoch=0
                )
            )
        await db.rollback()
