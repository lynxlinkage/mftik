"""0036 adds nullable code-identity pins, on sqlite and on Postgres.

Existing rows stay null. A built-in strategy has no digest, so the
columns cannot be NOT NULL. Downgrade gives them back.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "mftik_db"
    / "migrations"
    / "versions"
    / "0036_session_code_identity.py"
)
_DIGEST = "sha256:" + "ab" * 32


def _migration():
    spec = importlib.util.spec_from_file_location("m0036_up", _PATH)
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
        return f"sqlite:///{path / 'pins.db'}"
    return url.replace("postgresql+asyncpg", "postgresql+psycopg", 1)


def _columns(conn: sa.Connection, schema: str | None) -> set[str]:
    return {
        col["name"]
        for col in sa.inspect(conn).get_columns("sts_sessions", schema=schema)
    }


def _digest_length(conn: sa.Connection, schema: str | None) -> int | None:
    for col in sa.inspect(conn).get_columns("sts_sessions", schema=schema):
        if col["name"] == "strategy_digest":
            return getattr(col["type"], "length", None)
    raise AssertionError("strategy_digest column missing")


def test_0036_adds_nullable_pins_and_can_drop_them(
    database_url: str, tmp_path: Path
) -> None:
    postgres = database_url.startswith("postgresql")
    engine = sa.create_engine(_sync_url(database_url, tmp_path))
    try:
        with engine.begin() as conn:
            schema = "mig0036" if postgres else None
            if postgres:
                conn.execute(sa.text("CREATE SCHEMA mig0036"))
                conn.execute(sa.text("SET LOCAL search_path TO mig0036"))
            meta = sa.MetaData()
            sa.Table(
                "sts_sessions",
                meta,
                sa.Column("session_id", sa.String(64), primary_key=True),
            )
            meta.create_all(conn)
            conn.execute(
                sa.text("INSERT INTO sts_sessions (session_id) VALUES ('s-old')")
            )
            _apply(conn, "upgrade")

            cols = _columns(conn, schema)
            assert "strategy_digest" in cols
            assert "env_generation" in cols
            if postgres:
                assert _digest_length(conn, schema) == 71

            before = conn.execute(
                sa.text(
                    "SELECT strategy_digest, env_generation FROM sts_sessions "
                    "WHERE session_id = 's-old'"
                )
            ).one()
            assert before.strategy_digest is None
            assert before.env_generation is None

            conn.execute(
                sa.text(
                    "UPDATE sts_sessions SET strategy_digest = :digest, "
                    "env_generation = 3 WHERE session_id = 's-old'"
                ),
                {"digest": _DIGEST},
            )
            conn.execute(
                sa.text("INSERT INTO sts_sessions (session_id) VALUES ('s-new')")
            )
            pinned = conn.execute(
                sa.text(
                    "SELECT strategy_digest, env_generation FROM sts_sessions "
                    "WHERE session_id = 's-old'"
                )
            ).one()
            assert pinned.strategy_digest == _DIGEST
            assert pinned.env_generation == 3
            fresh = conn.execute(
                sa.text(
                    "SELECT strategy_digest, env_generation FROM sts_sessions "
                    "WHERE session_id = 's-new'"
                )
            ).one()
            assert fresh.strategy_digest is None
            assert fresh.env_generation is None

            _apply(conn, "downgrade")
            after = _columns(conn, schema)
            assert "strategy_digest" not in after
            assert "env_generation" not in after
    finally:
        if postgres:
            with engine.begin() as conn:
                conn.execute(sa.text("DROP SCHEMA IF EXISTS mig0036 CASCADE"))
        engine.dispose()
