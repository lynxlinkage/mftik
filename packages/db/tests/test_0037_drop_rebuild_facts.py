"""0037 drops rebuild_count and st_facts without rewriting anything else.

The rows are synthetic. A main-shaped database is 0034; this test fills
one, upgrades, checks the surviving columns, downgrades, and upgrades
again. Postgres uses its own schema so the shared test database's public
tables stay put.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa
from prod_shape import (
    HEAD_REVISION,
    PRE_REVISION,
    alembic_problems,
    async_url_for,
    compare_surviving,
    database_name,
    dropped_columns_absent,
    dropped_columns_are_defaults,
    fill,
    history_problems,
    migrate,
    require_explicit_database,
    revision_of,
    sequences_continue,
    shape_is_0034,
    snapshot,
)
from sqlalchemy import text
from sqlalchemy.pool import NullPool


def _sync_url(url: str, path: Path) -> str:
    if url.startswith("sqlite"):
        return f"sqlite:///{path / 'shape.db'}"
    return url.replace("postgresql+asyncpg", "postgresql+psycopg", 1)


def _use_schema(connection, schema: str | None) -> None:
    if schema is None:
        connection.execute(text("PRAGMA foreign_keys=ON"))
        return
    connection.execute(text(f'SET search_path TO "{schema}"'))


def _rehearse():
    import importlib.util

    path = Path(__file__).resolve().parents[3] / "scripts" / "b10_01_rehearse.py"
    spec = importlib.util.spec_from_file_location("b10_01_rehearse", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_rehearsal_refuses_an_unnamed_database() -> None:
    url = "postgresql+psycopg://scratch:scratch@127.0.0.1:55432/b10_01_test"
    assert database_name(url) == "b10_01_test"
    require_explicit_database(url, "b10_01_test")
    with pytest.raises(SystemExit, match="does not read DATABASE_URL"):
        require_explicit_database(url, "mftik")
    with pytest.raises(SystemExit, match="does not read DATABASE_URL"):
        require_explicit_database(url, "")
    script = _rehearse()
    with pytest.raises(SystemExit, match="does not read DATABASE_URL"):
        script.main(
            ["snapshot", "--url", url, "--database", "other", "--out", "x.json"]
        )
    async_url = url.replace("postgresql+psycopg", "postgresql+asyncpg", 1)
    with pytest.raises(SystemExit, match="sync driver"):
        script.main(
            [
                "snapshot",
                "--url",
                async_url,
                "--database",
                "b10_01_test",
                "--out",
                "x.json",
            ]
        )
    source = (
        Path(__file__).resolve().parents[3] / "scripts" / "b10_01_rehearse.py"
    ).read_text()
    assert "os.environ" not in source
    assert "getenv" not in source
    assert "dotenv" not in source


# Full migration chain, twice, against a real engine.
@pytest.mark.integration
async def test_main_shaped_rows_survive_the_drop(
    database_url: str, tmp_path: Path
) -> None:
    sync_url = _sync_url(database_url, tmp_path)
    postgres = sync_url.startswith("postgresql")
    schema = "b10s" + uuid.uuid4().hex[:8] if postgres else None
    engine = sa.create_engine(sync_url, poolclass=NullPool)
    try:
        with engine.connect() as connection:
            if schema is not None:
                connection.execute(text(f'CREATE SCHEMA "{schema}"'))
                connection.commit()
            _use_schema(connection, schema)
            connection.commit()
            migrate(connection, PRE_REVISION)
            fill(connection, rows=2)
            connection.commit()
            taken = snapshot(connection)
            assert taken["revision"] == PRE_REVISION
            assert taken["tables"]["sts_sessions"]["count"] >= 5
            assert "rebuild_count" not in taken["tables"]["sts_sessions"]["surviving"]
            assert "st_facts" not in taken["tables"]["sts_sessions"]["surviving"]

            migrate(connection, "head")
            connection.commit()
            problems = (
                compare_surviving(connection, taken)
                + dropped_columns_absent(connection)
                + sequences_continue(connection)
                + alembic_problems(connection)
            )
            assert problems == []
            assert revision_of(connection) == HEAD_REVISION

        read_problems = await history_problems(async_url_for(sync_url), schema)
        assert read_problems == []

        with engine.connect() as connection:
            _use_schema(connection, schema)
            migrate(connection, PRE_REVISION)
            connection.commit()
            problems = (
                compare_surviving(connection, taken)
                + shape_is_0034(connection, taken)
                + dropped_columns_are_defaults(connection)
            )
            assert problems == []

            migrate(connection, "head")
            connection.commit()
            problems = (
                compare_surviving(connection, taken)
                + dropped_columns_absent(connection)
                + alembic_problems(connection)
            )
            assert problems == []
    finally:
        if schema is not None:
            with engine.connect() as connection:
                connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
                connection.commit()
        engine.dispose()
