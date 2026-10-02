"""A sequence ahead of max(id) still passes, and a second check does too.

``nextval`` moves the sequence and is not rolled back. Equality with
``max(id) + 1`` fails a restored gap and then fails again on the next
verify. The check is ``nextval > max(id)``, or ``>= 1`` when the table
is empty.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import sqlalchemy as sa
from prod_shape import _id_continues, _serial_columns, sequences_continue
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.pool import NullPool

_TIED = "orders.id next value is not past max(id)"
_BEHIND = "lags.id next value is not past max(id)"


def _sync_url(url: str, path: Path) -> str:
    if url.startswith("sqlite"):
        return f"sqlite:///{path / 'seq.db'}"
    return url.replace("postgresql+asyncpg", "postgresql+psycopg", 1)


def _set_next(connection: Connection, table: str, nxt: int) -> None:
    """Make the next insert into ``table`` take ``nxt``."""
    if connection.dialect.name == "postgresql":
        seq = connection.execute(
            text("SELECT pg_get_serial_sequence(:table, 'id')"),
            {"table": table},
        ).scalar()
        assert seq, table
        connection.execute(
            text("SELECT setval(:seq, :nxt, false)"),
            {"seq": seq, "nxt": nxt},
        )
        return
    updated = connection.execute(
        text("UPDATE sqlite_sequence SET seq = :seq WHERE name = :name"),
        {"seq": nxt - 1, "name": table},
    )
    assert updated.rowcount == 1


def _create(connection: Connection) -> None:
    serial = (
        "SERIAL PRIMARY KEY"
        if connection.dialect.name == "postgresql"
        else "INTEGER PRIMARY KEY AUTOINCREMENT"
    )
    connection.execute(
        text(
            f"CREATE TABLE users ("
            f"id {serial}, display_name TEXT NOT NULL, "
            f"email TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
    )
    connection.execute(
        text(
            f"CREATE TABLE td_sessions ("
            f"id {serial}, session_id TEXT NOT NULL, api_id INTEGER NOT NULL, "
            f"created_by INTEGER NOT NULL, status TEXT NOT NULL, "
            f"created_at TEXT NOT NULL)"
        )
    )
    connection.execute(
        text(
            f"CREATE TABLE md_sessions ("
            f"id {serial}, instance TEXT NOT NULL, venue TEXT NOT NULL, "
            f"session_id TEXT NOT NULL, created_by INTEGER NOT NULL, "
            f"status TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
    )
    for name in ("fills", "orders", "lags"):
        connection.execute(text(f"CREATE TABLE {name} (id {serial})"))
        connection.execute(text(f"INSERT INTO {name} (id) VALUES (4)"))
    connection.execute(text(f"CREATE TABLE quiet (id {serial})"))


@contextmanager
def _opened(database_url: str, tmp_path: Path) -> Iterator[Connection]:
    postgres = database_url.startswith("postgresql")
    engine = sa.create_engine(
        _sync_url(database_url, tmp_path), poolclass=NullPool
    )
    schema = "b10seq" + uuid.uuid4().hex[:8] if postgres else None
    try:
        with engine.connect() as connection:
            if schema is not None:
                connection.execute(text(f'CREATE SCHEMA "{schema}"'))
                connection.execute(text(f'SET search_path TO "{schema}"'))
                connection.commit()
            _create(connection)
            connection.commit()
            yield connection
    finally:
        if schema is not None:
            with engine.connect() as connection:
                connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
                connection.commit()
        engine.dispose()


def _arm(connection: Connection, nxt: dict[str, int]) -> None:
    for table, value in nxt.items():
        _set_next(connection, table, value)
    connection.commit()


def test_an_id_at_or_below_the_max_does_not_continue() -> None:
    assert _id_continues(4, 4) is False
    assert _id_continues(2, 4) is False
    assert _id_continues(0, None) is False
    assert _id_continues(5, 4) is True
    assert _id_continues(40, 4) is True
    assert _id_continues(1, None) is True


def test_a_sequence_ahead_of_max_passes(
    database_url: str, tmp_path: Path
) -> None:
    with _opened(database_url, tmp_path) as connection:
        _arm(connection, {"fills": 40, "orders": 5, "lags": 5})
        checked = {table for table, _column in _serial_columns(connection)}
        assert {"fills", "orders", "lags", "quiet"} <= checked
        assert sequences_continue(connection) == []


def test_two_verifies_both_pass(database_url: str, tmp_path: Path) -> None:
    with _opened(database_url, tmp_path) as connection:
        _arm(connection, {"fills": 5, "orders": 5, "lags": 5})
        assert sequences_continue(connection) == []
        assert sequences_continue(connection) == []


def test_a_next_value_at_or_behind_max_fails(
    database_url: str, tmp_path: Path
) -> None:
    """Postgres ``nextval`` can hand out an id that is already used.

    Sqlite will not. Its next rowid is one past the larger of the
    sequence counter and ``max(id)``, so a counter left behind the
    rows still continues. The shared comparison rejects that id; this
    test is the engine path that can actually produce one.
    """
    with _opened(database_url, tmp_path) as connection:
        # orders hands out max(id) itself. lags hands out something smaller.
        _arm(connection, {"fills": 5, "orders": 4, "lags": 2})
        problems = set(sequences_continue(connection))
        if connection.dialect.name == "postgresql":
            assert problems == {_TIED, _BEHIND}
        else:
            assert problems == set()
