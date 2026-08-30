"""Migration 0027 against a real table — the ``upgrade()`` body, not its helpers.

``test_0027_sts_td_mapping.py`` covers the two pure functions. What is left
untested there is everything around them: the column swap, the ``type`` test
that decides whether ``st_paras`` is touched at all, and the SQL assembled
differently depending on that answer. Those only run against a table.

Each test puts the table back into its pre-0027 shape and runs the migration
over it, so the connection ends in exactly the shape the models declare. That
is not tidiness: for Postgres the harness shares one schema across the whole
database suite, and a test that left ``td_api_ids`` behind would break every
test after it.
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from db_harness import a_database

_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "mftik_db"
    / "migrations"
    / "versions"
    / "0027_sts_td_mapping.py"
)


def _migration():
    """The module by path — a name starting with a digit cannot be imported."""
    spec = importlib.util.spec_from_file_location("m0027_up", _PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


#: Only the columns a seeded row has to fill; the rest are nullable or carry a
#: server default. Typed rather than raw SQL so JSON round-trips the same way
#: on both engines.
_PRE = sa.Table(
    "sts_sessions",
    sa.MetaData(),
    sa.Column("session_id", sa.String(64), primary_key=True),
    sa.Column("created_by", sa.Integer),
    sa.Column("type", sa.String(128)),
    sa.Column("status", sa.String(16)),
    sa.Column("restart", sa.String(8)),
    sa.Column("rebuild_count", sa.Integer),
    sa.Column("td_api_ids", sa.JSON),
    sa.Column("md_ids", sa.JSON),
    sa.Column("st_paras", sa.JSON),
    sa.Column("st_facts", sa.JSON),
)

_POST = sa.Table(
    "sts_sessions",
    sa.MetaData(),
    sa.Column("session_id", sa.String(64), primary_key=True),
    sa.Column("td", sa.JSON),
    sa.Column("st_paras", sa.JSON),
)

_USERS = sa.Table(
    "users",
    sa.MetaData(),
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("display_name", sa.String(128)),
)

OWNER_ID = 1


def _row(session_id: str, **over: Any) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "created_by": OWNER_ID,
        "type": "NoopStrategy",
        "status": "live",
        "restart": "always",
        "rebuild_count": 0,
        "td_api_ids": [],
        "md_ids": [],
        "st_paras": {},
        "st_facts": {},
        **over,
    }


def _seed_and_upgrade(conn: sa.Connection, rows: list[dict[str, Any]]) -> None:
    """Undo 0027 on the schema, write ``rows`` as 0026 saw them, migrate."""
    ops = Operations(MigrationContext.configure(conn))
    with Operations.context(ops.migration_context):
        ops.drop_column("sts_sessions", "td")
        ops.add_column(
            "sts_sessions",
            sa.Column("td_api_ids", sa.JSON(), nullable=False, server_default="[]"),
        )
        conn.execute(_USERS.insert().values(id=OWNER_ID, display_name=""))
        conn.execute(_PRE.insert(), rows)
        _migration().upgrade()


@dataclass(frozen=True)
class Migrated:
    """What the table holds once ``upgrade()`` has run over the seed."""

    rows: dict[str, sa.Row[Any]]
    columns: set[str]

    def __getitem__(self, session_id: str) -> sa.Row[Any]:
        return self.rows[session_id]


def _read(conn: sa.Connection) -> Migrated:
    rows = {row.session_id: row for row in conn.execute(sa.select(_POST)).all()}
    columns = {
        c["name"] for c in sa.inspect(conn).get_columns("sts_sessions")
    }
    return Migrated(rows=rows, columns=columns)


@pytest.fixture
async def migrated(database_url):
    """Run ``upgrade()`` over seeded rows; hand back what the table holds."""

    async def run(rows: list[dict[str, Any]]) -> Migrated:
        async with a_database(database_url) as database:
            async with database.engine.begin() as conn:
                await conn.run_sync(_seed_and_upgrade, rows)
                return await conn.run_sync(_read)

    return run


async def test_the_old_list_becomes_a_mapping_keyed_by_api_id(migrated) -> None:
    table = await migrated([_row("s-1", td_api_ids=[3, 7])])

    assert table["s-1"].td == {
        "account-3": {"api_id": 3},
        "account-7": {"api_id": 7},
    }


async def test_a_session_with_no_accounts_gets_an_empty_mapping(migrated) -> None:
    table = await migrated([_row("s-none")])

    assert table["s-none"].td == {}


async def test_the_old_column_is_gone_and_the_new_one_is_there(
    migrated,
) -> None:
    """``td_api_ids`` survives as a property on the model, not as a column."""
    table = await migrated([_row("s-1", td_api_ids=[3])])

    assert "td" in table.columns
    assert "td_api_ids" not in table.columns


async def test_cross_arb_gets_its_account_names_from_the_mapping(
    migrated,
) -> None:
    """Without this a session live across the upgrade cannot rebuild.

    ``CrossArb.on_initialized`` now requires the two names, and it runs on the
    rebuild path — so a row whose ``st_paras`` predates them fails there, not
    here, where nothing is left to explain what went wrong.
    """
    table = await migrated(
        [
            _row(
                "s-arb",
                type="CrossArb",
                td_api_ids=[11, 22],
                st_paras={"qty": "0.001"},
            )
        ]
    )

    assert table["s-arb"].st_paras == {
        "qty": "0.001",
        "quote_account": "account-11",
        "hedge_account": "account-22",
    }


async def test_cross_arb_names_already_written_are_left_alone(migrated) -> None:
    paras = {"quote_account": "binance quoter", "hedge_account": "gate hedger"}
    table = await migrated(
        [_row("s-named", type="CrossArb", td_api_ids=[11, 22], st_paras=paras)]
    )

    assert table["s-named"].st_paras == paras


async def test_only_cross_arb_rows_have_st_paras_touched(migrated) -> None:
    """The backfill is keyed on ``type``; every other strategy is left as is."""
    table = await migrated(
        [
            _row("s-noop", td_api_ids=[11, 22], st_paras={"qty_quote": 100}),
            _row(
                "s-arb",
                type="CrossArb",
                td_api_ids=[11, 22],
                st_paras={"qty": "0.001"},
            ),
        ]
    )

    assert table["s-noop"].st_paras == {"qty_quote": 100}
    assert "quote_account" in table["s-arb"].st_paras
