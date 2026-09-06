"""Migration 0031 against a real table — the seed and the backfill.

The interesting half of ``upgrade()`` is not DDL. It is that three rows appear
without a ``users`` row to attribute them to, and that every existing
credential ends up naming ``td`` before the column is tightened to ``NOT
NULL``. Both only mean anything against a table with rows in it.

Postgres only, and not for convenience: this revision alters a column's
nullability and adds a foreign key, neither of which the sqlite dialect
implements. Production is Postgres and CI sets ``TEST_POSTGRES_URL``
(``conftest.py`` refuses to run there without it), so the case that matters is
covered where it matters.

Each test runs ``downgrade()`` to put the schema back the way 0030 left it,
writes rows as 0030 saw them, then runs ``upgrade()`` — so the connection ends
in exactly the shape the models declare. For Postgres the harness shares one
schema across the database suite, and a test that left ``instances`` dropped
would break every test after it.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

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
    / "0031_plane_instances.py"
)


def _migration():
    """The module by path — a name starting with a digit cannot be imported."""
    spec = importlib.util.spec_from_file_location("m0031_up", _PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_USERS = sa.Table(
    "users",
    sa.MetaData(),
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("display_name", sa.String(128)),
)

#: ``apis`` as 0030 left it: no ``instance_id``.
_PRE_APIS = sa.Table(
    "apis",
    sa.MetaData(),
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("owner_id", sa.Integer),
    sa.Column("venue", sa.String(64)),
    sa.Column("api_key", sa.String(256)),
    sa.Column("api_secret", sa.Text),
    sa.Column("type", sa.String(32)),
)


@pytest.fixture
def postgres_only(database_url: str) -> str:
    if database_url.startswith("sqlite"):
        pytest.skip(
            "0031 alters nullability and adds a foreign key; the sqlite "
            "dialect implements neither"
        )
    return database_url


def _down(conn: sa.Connection) -> Operations:
    ops = Operations(MigrationContext.configure(conn))
    with Operations.context(ops.migration_context):
        _migration().downgrade()
    return ops


def _up(conn: sa.Connection) -> None:
    ops = Operations(MigrationContext.configure(conn))
    with Operations.context(ops.migration_context):
        _migration().upgrade()


async def test_an_empty_database_gets_three_instances_and_needs_no_user(
    postgres_only: str,
) -> None:
    """The case a ``NOT NULL`` ``created_by`` would have failed.

    ``migrate`` waits only on Postgres; ``seed`` — which writes the Owner —
    waits on ``migrate`` finishing. So there is no ``users`` row when this
    revision runs on a database that has never been upgraded before.
    """
    async with a_database(postgres_only) as database:
        async with database.engine.begin() as conn:
            await conn.run_sync(_down)
            await conn.run_sync(_up)

            rows = (
                await conn.execute(
                    sa.text(
                        "SELECT name, domain, enabled, created_by "
                        "FROM instances ORDER BY name"
                    )
                )
            ).all()
            users = (
                await conn.execute(sa.text("SELECT count(*) FROM users"))
            ).scalar_one()

    assert [(r[0], r[1]) for r in rows] == [
        ("md", "md"),
        ("sts", "sts"),
        ("td", "td"),
    ]
    assert all(r[2] is True for r in rows), "seeded instances start enabled"
    assert all(r[3] is None for r in rows), (
        "nobody created these — attributing them to a user is what would have "
        "failed on an empty database"
    )
    assert users == 0


async def test_existing_credentials_are_pointed_at_td_and_then_required(
    postgres_only: str,
) -> None:
    """An upgrade in place: every credential names ``td`` and cannot be null."""
    async with a_database(postgres_only) as database:
        async with database.engine.begin() as conn:
            await conn.run_sync(_down)
            await conn.execute(_USERS.insert().values(id=1, display_name=""))
            await conn.execute(
                _PRE_APIS.insert(),
                [
                    {
                        "owner_id": 1,
                        "venue": "Paper",
                        "api_key": "k1",
                        "api_secret": "s",
                        "type": "HMAC",
                    },
                    {
                        "owner_id": 1,
                        "venue": "Bybit",
                        "api_key": "k2",
                        "api_secret": "s",
                        "type": "HMAC",
                    },
                ],
            )
            await conn.run_sync(_up)

            named = (
                await conn.execute(
                    sa.text(
                        "SELECT a.api_key, i.name FROM apis a "
                        "JOIN instances i ON i.id = a.instance_id "
                        "ORDER BY a.api_key"
                    )
                )
            ).all()
            required = (
                await conn.execute(
                    sa.text(
                        "SELECT attnotnull FROM pg_attribute "
                        "WHERE attrelid = 'apis'::regclass "
                        "AND attname = 'instance_id'"
                    )
                )
            ).scalar_one()

    assert named == [("k1", "td"), ("k2", "td")]
    assert required is True


async def test_retiring_an_instance_a_credential_names_is_refused(
    postgres_only: str,
) -> None:
    """``ON DELETE RESTRICT``, enforced by the database rather than by a check."""
    async with a_database(postgres_only) as database:
        async with database.engine.begin() as conn:
            await conn.run_sync(_down)
            await conn.execute(_USERS.insert().values(id=1, display_name=""))
            await conn.execute(
                _PRE_APIS.insert().values(
                    owner_id=1,
                    venue="Paper",
                    api_key="k1",
                    api_secret="s",
                    type="HMAC",
                )
            )
            await conn.run_sync(_up)

        with pytest.raises(sa.exc.IntegrityError):
            async with database.engine.begin() as conn:
                await conn.execute(
                    sa.text("DELETE FROM instances WHERE name = 'td'")
                )


async def test_two_instances_may_hold_one_venue_for_one_session(
    postgres_only: str,
) -> None:
    """What the relaxed ``md_sessions`` constraint exists to allow.

    The old uniqueness was ``(venue, session_id)``, which refused the second
    row the moment a session's feeds were split across two MDs. The new one
    still refuses a genuine duplicate.
    """
    async with a_database(postgres_only) as database:
        async with database.engine.begin() as conn:
            await conn.run_sync(_down)
            await conn.execute(_USERS.insert().values(id=1, display_name=""))
            await conn.run_sync(_up)
            await conn.execute(
                sa.text(
                    "INSERT INTO md_sessions "
                    "(instance, venue, session_id, created_by, status) VALUES "
                    "('md-jp-1','Bybit','s1',1,'live'), "
                    "('md-jp-2','Bybit','s1',1,'live')"
                )
            )
            held = (
                await conn.execute(
                    sa.text(
                        "SELECT count(*) FROM md_sessions "
                        "WHERE venue='Bybit' AND session_id='s1'"
                    )
                )
            ).scalar_one()

        assert held == 2

        with pytest.raises(sa.exc.IntegrityError):
            async with database.engine.begin() as conn:
                await conn.execute(
                    sa.text(
                        "INSERT INTO md_sessions "
                        "(instance, venue, session_id, created_by, status) "
                        "VALUES ('md-jp-1','Bybit','s1',1,'live')"
                    )
                )
