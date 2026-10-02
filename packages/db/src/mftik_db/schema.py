"""Whether the database on the other end has run the migrations a build needs.

A process that starts against a schema older than its own code does not fail
on the first query — it fails on the one row that happens to need the column
that is not there yet, hours later. This is read at boot, and the answer is a
sentence naming the revision to run.

Every unsatisfactory answer reads the same way on purpose: unreachable, no
tables yet, and one revision short are all states a cold start passes through
while the database and the one-shot migration step come up, and all of them
become the right answer on their own. The caller retries until one does or
until it runs out of patience — see ``mftik_sts.app.schema_is_current``.

Two revisions are load-bearing. ``0034_strategy_type_key`` made
``sts_sessions.type`` the only strategy identity: before it, a row keeps its
short name in the dropped ``strategy`` column, which this ORM does not
select. ``0036_session_code_identity`` is the floor (``MIN_STS_REVISION``).
It adds ``strategy_digest`` and ``env_generation``, which this ORM selects.
``0035_plane_schema`` added the Spec/Status columns underneath those.
A database still at 0034 has already dropped ``strategy`` and is still too
old; a database at 0035 is one revision short of the pins.

``0037_drop_rebuild_facts`` drops ``rebuild_count`` and ``st_facts``. This
ORM no longer selects either column, so a database at 0036 and a database
at 0037 both serve. The floor stays at 0036 for that reason.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from mftik_db.session import get_engine

#: The oldest schema STS may serve. Named, not numbered, because this is the
#: string ``alembic upgrade`` takes.
MIN_STS_REVISION = "0036_session_code_identity"
#: Named in the refusal that keys off the dropped ``strategy`` column. That
#: column can still be present on a database that recorded no revision.
_STRATEGY_IDENTITY_REVISION = "0034_strategy_type_key"


class SchemaTooOld(RuntimeError):
    """The database predates a migration this build cannot work without."""


@dataclass(frozen=True, slots=True)
class SchemaState:
    """What one read of the database found. Both parts can be absent.

    ``revision`` is null when there is no ``alembic_version`` table at all,
    which is what a schema built straight from the models looks like — the
    test suite, and nothing in production. ``sts_columns`` is empty when the
    table itself is missing, which is a database the migrations have not run
    against yet.
    """

    revision: str | None
    sts_columns: frozenset[str]


def _ordinal(revision: str | None) -> int | None:
    """The number a revision id starts with, or None if it does not."""
    if revision is None:
        return None
    head = revision.split("_", 1)[0]
    return int(head) if head.isdigit() else None


def describe_too_old(state: SchemaState) -> str | None:
    """Why this schema is too old for STS, or None when it will serve.

    The dropped ``strategy`` column is the test for 0034: its absence is the
    one fact that cannot be true before that migration and false after it.
    The revision ordinal is what catches a database that has passed 0034 and
    still predates ``MIN_STS_REVISION``.
    """
    if not state.sts_columns:
        # A database nothing has migrated yet. On a cold start that is a
        # state to wait out rather than serve: whatever creates the schema
        # creates it at head, and until then this process cannot tell a
        # database that is coming up from one nobody ever migrated. The
        # caller decides how long to wait; what it must not do is start.
        return (
            "the database has no sts_sessions table, so nothing has "
            "migrated it yet. Run mftik-db-migrate up to at least "
            f"{MIN_STS_REVISION}."
        )
    at = (
        f"is at revision {state.revision}"
        if state.revision is not None
        else "has no alembic revision recorded"
    )
    if "strategy" in state.sts_columns:
        return (
            f"the database {at} and still has the sts_sessions.strategy "
            f"column, so {_STRATEGY_IDENTITY_REVISION} has not run. This "
            "build reads a strategy's identity from sts_sessions.type alone, "
            "and every session written before that migration would read as "
            "naming no strategy. The columns this build selects start at "
            f"{MIN_STS_REVISION}. Stop the old STS and API, run "
            f"mftik-db-migrate {MIN_STS_REVISION}, then start this build."
        )
    floor = _ordinal(MIN_STS_REVISION)
    here = _ordinal(state.revision)
    if floor is not None and here is not None and here < floor:
        return (
            f"the database {at}, which is below {MIN_STS_REVISION}. Run "
            "mftik-db-migrate before starting STS."
        )
    return None


def _read_state(conn: Connection) -> SchemaState:
    inspector = sa.inspect(conn)
    if inspector.has_table("sts_sessions"):
        columns = frozenset(
            column["name"] for column in inspector.get_columns("sts_sessions")
        )
    else:
        columns = frozenset()
    revision: str | None = None
    if inspector.has_table("alembic_version"):
        # ``first``, not ``scalar``: a branched history has a row per head,
        # and reading one of several is still enough to name where it is.
        row = conn.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).first()
        if row is not None:
            revision = str(row[0])
    return SchemaState(revision=revision, sts_columns=columns)


async def read_schema_state(engine: AsyncEngine | None = None) -> SchemaState:
    engine = engine or get_engine()
    async with engine.connect() as conn:
        return await conn.run_sync(_read_state)


async def require_sts_schema(engine: AsyncEngine | None = None) -> None:
    """Raise :class:`SchemaTooOld` when STS must not serve this database."""
    problem = describe_too_old(await read_schema_state(engine))
    if problem is not None:
        raise SchemaTooOld(problem)
