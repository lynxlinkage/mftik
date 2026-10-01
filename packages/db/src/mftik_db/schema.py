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

``0035_sts_abort_target`` is the floor for STS. The ORM selects that column
on every session read, so a database that does not have it fails the query
instead of serving. ``0034_strategy_type_key`` is still checked on its own:
before it, a row keeps its short name in the dropped ``strategy`` column,
which this model does not select, and every such session reads as naming no
strategy at all.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from mftik_db.session import get_engine

#: The oldest schema STS may serve. Named, not numbered, because this is the
#: string ``alembic upgrade`` takes.
MIN_STS_REVISION = "0035_sts_abort_target"


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

    The dropped column is the test rather than the revision number: dropping
    ``sts_sessions.strategy`` is the whole of what 0034 changes about the
    shape, so its absence is the one fact that cannot be true before 0034 and
    false after it. The revision id is read to *name* what is on the database
    in the refusal, and to catch a history that is behind for some other
    reason.
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
            f"column, so {MIN_STS_REVISION} has not run. This build reads a "
            "strategy's identity from sts_sessions.type alone, and every "
            "session written before that migration would read as naming no "
            "strategy. Stop the old STS and API, run mftik-db-migrate "
            f"{MIN_STS_REVISION}, then start this build."
        )
    if "abort_target" not in state.sts_columns:
        return (
            f"the database {at} has no sts_sessions.abort_target column, so "
            f"{MIN_STS_REVISION} has not run. This build selects that column "
            "on every session read, and stores a create abort there so a "
            "restart can still kill a session the API already reported as "
            "failed. Run mftik-db-migrate before starting STS."
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
