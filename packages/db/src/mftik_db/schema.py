"""Whether the database on the other end has run the migrations a build needs.

A process that starts against a schema older than its own code does not fail
on the first query — it fails on the one row that happens to need the column
that is not there yet, hours later. This is read once at boot, and the answer
is a sentence naming the revision to run.

``0034_strategy_type_key`` is the floor for STS because it is the migration
that made ``sts_sessions.type`` the only strategy identity. Before it, a row
written by an older build keeps its short name in the dropped ``strategy``
column, which the current ORM model does not select: every such session reads
as a row that names no strategy at all.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from mftik_db.session import get_engine

#: The oldest schema STS may serve. Named, not numbered, because this is the
#: string ``alembic upgrade`` takes.
MIN_STS_REVISION = "0034_strategy_type_key"


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
        # No table at all is a database nothing has migrated yet, not a
        # revision that is behind — whatever creates it creates it at head,
        # and every read until then fails on its own. Refusing here would
        # turn a service that merely started before the migration step into
        # a container that stays down.
        return None
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
