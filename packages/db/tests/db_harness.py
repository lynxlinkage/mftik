"""One place that builds the database a test runs against.

Nine fixtures across five packages each stood up their own engine, and they
had drifted into four shapes of the same four lines. That mattered less as
duplication than as a single point of control: the URL was hardcoded in every
one of them, so pointing the suite at anything other than in-memory sqlite
would have meant editing all nine. Here it is an argument.

``scope`` is the shape most tests want. It mirrors ``mftik_db.session_scope``
exactly — commit on clean exit, rollback and re-raise otherwise — because the
code under test is usually handed one and cannot tell the difference.

**Why more than sqlite.** sqlite is not a strict SQL engine and does not
pretend to be: it has no decimal type, and it ignores ``VARCHAR`` length
entirely — it will store 107 characters in a ``String(64)`` column without
complaint, which is exactly how a 107-character Bybit cursor reached production
before the column that had to hold it was found to be too small. A suite that
only runs here can confirm what we already believed and little else. Set
``TEST_POSTGRES_URL`` and every database test runs a second time against the
engine production actually writes to.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass

from mftik_db.models import Base, User
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool, StaticPool

#: Every connection to ``:memory:`` opens a database of its own unless the pool
#: hands out the same one, and a writer that opens a session per flush would
#: otherwise write into a database nothing else can read.
SQLITE_URL = "sqlite+aiosqlite:///:memory:"

#: Where a real Postgres is, if there is one. Unset locally is fine; unset in
#: CI is not, and ``conftest.py`` refuses to run that way.
POSTGRES_URL_ENV = "TEST_POSTGRES_URL"


def dialect_urls() -> dict[str, str]:
    """The databases this run tests against, keyed by the id pytest shows."""
    urls = {"sqlite": SQLITE_URL}
    postgres = os.getenv(POSTGRES_URL_ENV)
    if postgres:
        urls["postgres"] = postgres
    return urls


#: Sessions, accounts and strategies are all created *by* somebody, and the
#: column saying so is a foreign key. Tests that do not care who name this one.
OWNER_ID = 1


async def an_owner(session: AsyncSession, user_id: int = OWNER_ID) -> User:
    """The user row a ``created_by`` points at.

    sqlite does not enforce foreign keys unless a pragma asks it to, so tests
    naming a creator who does not exist passed for as long as sqlite was the
    only engine. Postgres refuses the insert, correctly.
    """
    user = User(id=user_id, email=f"owner-{user_id}@test.invalid")
    session.add(user)
    await session.flush()
    return user


@dataclass(frozen=True)
class Database:
    """A live database and the three ways tests reach into it."""

    engine: AsyncEngine
    maker: async_sessionmaker[AsyncSession]
    scope: Callable[[], AbstractAsyncContextManager[AsyncSession]]


def _is_sqlite(url: str) -> bool:
    return url.startswith("sqlite")


def _engine_kwargs(url: str) -> dict:
    if _is_sqlite(url):
        return {
            "poolclass": StaticPool,
            "connect_args": {"check_same_thread": False},
        }
    # NullPool because pytest-asyncio gives each test its own event loop, and a
    # pooled asyncpg connection handed to the next test belongs to a loop that
    # has already closed. Nothing is kept, so nothing is stale — at the cost of
    # a connect per session, which against localhost is not worth optimising.
    return {"poolclass": NullPool}


#: Servers, unlike ``:memory:``, outlive the test that used them. Their engine
#: and schema are built once and the rows are cleared between tests instead.
_shared: dict[str, AsyncEngine] = {}


def _enforce_foreign_keys(engine: AsyncEngine) -> None:
    """Make sqlite check the constraints it declares but ignores by default.

    Without this the two engines disagree about what a valid row is, and the
    cheap one is the permissive one — so a test written here would pass and the
    same test would fail on the engine production uses. Closing the gap is
    worth more than the handful of rows it forces tests to seed.
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _pragma(dbapi_connection, _record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def _refuse_a_real_database(url: str) -> None:
    """Setup drops every table, so the name has to say it is disposable.

    ``docker compose`` puts a development database on the same host and port a
    developer would reach for first, and one exported variable is all that
    stands between a test run and dropping it.
    """
    name = url.rsplit("/", 1)[-1].split("?")[0]
    if "test" not in name.lower():
        raise RuntimeError(
            f"{POSTGRES_URL_ENV} points at {name!r}, which is not named as a "
            "test database. Setup drops every table; point it at one that is."
        )


async def _shared_engine(url: str) -> AsyncEngine:
    engine = _shared.get(url)
    if engine is None:
        _refuse_a_real_database(url)
        engine = create_async_engine(url, **_engine_kwargs(url))
        async with engine.begin() as conn:
            # Drop first: a previous run that died mid-test leaves its tables
            # behind, and a stale one differs from the model in exactly the way
            # this suite exists to notice.
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        _shared[url] = engine
    return engine


async def _truncate(engine: AsyncEngine) -> None:
    """Empty every table and rewind its sequence.

    ``RESTART IDENTITY`` is not cosmetic. A fresh ``:memory:`` numbers the
    first row 1, so tests written against sqlite may assume it; without this
    they would pass on the first Postgres test in a file and fail on the rest.
    """
    tables = ", ".join(f'"{t.name}"' for t in Base.metadata.sorted_tables)
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))


@asynccontextmanager
async def a_database(url: str = SQLITE_URL) -> AsyncIterator[Database]:
    """An empty database with the schema on it, cleaned up on the way out."""
    if _is_sqlite(url):
        engine = create_async_engine(url, **_engine_kwargs(url))
        _enforce_foreign_keys(engine)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    else:
        engine = await _shared_engine(url)
        await _truncate(engine)

    maker = async_sessionmaker(engine, expire_on_commit=False)

    @asynccontextmanager
    async def scope() -> AsyncIterator[AsyncSession]:
        async with maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    try:
        yield Database(engine=engine, maker=maker, scope=scope)
    finally:
        if _is_sqlite(url):
            await engine.dispose()
