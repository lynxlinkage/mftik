"""One place that builds the database a test runs against.

Nine fixtures across five packages each stood up their own engine, and they had
drifted into four shapes of the same four lines. That matters less as
duplication than as a single point of control: the URL was hardcoded in every
one of them, so pointing the suite at anything other than in-memory sqlite
would have meant editing all nine. Here it is an argument.

``scope`` is the shape most tests want. It mirrors ``mft_db.session_scope``
exactly — commit on clean exit, rollback and re-raise otherwise — because the
code under test is usually handed one and cannot tell the difference.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass

from mft_db.models import Base
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

#: Every connection to ``:memory:`` opens a database of its own unless the pool
#: hands out the same one, and a writer that opens a session per flush would
#: otherwise write into a database nothing else can read.
DEFAULT_URL = "sqlite+aiosqlite:///:memory:"


@dataclass(frozen=True)
class Database:
    """A live database and the three ways tests reach into it."""

    engine: AsyncEngine
    maker: async_sessionmaker[AsyncSession]
    scope: Callable[[], AbstractAsyncContextManager[AsyncSession]]


def _engine_kwargs(url: str) -> dict:
    if url.startswith("sqlite"):
        return {
            "poolclass": StaticPool,
            "connect_args": {"check_same_thread": False},
        }
    return {}


@asynccontextmanager
async def a_database(url: str = DEFAULT_URL) -> AsyncIterator[Database]:
    """An empty database with the schema on it, disposed on the way out."""
    engine = create_async_engine(url, **_engine_kwargs(url))
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
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
        await engine.dispose()
