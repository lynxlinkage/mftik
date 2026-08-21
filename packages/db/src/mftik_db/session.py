from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from mftik_db.config import DatabaseConfig

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _enable_sqlite_foreign_keys(engine: AsyncEngine) -> None:
    """SQLite ignores foreign keys unless a pragma asks. The harness
    already sets this for tests; the runtime engine must too, or a
    SQLite-backed node would drop a Matcher and keep its join rows.
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _pragma(dbapi_connection, _record) -> None:  # noqa: ANN001
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def build_engine(url: str) -> AsyncEngine:
    """An engine with the SQLite FK listener attached when needed.

    ``get_engine`` is a process singleton; tests that must not touch
    it call this instead.
    """
    kwargs: dict = {"pool_pre_ping": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    engine = create_async_engine(url, **kwargs)
    if url.startswith("sqlite"):
        _enable_sqlite_foreign_keys(engine)
    return engine


def get_engine(config: DatabaseConfig | None = None) -> AsyncEngine:
    global _engine
    if _engine is None:
        cfg = config or DatabaseConfig.from_env()
        _engine = build_engine(cfg.url)
    return _engine


def get_async_session_factory(
    config: DatabaseConfig | None = None,
) -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            get_engine(config),
            expire_on_commit=False,
            class_=AsyncSession,
        )
    return _session_factory


@asynccontextmanager
async def session_scope(
    config: DatabaseConfig | None = None,
) -> AsyncIterator[AsyncSession]:
    factory = get_async_session_factory(config)
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
