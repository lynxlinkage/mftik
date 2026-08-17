from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from mftik_db.config import DatabaseConfig

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine(config: DatabaseConfig | None = None) -> AsyncEngine:
    global _engine
    if _engine is None:
        cfg = config or DatabaseConfig.from_env()
        _engine = create_async_engine(cfg.url, pool_pre_ping=True)
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
