"""``MFTIK_DB_POOL_SIZE`` sizes a worker's pool, and only a worker's."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from mftik_db import session as session_mod
from mftik_db.session import POOL_SIZE_ENV, build_engine


def test_unset_pool_env_leaves_the_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(POOL_SIZE_ENV, raising=False)
    assert session_mod._pool_kwargs("postgresql+asyncpg://localhost/mftik") == {}


def test_pool_size_also_sets_max_overflow(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(POOL_SIZE_ENV, "1")
    assert session_mod._pool_kwargs("postgresql+asyncpg://localhost/mftik") == {
        "pool_size": 1,
        "max_overflow": 0,
    }


def test_sqlite_does_not_take_pool_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(POOL_SIZE_ENV, "1")
    assert session_mod._pool_kwargs("sqlite+aiosqlite:///:memory:") == {}
    # Passing pool_size to this URL raises. Getting an engine back is the
    # assertion.
    engine = build_engine("sqlite+aiosqlite:///:memory:")
    engine.sync_engine.dispose()


def test_invalid_pool_size_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(POOL_SIZE_ENV, "nope")
    assert session_mod._pool_kwargs("postgresql+asyncpg://localhost/mftik") == {}
    monkeypatch.setenv(POOL_SIZE_ENV, "0")
    assert session_mod._pool_kwargs("postgresql+asyncpg://localhost/mftik") == {}


def test_non_sqlite_engine_receives_both_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(POOL_SIZE_ENV, "1")
    captured: dict = {}

    def fake(url: str, **kwargs: object) -> MagicMock:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return MagicMock()

    monkeypatch.setattr(session_mod, "create_async_engine", fake)
    session_mod.build_engine("postgresql+asyncpg://localhost/mftik")
    assert captured["kwargs"]["pool_size"] == 1
    assert captured["kwargs"]["max_overflow"] == 0
    assert captured["kwargs"]["pool_pre_ping"] is True
