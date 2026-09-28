"""STS will not serve a database that predates ``0034_strategy_type_key``.

The deploy has an order — stop STS and the API, rename the registry trees,
run Alembic, then start this build — and this is what happens when it is run
out of it. Starting anyway is the worst of the options: rows written before
that migration carry their strategy's short name in a column this build does
not select, so each of them reads as a session that names no strategy.
"""

from __future__ import annotations

import logging

import pytest
from mftik_db.schema import SchemaTooOld
from mftik_sts import app


async def test_a_schema_that_is_too_old_stops_the_process(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def refuse(engine: object | None = None) -> None:
        raise SchemaTooOld("0034_strategy_type_key has not run")

    monkeypatch.setattr(app, "require_sts_schema", refuse)
    with caplog.at_level(logging.ERROR, logger="sts"):
        assert await app.schema_is_current() is False
    assert "0034_strategy_type_key has not run" in caplog.text


async def test_amain_returns_without_serving_anything(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """No broker, no RPC loops, no sessions — the process exits non-zero."""

    async def refuse() -> bool:
        return False

    monkeypatch.setattr(app, "schema_is_current", refuse)
    assert await app.amain() is False


async def test_a_database_that_cannot_be_read_does_not_stop_boot(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Postgres may simply not be up yet. That is a wait, not a wrong deploy."""

    async def unreachable(engine: object | None = None) -> None:
        raise OSError("connection refused")

    monkeypatch.setattr(app, "require_sts_schema", unreachable)
    with caplog.at_level(logging.ERROR, logger="sts"):
        assert await app.schema_is_current() is True
    assert "could not read the database's migration revision" in caplog.text
