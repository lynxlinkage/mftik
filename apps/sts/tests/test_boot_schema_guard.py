"""STS will not serve a database that predates ``0034_strategy_type_key``.

The deploy has an order — stop STS and the API, rename the registry trees,
run Alembic, then start this build — and this is what happens when it is run
out of it. Starting anyway is the worst of the options: rows written before
that migration carry their strategy's short name in a column this build does
not select, so each of them reads as a session that names no strategy, and
the rebuild scan that reads them runs once, at boot.

A cold start is the other half of it. Nothing orders STS after the migration
step in the compose stack, so the first reads can fail, find no tables, or
find one revision too few — each of which fixes itself in a few seconds. So
the wait is real and bounded, and running out of it is fatal rather than a
reason to serve.
"""

from __future__ import annotations

import logging

import pytest
from mftik_db.schema import SchemaTooOld
from mftik_sts import app


# over the 50 ms unit call cap; still inside component
@pytest.mark.component
@pytest.mark.real_sleep(
    reason="STS schema check still sleeps on the wall clock"
)
async def test_a_schema_that_is_too_old_stops_the_process(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def refuse(engine: object | None = None) -> None:
        raise SchemaTooOld("0034_strategy_type_key has not run")

    monkeypatch.setattr(app, "require_sts_schema", refuse)
    with caplog.at_level(logging.WARNING, logger="sts"):
        assert await app.schema_is_current(0.05) is False
    assert "0034_strategy_type_key has not run" in caplog.text
    assert "STS will not start" in caplog.text


@pytest.mark.real_sleep(
    reason="STS schema check still sleeps on the wall clock"
)
async def test_a_database_that_is_not_up_yet_is_waited_for(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Postgres answering late is the compose stack's normal cold start."""
    attempts: list[int] = []

    async def late(engine: object | None = None) -> None:
        attempts.append(1)
        if len(attempts) < 3:
            raise OSError("connection refused")

    monkeypatch.setattr(app, "require_sts_schema", late)
    monkeypatch.setattr(app, "_SCHEMA_RETRY_S", 0.01)
    monkeypatch.setattr(app, "_SCHEMA_RETRY_MAX_S", 0.01)
    with caplog.at_level(logging.WARNING, logger="sts"):
        assert await app.schema_is_current(5.0) is True
    assert len(attempts) == 3
    assert "waiting for the database" in caplog.text
    assert "connection refused" in caplog.text


# over the 50 ms unit call cap; still inside component
@pytest.mark.component
@pytest.mark.real_sleep(
    reason="STS schema check still sleeps on the wall clock"
)
async def test_a_database_that_never_answers_stops_the_process(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The window runs out. It does not serve a schema nobody checked."""

    async def never(engine: object | None = None) -> None:
        raise OSError("connection refused")

    monkeypatch.setattr(app, "require_sts_schema", never)
    monkeypatch.setattr(app, "_SCHEMA_RETRY_S", 0.01)
    monkeypatch.setattr(app, "_SCHEMA_RETRY_MAX_S", 0.01)
    with caplog.at_level(logging.ERROR, logger="sts"):
        assert await app.schema_is_current(0.05) is False
    assert "could not be read" in caplog.text


@pytest.mark.real_sleep(
    reason="STS schema check still sleeps on the wall clock"
)
async def test_a_migration_that_lands_mid_wait_is_picked_up(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """A schema one revision short is a state, not a verdict.

    The migration step runs while STS is already up in the compose stack, so
    the database passes through "still has sts_sessions.strategy" on its way
    to the revision this build needs.
    """
    attempts: list[int] = []

    async def migrating(engine: object | None = None) -> None:
        attempts.append(1)
        if len(attempts) == 1:
            raise SchemaTooOld("no sts_sessions table yet")
        if len(attempts) == 2:
            raise SchemaTooOld("still has the sts_sessions.strategy column")

    monkeypatch.setattr(app, "require_sts_schema", migrating)
    monkeypatch.setattr(app, "_SCHEMA_RETRY_S", 0.01)
    monkeypatch.setattr(app, "_SCHEMA_RETRY_MAX_S", 0.01)
    assert await app.schema_is_current(5.0) is True
    assert len(attempts) == 3


async def test_the_window_is_configurable(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(app.SCHEMA_WAIT_ENV, "45")
    assert app._schema_wait_s() == 45.0
    monkeypatch.setenv(app.SCHEMA_WAIT_ENV, "soon")
    assert app._schema_wait_s() == app.SCHEMA_WAIT_S


def test_the_process_supervisor_takes_the_pin_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """B3-07: this plane passes the pin path; it does not read the env itself."""
    pin = tmp_path / "pinned-releases"
    seen: dict[str, object] = {}

    class _Supervisor:
        def __init__(self, work_dir, *, plane, instance, pin_path=None, budget=None):
            seen["work_dir"] = work_dir
            seen["plane"] = plane
            seen["instance"] = instance
            seen["pin_path"] = pin_path
            seen["budget"] = budget

    monkeypatch.setattr("mftik.procman.Supervisor", _Supervisor)
    monkeypatch.setattr("mftik.procman.pinned_releases_path", lambda: pin)
    app._open_supervisor()
    assert seen["plane"] == "sts"
    assert seen["instance"] == app.INSTANCE
    assert seen["pin_path"] == pin
    assert seen["budget"] is None
    assert seen["work_dir"] == app._supervisor_work_dir("sts", app.INSTANCE)


async def test_amain_returns_without_serving_anything(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """No broker, no RPC loops, no sessions — the process exits non-zero."""

    async def refuse() -> bool:
        return False

    monkeypatch.setattr(app, "schema_is_current", refuse)
    assert await app.amain() is False
