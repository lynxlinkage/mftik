"""0034 keeps ``type`` as a qualified key and drops ``strategy``."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "mftik_db"
    / "migrations"
    / "versions"
    / "0034_strategy_type_key.py"
)


def _migration():
    spec = importlib.util.spec_from_file_location("m0034_up", _PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _up(conn: sa.Connection) -> None:
    ops = Operations(MigrationContext.configure(conn))
    with Operations.context(ops.migration_context):
        _migration().upgrade()


def _down(conn: sa.Connection) -> None:
    ops = Operations(MigrationContext.configure(conn))
    with Operations.context(ops.migration_context):
        _migration().downgrade()


def _table(conn: sa.Connection) -> None:
    conn.execute(
        sa.text(
            "CREATE TABLE sts_sessions ("
            " session_id VARCHAR(64) PRIMARY KEY,"
            " status VARCHAR(32) NOT NULL,"
            " strategy VARCHAR(128),"
            " type VARCHAR(256)"
            ")"
        )
    )


def _insert(conn: sa.Connection, rows: list[tuple]) -> None:
    conn.execute(
        sa.text(
            "INSERT INTO sts_sessions (session_id, status, strategy, type)"
            " VALUES (:session_id, :status, :strategy, :type)"
        ),
        [
            {
                "session_id": session_id,
                "status": status,
                "strategy": strategy,
                "type": type_name,
            }
            for session_id, status, strategy, type_name in rows
        ],
    )


def _types(conn: sa.Connection) -> dict[str, str | None]:
    rows = conn.execute(
        sa.text("SELECT session_id, type FROM sts_sessions ORDER BY session_id")
    ).all()
    return {row[0]: row[1] for row in rows}


def _columns(conn: sa.Connection) -> set[str]:
    return {
        row[1]
        for row in conn.execute(sa.text("PRAGMA table_info(sts_sessions)"))
    }


def test_resolved_rules() -> None:
    mod = _migration()
    assert mod._resolved("live", "noop", None) == ("NoopStrategy", False)
    assert mod._resolved("done", "cross_arb", None) == ("CrossArb", False)
    assert mod._resolved("interrupted", "macd_volume", "macd_dollar") == (
        "MacdDollarBars",
        False,
    )
    assert mod._resolved("live", "pr130_probe", "private::Probe") == (
        "private::Probe",
        False,
    )
    assert mod._resolved("live", "CrossArb", "CrossArb") == ("CrossArb", False)
    assert mod._resolved("live", None, None) == (None, False)
    assert mod._resolved("failed", None, None) == (None, False)
    assert mod._resolved("done", "tiny", None) == (None, False)
    assert mod._resolved("failed", "tiny", "not_a_key") == (None, False)
    assert mod._resolved("live", "tiny", None) == (None, True)
    assert mod._resolved("interrupted", None, "not_a_key") == (None, True)


def test_upgrade_rewrites_mapped_rows_and_drops_strategy(tmp_path: Path) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'm.db'}")
    with engine.begin() as conn:
        _table(conn)
        _insert(
            conn,
            [
                ("live-noop", "live", "noop", None),
                ("type-short", "interrupted", "macd_volume", "macd_dollar"),
                ("qualified", "live", "pr130_probe", "private::Probe"),
                ("already", "live", "CrossArb", "CrossArb"),
                ("both-null", "live", None, None),
                ("done-tiny", "done", "tiny", None),
                ("failed-junk", "failed", "tiny", "not_a_key"),
            ],
        )
        _up(conn)
        assert "strategy" not in _columns(conn)
        assert _types(conn) == {
            "live-noop": "NoopStrategy",
            "type-short": "MacdDollarBars",
            "qualified": "private::Probe",
            "already": "CrossArb",
            "both-null": None,
            "done-tiny": None,
            "failed-junk": None,
        }
        _down(conn)
        assert "strategy" in _columns(conn)
        restored = conn.execute(
            sa.text(
                "SELECT session_id, strategy FROM sts_sessions "
                "ORDER BY session_id"
            )
        ).all()
    assert dict(restored)["live-noop"] == "NoopStrategy"
    assert dict(restored)["qualified"] == "private::Probe"
    assert dict(restored)["both-null"] is None


def test_a_live_unmapped_row_stops_the_migration(tmp_path: Path) -> None:
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'm.db'}")
    with engine.begin() as conn:
        _table(conn)
        _insert(
            conn,
            [
                ("aa00c9", "interrupted", "tiny", None),
                ("ok", "live", "noop", None),
            ],
        )
        with pytest.raises(RuntimeError, match="aa00c9"):
            _up(conn)
        assert "strategy" in _columns(conn)
        assert _types(conn) == {"aa00c9": None, "ok": None}
