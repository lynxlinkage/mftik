"""GET /logs/{domain}/{stream_id} — cursor pagination over persisted logs."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException
from mft_api.routes import logs as logs_routes


class _FakeDb:
    pass


def _session_scope_stub():
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def scope() -> Any:
        yield _FakeDb()

    return scope


def _row(
    *,
    db_id: int,
    envelope_id: str,
    ts: float,
    message: str = "msg",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=db_id,
        envelope_id=envelope_id,
        ts=ts,
        source="sts",
        level="info",
        message=message,
    )


async def test_invalid_domain_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        await logs_routes.list_session_logs("xx", "s1")
    assert exc.value.status_code == 400


async def test_list_newest_and_has_more(monkeypatch: pytest.MonkeyPatch) -> None:
    stored = [
        _row(db_id=4, envelope_id="e4", ts=4.0),
        _row(db_id=3, envelope_id="e3", ts=3.0),
        _row(db_id=2, envelope_id="e2", ts=2.0),
    ]

    class FakeRepo:
        def __init__(self, _db: Any) -> None:
            pass

        async def list_before(
            self,
            domain: str,
            stream_id: str,
            *,
            before_ts: float | None = None,
            before_id: int | None = None,
            limit: int = 100,
        ) -> list[SimpleNamespace]:
            assert domain == "sts"
            assert stream_id == "sess-1"
            assert before_ts is None
            return stored[:limit]

    monkeypatch.setattr(logs_routes, "session_scope", _session_scope_stub())
    monkeypatch.setattr(logs_routes, "SessionLogRepository", FakeRepo)

    result = await logs_routes.list_session_logs(
        "sts", "sess-1", before_ts=None, before_id=None, limit=2
    )
    assert [log.id for log in result.logs] == ["e4", "e3"]
    assert result.has_more is True
    assert result.logs[0].db_id == 4


async def test_list_with_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeRepo:
        def __init__(self, _db: Any) -> None:
            pass

        async def list_before(
            self,
            domain: str,
            stream_id: str,
            *,
            before_ts: float | None = None,
            before_id: int | None = None,
            limit: int = 100,
        ) -> list[SimpleNamespace]:
            assert before_ts == 3.0
            assert before_id == 30
            return [_row(db_id=2, envelope_id="e2", ts=2.0)]

    monkeypatch.setattr(logs_routes, "session_scope", _session_scope_stub())
    monkeypatch.setattr(logs_routes, "SessionLogRepository", FakeRepo)

    result = await logs_routes.list_session_logs(
        "sts", "sess-1", before_ts=3.0, before_id=30, limit=100
    )
    assert len(result.logs) == 1
    assert result.logs[0].id == "e2"
    assert result.has_more is False
