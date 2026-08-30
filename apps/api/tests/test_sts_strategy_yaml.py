"""GET /sts/sessions/{session_id}/yaml — the strategy.yml behind a past deploy.

The stored text comes back byte for byte. A deploy that never stored a
document has nothing to serve. The repositories are stubbed: none of this
needs a database.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException
from mftik_api.routes import sts as sts_routes


class _FakeDb:
    pass


def _session_scope_stub():
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def scope() -> Any:
        yield _FakeDb()

    return scope


def _install(monkeypatch: pytest.MonkeyPatch, *, session: Any) -> None:
    """Point the handler at a canned row instead of Postgres."""

    class FakeSessionRepo:
        def __init__(self, _db: Any) -> None:
            pass

        async def get_by_session_id(self, session_id: str) -> Any:
            if session is None or session.session_id != session_id:
                return None
            return session

    monkeypatch.setattr(sts_routes, "session_scope", _session_scope_stub())
    monkeypatch.setattr(sts_routes, "StsSessionRepository", FakeSessionRepo)


def _row(
    *,
    session_id: str = "sess-abc",
    type: str | None = "NoopStrategy",
    yaml_text: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        session_id=session_id,
        type=type,
        yaml_text=yaml_text,
    )


SUBMITTED = """\
# how the desk runs this one
td:
  paper trader:      # the only live credential
md: [orderbook.Paper_Spot_BTCUSDT]

sts:
  gap_bps: 10
"""


async def test_submitted_document_comes_back_verbatim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, session=_row(yaml_text=SUBMITTED))

    result = await sts_routes.strategy_yaml("sess-abc")

    assert result.yaml == SUBMITTED
    assert result.type == "NoopStrategy"
    assert result.session_id == "sess-abc"
    assert not hasattr(result, "reconstructed")
    assert not hasattr(result, "unresolved_td")


async def test_a_session_without_yaml_text_is_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, session=_row(yaml_text=None))

    with pytest.raises(HTTPException) as exc:
        await sts_routes.strategy_yaml("sess-abc")

    assert exc.value.status_code == 404
    assert "predates document storage" in exc.value.detail


async def test_unknown_session_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, session=None)

    with pytest.raises(HTTPException) as exc:
        await sts_routes.strategy_yaml("missing")

    assert exc.value.status_code == 404
    assert "session not found" in exc.value.detail
