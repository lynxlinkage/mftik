"""PATCH /apis/{api_id} — rename the trading account bound to a credential.

Repositories are stubbed so the handler can be driven with no database.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException
from mft_api.routes import apis as apis_routes
from mft_api.schemas import ApiRenameBody


class _FakeDb:
    async def flush(self) -> None:
        return None


def _session_scope_stub():
    @asynccontextmanager
    async def scope() -> Any:
        yield _FakeDb()

    return scope


def _api(*, api_id: int = 3, owner_id: int = 1) -> SimpleNamespace:
    return SimpleNamespace(
        id=api_id,
        owner_id=owner_id,
        venue="Paper",
        api_key="paper-key",
        type="HMAC",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )


def _install(
    monkeypatch: pytest.MonkeyPatch,
    *,
    api: Any,
    account: Any,
    names: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Point the handler at canned rows; return a sink for audit calls."""
    audits: list[dict[str, Any]] = []
    by_name = dict(names or {})

    class FakeApiRepo:
        def __init__(self, _db: Any) -> None:
            pass

        async def get(self, api_id: int) -> Any:
            if api is None or api.id != api_id:
                return None
            return api

    class FakeAccountRepo:
        def __init__(self, _db: Any) -> None:
            pass

        async def get_by_api_id(self, api_id: int) -> Any:
            if account is None or account.api_id != api_id:
                return None
            return account

        async def get_by_name(self, name: str) -> Any:
            return by_name.get(name)

    async def fake_audit(**kwargs: Any) -> None:
        audits.append(kwargs)

    monkeypatch.setattr(apis_routes, "session_scope", _session_scope_stub())
    monkeypatch.setattr(apis_routes, "ApiRepository", FakeApiRepo)
    monkeypatch.setattr(apis_routes, "AccountRepository", FakeAccountRepo)
    monkeypatch.setattr(apis_routes, "record_audit", fake_audit)
    return audits


async def test_rename_updates_account_name(monkeypatch: pytest.MonkeyPatch) -> None:
    api = _api()
    account = SimpleNamespace(id=9, api_id=api.id, name="old name")
    audits = _install(monkeypatch, api=api, account=account)

    result = await apis_routes.rename_api(api.id, ApiRenameBody(name="  new name  "))

    assert result.name == "new name"
    assert account.name == "new name"
    assert result.account_id == 9
    assert len(audits) == 1
    assert audits[0]["operation"] == "api.rename"
    assert "old name" in audits[0]["result"]
    assert "new name" in audits[0]["result"]


async def test_rename_noop_skips_audit(monkeypatch: pytest.MonkeyPatch) -> None:
    api = _api()
    account = SimpleNamespace(id=9, api_id=api.id, name="same")
    audits = _install(monkeypatch, api=api, account=account)

    result = await apis_routes.rename_api(api.id, ApiRenameBody(name="  same  "))

    assert result.name == "same"
    assert audits == []


async def test_rename_conflict_is_409(monkeypatch: pytest.MonkeyPatch) -> None:
    api = _api()
    account = SimpleNamespace(id=9, api_id=api.id, name="mine")
    _install(
        monkeypatch,
        api=api,
        account=account,
        names={"taken": SimpleNamespace(id=10, api_id=99, name="taken")},
    )

    with pytest.raises(HTTPException) as exc:
        await apis_routes.rename_api(api.id, ApiRenameBody(name="taken"))

    assert exc.value.status_code == 409
    assert account.name == "mine"


async def test_rename_unknown_api_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, api=None, account=None)

    with pytest.raises(HTTPException) as exc:
        await apis_routes.rename_api(404, ApiRenameBody(name="x"))

    assert exc.value.status_code == 404


async def test_blank_name_is_rejected_before_db() -> None:
    with pytest.raises(HTTPException) as exc:
        await apis_routes.rename_api(1, ApiRenameBody(name="   "))

    assert exc.value.status_code == 400
    assert "required" in exc.value.detail
