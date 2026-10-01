"""``session_id`` is six hex digits, and a taken one is minted again."""

from __future__ import annotations

from contextlib import asynccontextmanager

from mftik_api import orchestrate


async def test_mint_session_id_retries_when_the_row_exists(monkeypatch) -> None:
    seen: list[str] = []

    class FakeRepo:
        def __init__(self, _db: object) -> None:
            pass

        async def get_by_session_id(self, session_id: str) -> object | None:
            seen.append(session_id)
            if len(seen) == 1:
                return object()
            return None

    ids = iter(["aaaaaa", "bbbbbb"])
    monkeypatch.setattr(orchestrate.secrets, "token_hex", lambda _n: next(ids))
    monkeypatch.setattr(orchestrate, "StsSessionRepository", FakeRepo)

    @asynccontextmanager
    async def scope():
        yield object()

    monkeypatch.setattr(orchestrate, "session_scope", scope)
    assert await orchestrate.mint_session_id() == "bbbbbb"
    assert seen == ["aaaaaa", "bbbbbb"]
