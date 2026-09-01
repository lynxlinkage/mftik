"""The three numbered lists refuse an offset past the far side of a browse.

Over HTTP rather than by calling the handlers, because the cap is a
validator on the query parameter: a handler called directly is handed
whatever offset the caller passes, so it would prove nothing.

The database is here only for the boundary case. A refused request never
reaches one; an accepted request has to, and an empty list is enough to
tell "the validator let this through" from "the validator stopped it".
"""

from __future__ import annotations

import httpx
import pytest
from db_harness import a_database
from fastapi import FastAPI
from mftik_api.deps import MAX_LIST_OFFSET
from mftik_api.routes import audits as audits_routes
from mftik_api.routes import board as board_routes
from mftik_api.routes import sts as sts_routes

#: Every list that pages by number.
PATHS = ("/audits", "/sts/strategies", "/board/sessions")


@pytest.fixture
async def client(monkeypatch, database_url):
    async with a_database(database_url) as database:
        for module in (audits_routes, board_routes, sts_routes):
            monkeypatch.setattr(module, "session_scope", database.scope)
        app = FastAPI()
        for module in (audits_routes, board_routes, sts_routes):
            app.include_router(module.router)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://mftik.test"
        ) as http:
            yield http


@pytest.mark.parametrize("path", PATHS)
async def test_an_offset_past_the_cap_is_refused(client, path: str) -> None:
    """A 422, not a scan: ``offset`` skips index entries one by one."""
    res = await client.get(path, params={"offset": MAX_LIST_OFFSET + 1})
    assert res.status_code == 422
    detail = res.json()["detail"]
    assert any(err["loc"] == ["query", "offset"] for err in detail), detail


@pytest.mark.parametrize("path", PATHS)
async def test_the_cap_itself_is_not_past_it(client, path: str) -> None:
    """Inclusive. The boundary is a page, not the first refusal."""
    res = await client.get(path, params={"offset": MAX_LIST_OFFSET})
    assert res.status_code == 200
