"""Every list that pages refuses an offset past the far side of a browse.

Over HTTP rather than by calling the handlers, because the cap is a
validator on the query parameter: a handler called directly is handed
whatever offset the caller passes, so it would prove nothing.

What is behind each list is stubbed to the emptiest thing that answers —
a database with no rows, a broker that returns no symbols. A refused
request never reaches either; an accepted one has to, and an empty list
is enough to tell "the validator let this through" from "the validator
stopped it".
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from db_harness import a_database
from fastapi import FastAPI
from mftik.protocol import (
    SYM_LIST,
    Envelope,
    SymListResult,
    SymListResultEnvelope,
    UntypedEnvelope,
)
from mftik_api.deps import MAX_LIST_OFFSET
from mftik_api.routes import audits as audits_routes
from mftik_api.routes import board as board_routes
from mftik_api.routes import sts as sts_routes
from mftik_api.routes import sym as sym_routes

#: Every list that pages by number or by browse.
PATHS = ("/audits", "/sts/strategies", "/board/sessions", "/sym/symbols")

#: The routes whose list handler reads the database directly.
_DB_MODULES = (audits_routes, board_routes, sts_routes)


class _EmptySymBroker:
    """Answers ``sym.list`` with no symbols, and nothing else."""

    async def request(
        self, subject: str, envelope: Envelope[Any], *, timeout: float | None = None
    ) -> UntypedEnvelope:
        if envelope.type != SYM_LIST:
            raise AssertionError(f"unexpected broker request: {envelope.type}")
        reply = SymListResultEnvelope.wrap(
            SymListResult(), type=SYM_LIST, source="sym"
        )
        return UntypedEnvelope.from_json(reply.to_json())


@pytest.fixture
async def client(monkeypatch, database_url):
    async with a_database(database_url) as database:
        for module in _DB_MODULES:
            monkeypatch.setattr(module, "session_scope", database.scope)
        app = FastAPI()
        for module in (*_DB_MODULES, sym_routes):
            app.include_router(module.router)
        app.state.broker = _EmptySymBroker()
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
