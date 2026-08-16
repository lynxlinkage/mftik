"""An API with the gate on it, and a database both halves of the gate share.

The auth tests drive HTTP rather than calling handlers, because what they are
checking mostly happens before a handler: a cookie parsed off the raw scope, a
401 written without an endpoint, a header removed. A composed app is the
smallest thing that has all of that and does not need a broker.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from mft_api.auth import AuthMiddleware, auth_router
from mft_api.auth.deps import OwnerId
from mft_api.deps import DEFAULT_USER_ID

#: https, so the cookie jar sends a ``Secure`` cookie back. Over http it would
#: silently withhold it and every authenticated test would look like a bug in
#: the gate rather than in the harness.
BASE_URL = "https://mft.test"


def an_api() -> FastAPI:
    """The auth routes, the gate, and one route of each kind to aim at."""
    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(auth_router)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/sts/sessions")
    async def gated(owner: OwnerId = DEFAULT_USER_ID) -> dict[str, int]:
        return {"owner": owner}

    return app


@asynccontextmanager
async def a_client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url=BASE_URL) as client:
        yield client


def use_database(monkeypatch, scope) -> None:
    """Point every module that opens a session at the test database.

    Both halves matter: routes write the session row, and the middleware reads
    it back on the next request. Patching one and not the other produces a
    login that succeeds and then does not exist.
    """
    from mft_api.auth import middleware as auth_middleware
    from mft_api.auth import routes as auth_routes

    async def _audit(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(auth_routes, "session_scope", scope)
    monkeypatch.setattr(auth_routes, "record_audit", _audit)
    monkeypatch.setattr(auth_middleware, "session_scope", scope)
