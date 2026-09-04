"""POST /apis — same key on two venues is allowed; same venue is not."""

from __future__ import annotations

import pytest
from auth_harness import a_client
from db_harness import a_database, an_owner
from fastapi import FastAPI
from mftik_api.auth import AuthMiddleware
from mftik_api.routes import apis as apis_routes
from mftik_api.routes.apis import router as apis_router
from mftik_db.models.api import Api


@pytest.fixture
async def db(monkeypatch, database_url):
    async def _no_audit(**_kwargs: object) -> None:
        return None

    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            await session.commit()
        monkeypatch.setattr(apis_routes, "session_scope", database.scope)
        monkeypatch.setattr(apis_routes, "record_audit", _no_audit)
        monkeypatch.setenv("MFTIK_AUTH_ENABLED", "0")
        yield database.scope


def _app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(apis_router)
    return app


def _body(*, name: str, venue: str) -> dict[str, str]:
    return {
        "name": name,
        "venue": venue,
        "api_key": "shared-ed25519-key",
        "api_secret": "shared-ed25519-secret",
        "type": "ED25519",
    }


async def test_same_binance_key_can_register_um_and_cm(db) -> None:
    async with a_client(_app()) as client:
        um = await client.post(
            "/apis", json=_body(name="binance um", venue="BinanceUM")
        )
        cm = await client.post(
            "/apis", json=_body(name="binance cm", venue="BinanceCM")
        )

    assert um.status_code == 201, um.text
    assert cm.status_code == 201, cm.text
    assert um.json()["id"] != cm.json()["id"]
    assert um.json()["venue"] == "BinanceUM"
    assert cm.json()["venue"] == "BinanceCM"
    assert um.json()["api_key"] == cm.json()["api_key"]


async def test_same_key_on_the_same_venue_is_409(db) -> None:
    async with a_client(_app()) as client:
        first = await client.post(
            "/apis", json=_body(name="binance um", venue="BinanceUM")
        )
        again = await client.post(
            "/apis", json=_body(name="binance um 2", venue="BinanceUM")
        )

    assert first.status_code == 201, first.text
    assert again.status_code == 409
    assert "BinanceUM" in again.json()["detail"]


async def test_key_on_a_legacy_venue_spelling_is_409(db) -> None:
    """A pre-0028 row may spell the venue differently; it still collides."""
    async with db() as session:
        session.add(
            Api(
                owner_id=1,
                venue="binanceum",
                api_key="shared-ed25519-key",
                api_secret="shared-ed25519-secret",
                type="ED25519",
            )
        )

    async with a_client(_app()) as client:
        again = await client.post(
            "/apis", json=_body(name="binance um", venue="BinanceUM")
        )

    assert again.status_code == 409
    assert "BinanceUM" in again.json()["detail"]
