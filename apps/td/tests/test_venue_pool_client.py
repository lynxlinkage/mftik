"""Trading REST borrows the resident pool and does not close it (R4).

Component: building the real connector is past the unit call budget.
No broker and no venue host.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from mftik.exchange.binance.delivery.protocol import BINANCE_DELIVERY_REST_URL
from mftik.exchange.binance.future.protocol import BINANCE_FUTURE_REST_URL
from mftik.exchange.bitget.protocol import BITGET_REST_URL
from mftik.exchange.bybit.protocol import BYBIT_REST_URL
from mftik.exchange.gate.future.protocol import GATE_FUTURES_REST_URL
from mftik.exchange.gate.spot.rest import GATE_SPOT_REST_URL
from mftik.exchange.okx.protocol import OKX_REST_URL
from mftik_td.session.factory import VenueSessionFactory

pytestmark = pytest.mark.component

ED25519_PEM = (
    Ed25519PrivateKey.generate()
    .private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    .decode("ascii")
)


@dataclass
class _Row:
    venue: str
    api_key: str
    api_secret: str
    passphrase: str | None = None


class _Symbols:
    """Construction does not resolve a ticker."""


def _factory(row: _Row) -> VenueSessionFactory:
    async def load_api(api_id: int) -> _Row | None:
        return row if api_id == 1 else None

    return VenueSessionFactory(
        object(),  # type: ignore[arg-type]
        load_api=load_api,
        symbols=_Symbols(),  # type: ignore[arg-type]
    )


def _row(venue: str) -> _Row:
    secret = ED25519_PEM if venue.startswith("Binance") else "secret"
    passphrase = "phrase" if venue in {"Okx", "Bitget"} else None
    return _Row(
        venue=venue,
        api_key="test-key",
        api_secret=secret,
        passphrase=passphrase,
    )


@pytest.mark.parametrize(
    ("venue", "url"),
    [
        ("Bybit", BYBIT_REST_URL),
        ("Okx", OKX_REST_URL),
        ("Bitget", BITGET_REST_URL),
        ("Gate", GATE_SPOT_REST_URL),
        ("GateFutures", GATE_FUTURES_REST_URL),
        ("BinanceUM", BINANCE_FUTURE_REST_URL),
        ("BinanceCM", BINANCE_DELIVERY_REST_URL),
    ],
)
async def test_the_trading_rest_client_is_the_pooled_one(venue: str, url: str) -> None:
    client = httpx.AsyncClient()
    seen: list[str] = []

    def client_for(base_url: str) -> httpx.AsyncClient:
        seen.append(base_url)
        return client

    try:
        session = await _factory(_row(venue)).create(1, client_for=client_for)
        rest = session.private.rest
        assert seen == [url]
        assert rest._client is client  # noqa: SLF001 — the ownership flag is the contract
        assert rest._owns_client is False  # noqa: SLF001
        await rest.close()
        assert client.is_closed is False
    finally:
        await client.aclose()


@pytest.mark.parametrize("venue", ["Binance", "Deribit"])
async def test_a_venue_without_trading_rest_does_not_borrow_the_pool(
    venue: str,
) -> None:
    client = httpx.AsyncClient()
    seen: list[str] = []

    def client_for(base_url: str) -> httpx.AsyncClient:
        seen.append(base_url)
        return client

    try:
        session = await _factory(_row(venue)).create(1, client_for=client_for)
        assert seen == []
        assert getattr(session.private, "rest", None) is None
    finally:
        await client.aclose()


async def test_omitting_the_pool_still_lets_the_connector_own_its_client() -> None:
    session = await _factory(_row("Bybit")).create(1)
    rest = session.private.rest
    assert rest._client is None  # noqa: SLF001
    assert rest._owns_client is True  # noqa: SLF001
