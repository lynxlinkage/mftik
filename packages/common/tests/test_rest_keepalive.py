"""Each venue's warm-pool request is a public read, and the expiry outlasts it.

The numbers are the Appendix D defaults. What this locks is the shape
B6-01 asked for: the interval is the adapter's, it is shorter than
that adapter's ``keepalive_expiry``, and the request is a public GET.
Paper has no HTTP pool.
"""

from __future__ import annotations

import httpx
import pytest
from mftik.exchange.binance.delivery.rest import BinanceDeliveryPublicRest
from mftik.exchange.binance.future.rest import BinanceFuturePublicRest
from mftik.exchange.binance.spot.rest import BinanceSpotPublicRest
from mftik.exchange.bitget.rest import BitgetPublicRest
from mftik.exchange.bybit.rest import BybitPublicRest
from mftik.exchange.deribit.rest import DeribitPublicRest
from mftik.exchange.gate.future.rest import GateFuturesPublicRest
from mftik.exchange.gate.spot.rest import GateSpotPublicRest
from mftik.exchange.keepalive import for_venue, venues
from mftik.exchange.okx.rest import OkxPublicRest
from mftik.exchange.venues import UnknownVenueError, names

_MS = 1_700_000_000_000

#: A 200 the venue's own parser accepts. The assertion is the request,
#: not the clock value.
_BODY: dict[str, object] = {
    "Binance": {"serverTime": _MS},
    "BinanceUM": {"serverTime": _MS},
    "BinanceCM": {"serverTime": _MS},
    "Bybit": {
        "retCode": 0,
        "retMsg": "OK",
        "result": {"timeSecond": "1700000000", "timeNano": "1700000000000000000"},
        "time": _MS,
    },
    "Okx": {"code": "0", "msg": "", "data": [{"ts": str(_MS)}]},
    "Bitget": {"code": "00000", "msg": "success", "data": {"serverTime": str(_MS)}},
    "Deribit": {"jsonrpc": "2.0", "result": _MS},
    "Gate": {"server_time": _MS},
    "GateFutures": {"server_time": _MS},
}

#: Headers a signed call would send. A public read sends none of them.
_SIGNED = frozenset(
    {
        "x-mbx-apikey",
        "x-bapi-api-key",
        "x-bapi-sign",
        "ok-access-key",
        "ok-access-sign",
        "access-key",
        "access-sign",
        "key",
        "sign",
    }
)


@pytest.fixture(scope="module", autouse=True)
def _warm_default_ssl_context() -> None:
    """Load the default CA bundle before any call phase.

    ``connect`` builds a real ``httpx.AsyncClient`` so the test can read
    the pool expiry the adapter passed in. The first client in a process
    pays to load that bundle, which is setup, not the assertion.
    """
    httpx.Client().close()


def test_every_non_paper_venue_has_one_keepalive() -> None:
    assert set(venues()) == set(names()) - {"Paper"}


def test_paper_has_no_rest_keepalive() -> None:
    with pytest.raises(UnknownVenueError):
        for_venue("Paper")


@pytest.mark.parametrize("venue", venues())
def test_the_interval_is_shorter_than_the_expiry(venue: str) -> None:
    spec = for_venue(venue)
    assert spec.interval_s > 0
    assert spec.interval_s < spec.expiry_s
    assert spec.limits.keepalive_expiry == spec.expiry_s
    # httpx's default cap, left in place so one request cannot fill the pool.
    assert spec.limits.max_keepalive_connections is not None
    assert spec.limits.max_keepalive_connections > 1
    assert spec.hosts


@pytest.mark.parametrize("venue", venues())
async def test_the_keepalive_is_a_public_read(venue: str) -> None:
    spec = for_venue(venue)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_BODY[venue])

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(
        base_url=spec.hosts[0], transport=transport
    ) as client:
        await spec.send(client)
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "GET"
    assert request.url.path == spec.path
    path = request.url.path.lower()
    assert "order" not in path
    assert "cancel" not in path
    assert "/private" not in path
    assert _SIGNED.isdisjoint({name.lower() for name in request.headers})


def _public(venue: str) -> object:
    return {
        "Binance": BinanceSpotPublicRest,
        "BinanceUM": BinanceFuturePublicRest,
        "BinanceCM": BinanceDeliveryPublicRest,
        "Bybit": BybitPublicRest,
        "Okx": OkxPublicRest,
        "Bitget": BitgetPublicRest,
        "Deribit": DeribitPublicRest,
        "Gate": GateSpotPublicRest,
        "GateFutures": GateFuturesPublicRest,
    }[venue]()


@pytest.mark.parametrize("venue", venues())
async def test_a_client_the_adapter_opens_uses_its_pool_limits(venue: str) -> None:
    """``connect`` without an injected client keeps the adapter's expiry.

    An injected ``client=`` is left alone: the resident pool is what
    passes that client in, already built with these limits.
    """
    spec = for_venue(venue)
    client = _public(venue)
    await client.connect()
    try:
        opened = client._client
        assert opened is not None
        assert client._owns_client is True
        pool = opened._transport._pool
        assert pool._keepalive_expiry == spec.expiry_s
    finally:
        await client.close()
