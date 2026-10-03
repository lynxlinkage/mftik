"""Each real venue's trading layer switches on a fake stream and the warm pool.

No test here opens a venue host. REST is :class:`httpx.MockTransport`.
Private sockets are in-process stand-ins. Deribit is the switch and
recon only: this ticket does not place a Deribit order.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from mftik.clock import FakeClock
from mftik.exchange.binance.delivery.private import BinanceDeliveryPrivateClient
from mftik.exchange.binance.delivery.protocol import BINANCE_DELIVERY_REST_URL
from mftik.exchange.binance.delivery.rest import BinanceDeliveryRest
from mftik.exchange.binance.future.private import BinanceFuturePrivateClient
from mftik.exchange.binance.future.protocol import BINANCE_FUTURE_REST_URL
from mftik.exchange.binance.future.rest import BinanceFutureRest
from mftik.exchange.binance.spot.private import BinanceSpotPrivateClient
from mftik.exchange.bitget.private import BitgetPrivateClient
from mftik.exchange.bitget.protocol import BITGET_REST_URL
from mftik.exchange.bitget.rest import BitgetRest
from mftik.exchange.bybit.private import BybitPrivateClient
from mftik.exchange.bybit.protocol import BYBIT_REST_URL
from mftik.exchange.bybit.rest import BybitRest
from mftik.exchange.deribit.private import DeribitPrivateClient
from mftik.exchange.gate.future.private import GateFuturesPrivateClient
from mftik.exchange.gate.future.protocol import GATE_FUTURES_REST_URL
from mftik.exchange.gate.future.rest import GateFuturesRest
from mftik.exchange.gate.spot.private import GateSpotPrivateClient
from mftik.exchange.gate.spot.rest import GATE_SPOT_REST_URL, GateSpotRest
from mftik.exchange.models import Order, OrderStatus, OrderType, Side
from mftik.exchange.okx.private import OkxPrivateClient
from mftik.exchange.okx.protocol import OKX_REST_URL
from mftik.exchange.okx.rest import OkxRest
from mftik.protocol import OrderSubmit, RejectCode
from mftik_td.account import AccountWorker
from mftik_td.account.session import Session

API = 7
SESSION = "sess"

ED25519_PEM = (
    Ed25519PrivateKey.generate()
    .private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    .decode("ascii")
)

#: Venue, ticker, REST host. ``None`` means the connector has no trading REST.
_VENUES: tuple[tuple[str, str, str | None], ...] = (
    ("Bybit", "Bybit_Spot_BTCUSDT", BYBIT_REST_URL),
    ("Okx", "Okx_Spot_BTCUSDT", OKX_REST_URL),
    ("Bitget", "Bitget_Spot_BTCUSDT", BITGET_REST_URL),
    ("Gate", "Gate_Spot_BTCUSDT", GATE_SPOT_REST_URL),
    ("GateFutures", "GateFutures_Perp_BTCUSDT", GATE_FUTURES_REST_URL),
    ("Binance", "Binance_Spot_BTCUSDT", None),
    ("BinanceUM", "BinanceUM_Perp_BTCUSDT", BINANCE_FUTURE_REST_URL),
    ("BinanceCM", "BinanceCM_Inverse_BTCUSD", BINANCE_DELIVERY_REST_URL),
    ("Deribit", "Deribit_Spot_BTCUSDC", None),
)


class _Quiet:
    async def publish(self, subject: str, envelope: object) -> None:
        return None


class _Symbols:
    """Empty recon does not resolve a ticker."""


class _Balances:
    def to_balances(self) -> list[object]:
        return []


class _QuietStream:
    def __init__(self) -> None:
        self._stop = asyncio.Event()

    def __aiter__(self) -> AsyncIterator[object]:
        return self

    async def __anext__(self) -> object:
        await self._stop.wait()
        raise StopAsyncIteration

    def close(self) -> None:
        self._stop.set()


class _Wire:
    """Stand-in for a private socket. ``connect`` does not open a host."""

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def on_reconnect(self, callback: object) -> None:
        return None

    async def rpc(
        self, method: object = None, params: object = None
    ) -> dict[str, object]:
        return {}

    async def watch_portfolios(self, currencies: object) -> None:
        return None

    async def fetch_account(self) -> _Balances:
        return _Balances()

    async def fetch_open_orders(self, symbol: object = None) -> list[object]:
        return []

    async def fetch_balances(self) -> list[object]:
        return []

    async def fetch_positions(self) -> list[object]:
        return []

    async def list_orders(self, **kwargs: object) -> list[object]:
        return []

    def __getattr__(self, name: str) -> object:
        if name.startswith("subscribe"):

            async def _subscribe(*args: object, **kwargs: object) -> _QuietStream:
                return _QuietStream()

            return _subscribe
        raise AttributeError(name)


class _Probe:
    def __init__(self) -> None:
        self.pings = 0

    async def __call__(self) -> None:
        self.pings += 1


def _http(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    host = request.url.host
    if "settings" in path:
        return httpx.Response(
            200,
            json={
                "code": "00000",
                "msg": "success",
                "data": {"accountMode": "unified", "holdMode": "one_way_mode"},
            },
        )
    if path.endswith("/time") or path.endswith("get_time"):
        if "okx.com" in host:
            return httpx.Response(
                200, json={"code": "0", "data": [{"ts": "1700000000000"}]}
            )
        if "bitget.com" in host:
            return httpx.Response(
                200,
                json={"code": "00000", "data": {"serverTime": "1700000000000"}},
            )
        if "gateio.ws" in host:
            return httpx.Response(200, json={"server_time": 1_700_000_000_000})
        if "deribit.com" in host:
            return httpx.Response(200, json={"result": 1_700_000_000_000})
        if "binance.com" in host:
            return httpx.Response(200, json={"serverTime": 1_700_000_000_000})
        return httpx.Response(
            200,
            json={
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "timeSecond": "1700000000",
                    "timeNano": "1700000000000000000",
                },
                "time": 1_700_000_000_000,
            },
        )
    if "okx.com" in host:
        return httpx.Response(200, json={"code": "0", "data": []})
    if "bitget.com" in host:
        return httpx.Response(200, json={"code": "00000", "data": []})
    if "binance.com" in host or "gateio.ws" in host:
        return httpx.Response(200, json=[])
    if "deribit.com" in host:
        return httpx.Response(200, json={"result": []})
    return httpx.Response(
        200,
        json={"retCode": 0, "retMsg": "OK", "result": {"list": []}, "time": 1},
    )


def _secret(venue: str) -> str:
    if venue.startswith("Binance"):
        return ED25519_PEM
    return "test-secret"


def _passphrase(venue: str) -> str:
    return "phrase"


def _private(venue: str, client_for):
    key = "test-key"
    secret = _secret(venue)
    symbols = _Symbols()
    if venue == "Bybit":
        return BybitPrivateClient(
            api_key=key,
            api_secret=secret,
            symbols=symbols,  # type: ignore[arg-type]
            trade=_Wire(),  # type: ignore[arg-type]
            stream=_Wire(),  # type: ignore[arg-type]
            rest=BybitRest(
                api_key=key,
                api_secret=secret,
                client=client_for(BYBIT_REST_URL),
            ),
        )
    if venue == "Okx":
        return OkxPrivateClient(
            api_key=key,
            api_secret=secret,
            passphrase=_passphrase(venue),
            symbols=symbols,  # type: ignore[arg-type]
            stream=_Wire(),  # type: ignore[arg-type]
            rest=OkxRest(
                api_key=key,
                api_secret=secret,
                passphrase=_passphrase(venue),
                client=client_for(OKX_REST_URL),
            ),
        )
    if venue == "Bitget":
        return BitgetPrivateClient(
            api_key=key,
            api_secret=secret,
            passphrase=_passphrase(venue),
            symbols=symbols,  # type: ignore[arg-type]
            stream=_Wire(),  # type: ignore[arg-type]
            rest=BitgetRest(
                api_key=key,
                api_secret=secret,
                passphrase=_passphrase(venue),
                client=client_for(BITGET_REST_URL),
            ),
        )
    if venue == "Gate":
        return GateSpotPrivateClient(
            api_key=key,
            api_secret=secret,
            symbols=symbols,  # type: ignore[arg-type]
            ws=_Wire(),  # type: ignore[arg-type]
            rest=GateSpotRest(
                api_key=key,
                api_secret=secret,
                client=client_for(GATE_SPOT_REST_URL),
            ),
        )
    if venue == "GateFutures":
        return GateFuturesPrivateClient(
            api_key=key,
            api_secret=secret,
            symbols=symbols,  # type: ignore[arg-type]
            ws=_Wire(),  # type: ignore[arg-type]
            rest=GateFuturesRest(
                api_key=key,
                api_secret=secret,
                client=client_for(GATE_FUTURES_REST_URL),
            ),
        )
    if venue == "Binance":
        return BinanceSpotPrivateClient(
            api_key=key,
            api_secret=secret,
            symbols=symbols,  # type: ignore[arg-type]
            api=_Wire(),  # type: ignore[arg-type]
        )
    if venue == "BinanceUM":
        return BinanceFuturePrivateClient(
            api_key=key,
            api_secret=secret,
            symbols=symbols,  # type: ignore[arg-type]
            api=_Wire(),  # type: ignore[arg-type]
            user=_Wire(),  # type: ignore[arg-type]
            rest=BinanceFutureRest(
                api_key=key,
                api_secret=secret,
                client=client_for(BINANCE_FUTURE_REST_URL),
            ),
        )
    if venue == "BinanceCM":
        return BinanceDeliveryPrivateClient(
            api_key=key,
            api_secret=secret,
            symbols=symbols,  # type: ignore[arg-type]
            api=_Wire(),  # type: ignore[arg-type]
            user=_Wire(),  # type: ignore[arg-type]
            rest=BinanceDeliveryRest(
                api_key=key,
                api_secret=secret,
                client=client_for(BINANCE_DELIVERY_REST_URL),
            ),
        )
    if venue == "Deribit":
        return DeribitPrivateClient(
            api_key=key,
            api_secret=secret,
            symbols=symbols,  # type: ignore[arg-type]
            stream=_Wire(),  # type: ignore[arg-type]
        )
    raise AssertionError(venue)


def _submit(ticker: str, client_order_id: str) -> OrderSubmit:
    return OrderSubmit(
        session_id=SESSION,
        api_id=API,
        universal_ticker=ticker,
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("0.01"),
        price=Decimal("1"),
        client_order_id=client_order_id,
    )


def _watch(private: object, recon: list[int], *, orders: bool) -> None:
    fetch = private.fetch_open_orders  # type: ignore[attr-defined]

    async def counting(*args: object, **kwargs: object) -> object:
        recon.append(1)
        return await fetch(*args, **kwargs)

    private.fetch_open_orders = counting  # type: ignore[attr-defined]
    if not orders:
        return

    async def place(request: object) -> Order:
        assert recon, "an order was accepted before recon"
        return Order(
            order_id="venue-1",
            client_order_id=request.client_order_id,  # type: ignore[attr-defined]
            universal_ticker=str(request.universal_ticker),  # type: ignore[attr-defined]
            side=request.side,  # type: ignore[attr-defined]
            type=request.type,  # type: ignore[attr-defined]
            qty=request.qty,  # type: ignore[attr-defined]
            price=request.price,  # type: ignore[attr-defined]
            status=OrderStatus.NEW,
        )

    private.place_order = place  # type: ignore[attr-defined]


@pytest.mark.component
@pytest.mark.real_sleep(
    reason=(
        "Session.start arms a sweep that sleeps; the resident FakeClock "
        "does not drive that loop"
    )
)
@pytest.mark.parametrize(("venue", "ticker", "rest_url"), _VENUES)
async def test_the_trading_layer_toggles_without_touching_the_pool(
    venue: str, ticker: str, rest_url: str | None
) -> None:
    clock = FakeClock()
    probe = _Probe()
    recon: list[int] = []
    orders = venue != "Deribit"
    worker = AccountWorker(API, venue=venue, keepalive=probe, clock=clock)
    await worker.resident.start(transport=httpx.MockTransport(_http))
    pool = worker.resident.pool
    assert pool is not None
    client = pool.clients()[0]

    def client_for(url: str) -> httpx.AsyncClient:
        return pool.client_for(url)

    def build() -> Session:
        private = _private(venue, client_for)
        _watch(private, recon, orders=orders)
        return Session(
            api_id=API,
            broker=_Quiet(),  # type: ignore[arg-type]
            private=private,
        )

    async def factory() -> Session:
        return build()

    try:
        worker.trading.set_factory(factory)
        assert worker.trading.active is False
        refused = await worker.orders.submit(_submit(ticker, "cid-off"))
        assert refused.accepted is False
        assert refused.error_code == RejectCode.TD_VENUE_NOT_CONNECTED
        assert recon == []

        await worker.trading.activate()
        assert worker.trading.active is True
        assert len(recon) >= 1
        if orders:
            taken = await worker.orders.submit(_submit(ticker, "cid-on"))
            assert taken.accepted is True
        if rest_url is not None:
            rest = worker.trading.private.rest  # type: ignore[union-attr]
            assert rest._client is pool.client_for(rest_url)  # noqa: SLF001
            assert rest._owns_client is False  # noqa: SLF001

        base = probe.pings
        await worker.resident.keepalive_once()
        await worker.trading.deactivate()
        assert worker.trading.active is False
        assert worker.resident.started
        assert worker.resident.pool is pool
        assert worker.resident.keepalive is probe
        assert client.is_closed is False
        refused_again = await worker.orders.submit(_submit(ticker, "cid-off-2"))
        assert refused_again.accepted is False
        assert refused_again.error_code == RejectCode.TD_VENUE_NOT_CONNECTED

        await worker.trading.activate()
        assert worker.trading.active is True
        assert len(recon) >= 2
        await worker.resident.keepalive_once()
        assert probe.pings == base + 2
        assert worker.resident.pool is pool
        assert worker.resident.keepalive is probe
        assert client.is_closed is False
        if rest_url is not None:
            rest = worker.trading.private.rest  # type: ignore[union-attr]
            assert rest._client is pool.client_for(rest_url)  # noqa: SLF001
            assert rest._owns_client is False  # noqa: SLF001
            await rest.close()
            assert client.is_closed is False
    finally:
        session = worker.trading.session
        if session is not None and session.started and not session.destroyed:
            await session.destroy()
        if worker.resident.started:
            await worker.resident.close()
