"""Where the account worker finds a venue's warm-pool request (F35).

The interval, the expiry and the public read are written on the
adapter. This module only points a venue name at them, so the resident
layer does not keep a second copy of the numbers.

Paper is absent: it has no HTTP pool.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx

from mftik.exchange.binance.delivery.protocol import BINANCE_DELIVERY_REST_URL
from mftik.exchange.binance.delivery.rest import keepalive as binance_cm
from mftik.exchange.binance.future.protocol import BINANCE_FUTURE_REST_URL
from mftik.exchange.binance.future.rest import keepalive as binance_um
from mftik.exchange.binance.spot.protocol import BINANCE_SPOT_REST_URL
from mftik.exchange.binance.spot.rest import keepalive as binance_spot
from mftik.exchange.bitget.protocol import BITGET_REST_URL
from mftik.exchange.bitget.rest import keepalive as bitget
from mftik.exchange.bybit.protocol import BYBIT_REST_URL
from mftik.exchange.bybit.rest import keepalive as bybit
from mftik.exchange.deribit.protocol import DERIBIT_REST_URL
from mftik.exchange.deribit.rest import keepalive as deribit
from mftik.exchange.gate.future.protocol import GATE_FUTURES_REST_URL
from mftik.exchange.gate.future.rest import keepalive as gate_futures
from mftik.exchange.gate.spot.rest import GATE_SPOT_REST_URL
from mftik.exchange.gate.spot.rest import keepalive as gate_spot
from mftik.exchange.okx.protocol import OKX_REST_URL
from mftik.exchange.okx.rest import keepalive as okx
from mftik.exchange.venues import UnknownVenueError, require

Send = Callable[[httpx.AsyncClient], Awaitable[None]]


@dataclass(frozen=True)
class RestKeepalive:
    """One venue's warm-pool request and the limits the pool is built with.

    ``hosts`` is one base URL per REST host that venue's account uses.
    Binance spot, USD-M and COIN-M are three venues, so three pools, each
    with its own host. ``path`` is the public read as it appears on the
    wire (``request.url.path``). ``send`` performs that read on a client
    the caller already holds.
    """

    venue: str
    interval_s: float
    expiry_s: float
    limits: httpx.Limits
    hosts: tuple[str, ...]
    path: str
    send: Send


def _spec(
    venue: str,
    *,
    interval_s: float,
    expiry_s: float,
    limits: httpx.Limits,
    hosts: tuple[str, ...],
    path: str,
    send: Send,
) -> RestKeepalive:
    return RestKeepalive(
        venue=venue,
        interval_s=interval_s,
        expiry_s=expiry_s,
        limits=limits,
        hosts=tuple(host.rstrip("/") for host in hosts),
        path=path,
        send=send,
    )


def _load() -> dict[str, RestKeepalive]:
    from mftik.exchange.binance.delivery import rest as cm
    from mftik.exchange.binance.future import rest as um
    from mftik.exchange.binance.spot import rest as spot
    from mftik.exchange.bitget import rest as bitget_rest
    from mftik.exchange.bybit import rest as bybit_rest
    from mftik.exchange.deribit import rest as deribit_rest
    from mftik.exchange.gate.future import rest as futures_rest
    from mftik.exchange.gate.spot import rest as gate_rest
    from mftik.exchange.okx import rest as okx_rest

    rows = (
        _spec(
            "Binance",
            interval_s=spot.KEEPALIVE_INTERVAL_S,
            expiry_s=spot.KEEPALIVE_EXPIRY_S,
            limits=spot.POOL_LIMITS,
            hosts=(BINANCE_SPOT_REST_URL,),
            path=spot.KEEPALIVE_PATH,
            send=binance_spot,
        ),
        _spec(
            "BinanceUM",
            interval_s=um.KEEPALIVE_INTERVAL_S,
            expiry_s=um.KEEPALIVE_EXPIRY_S,
            limits=um.POOL_LIMITS,
            hosts=(BINANCE_FUTURE_REST_URL,),
            path=um.KEEPALIVE_PATH,
            send=binance_um,
        ),
        _spec(
            "BinanceCM",
            interval_s=cm.KEEPALIVE_INTERVAL_S,
            expiry_s=cm.KEEPALIVE_EXPIRY_S,
            limits=cm.POOL_LIMITS,
            hosts=(BINANCE_DELIVERY_REST_URL,),
            path=cm.KEEPALIVE_PATH,
            send=binance_cm,
        ),
        _spec(
            "Bybit",
            interval_s=bybit_rest.KEEPALIVE_INTERVAL_S,
            expiry_s=bybit_rest.KEEPALIVE_EXPIRY_S,
            limits=bybit_rest.POOL_LIMITS,
            hosts=(BYBIT_REST_URL,),
            path=bybit_rest.KEEPALIVE_PATH,
            send=bybit,
        ),
        _spec(
            "Okx",
            interval_s=okx_rest.KEEPALIVE_INTERVAL_S,
            expiry_s=okx_rest.KEEPALIVE_EXPIRY_S,
            limits=okx_rest.POOL_LIMITS,
            hosts=(OKX_REST_URL,),
            path=okx_rest.KEEPALIVE_PATH,
            send=okx,
        ),
        _spec(
            "Bitget",
            interval_s=bitget_rest.KEEPALIVE_INTERVAL_S,
            expiry_s=bitget_rest.KEEPALIVE_EXPIRY_S,
            limits=bitget_rest.POOL_LIMITS,
            hosts=(BITGET_REST_URL,),
            path=bitget_rest.KEEPALIVE_PATH,
            send=bitget,
        ),
        _spec(
            "Deribit",
            interval_s=deribit_rest.KEEPALIVE_INTERVAL_S,
            expiry_s=deribit_rest.KEEPALIVE_EXPIRY_S,
            limits=deribit_rest.POOL_LIMITS,
            hosts=(DERIBIT_REST_URL,),
            path=deribit_rest.KEEPALIVE_PATH,
            send=deribit,
        ),
        _spec(
            "Gate",
            interval_s=gate_rest.KEEPALIVE_INTERVAL_S,
            expiry_s=gate_rest.KEEPALIVE_EXPIRY_S,
            limits=gate_rest.POOL_LIMITS,
            hosts=(GATE_SPOT_REST_URL,),
            path=gate_rest.KEEPALIVE_PATH,
            send=gate_spot,
        ),
        _spec(
            "GateFutures",
            interval_s=futures_rest.KEEPALIVE_INTERVAL_S,
            expiry_s=futures_rest.KEEPALIVE_EXPIRY_S,
            limits=futures_rest.POOL_LIMITS,
            hosts=(GATE_FUTURES_REST_URL,),
            path=futures_rest.KEEPALIVE_PATH,
            send=gate_futures,
        ),
    )
    return {row.venue: row for row in rows}


_BY_VENUE = _load()


def venues() -> tuple[str, ...]:
    """Venue names that have a REST keepalive, sorted."""
    return tuple(sorted(_BY_VENUE))


def for_venue(name: str) -> RestKeepalive:
    """The adapter's keepalive for ``name``.

    Paper raises :class:`UnknownVenueError`: it has no HTTP pool.
    """
    venue = require(name)
    spec = _BY_VENUE.get(venue.name)
    if spec is None:
        raise UnknownVenueError(f"{venue.name} has no REST keepalive")
    return spec


__all__ = [
    "RestKeepalive",
    "for_venue",
    "venues",
]
