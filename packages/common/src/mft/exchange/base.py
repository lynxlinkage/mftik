"""Shared lifecycle, and the private trading API every venue does implement.

There is deliberately no ``PublicClient`` here, and adding one back would be a
mistake worth arguing about first. Market data is where venues differ most:
which reads come over a socket and which need REST, which feeds exist at all,
whether history can be asked for. A single public interface made those
differences invisible at the type level and then leaked them anyway, as
optional methods that raise — and it forced a venue's REST reads and its socket
to be connected together, so a caller wanting one paid for both.

So ``mft.exchange.<venue>`` offers a *connector per venue* whose methods happen
to resemble each other, not an implementation of a shared contract. What a
consumer needs is the consumer's business to state: MD declares the shape it
drives its feeds through, and composes each venue behind it.

:class:`PrivateClient` stays because it is not the same animal. It carries real
shared behaviour — ``place_market_order``, ``cancel_by_client_order_id`` and
friends are written once here in terms of the abstract calls — so it is a mixin
that happens to define an interface, and removing it would mean copying that
behaviour into every venue.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from decimal import Decimal
from typing import Any

from mft.exchange.errors import ExchangeNotConnectedError
from mft.exchange.models import (
    Balance,
    Fill,
    Order,
    OrderType,
    PlaceOrderRequest,
    Side,
)
from mft.exchange.oms import Position


class BaseClient(ABC):
    """Shared lifecycle for public and private venue clients."""

    name: str = "base"

    def __init__(self) -> None:
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    def _ensure_connected(self) -> None:
        if not self._connected:
            raise ExchangeNotConnectedError(
                f"{self.name} client is not connected; call connect() first"
            )

    @abstractmethod
    async def connect(self) -> None:
        """Establish connectivity to the venue."""

    @abstractmethod
    async def close(self) -> None:
        """Tear down connectivity and open streams."""

    async def __aenter__(self) -> BaseClient:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()


class PrivateClient(BaseClient):
    """Private trading API: request-reply + account push streams."""

    # --- request-reply -----------------------------------------------------

    @abstractmethod
    async def place_order(self, request: PlaceOrderRequest) -> Order:
        """Submit a new order."""

    async def place_market_order(
        self,
        *,
        symbol: str,
        side: Side,
        qty: Decimal,
        client_order_id: str | None = None,
    ) -> Order:
        return await self.place_order(
            PlaceOrderRequest(
                symbol=symbol,
                side=side,
                type=OrderType.MARKET,
                qty=qty,
                client_order_id=client_order_id,
            )
        )

    async def place_limit_order(
        self,
        *,
        symbol: str,
        side: Side,
        qty: Decimal,
        price: Decimal,
        client_order_id: str | None = None,
    ) -> Order:
        return await self.place_order(
            PlaceOrderRequest(
                symbol=symbol,
                side=side,
                type=OrderType.LIMIT,
                qty=qty,
                price=price,
                client_order_id=client_order_id,
            )
        )

    @abstractmethod
    async def cancel_order(self, order_id: str) -> Order:
        """Cancel an open order by exchange ``order_id``."""

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        """Cancel an open order by ``client_order_id``."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support cancel_by_client_order_id"
        )

    @abstractmethod
    async def fetch_order(self, order_id: str) -> Order:
        """Fetch a single order by id."""

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, symbol: str | None = None
    ) -> Order | None:
        """Resolve an order we sent but never heard back about.

        This is the way out of ``UNKNOWN``: the order stream said nothing, so
        ask the venue directly. ``symbol`` is a hint for venues that cannot
        look an order up without its instrument.

        Returns None when the venue has no such order — which is itself an
        answer, and means the submit never landed. Adapters that cannot query
        by client id raise :class:`NotImplementedError`, leaving the order
        ``UNKNOWN`` until recon sweeps it.
        """
        raise NotImplementedError(
            f"{type(self).__name__} cannot fetch by client_order_id"
        )

    def on_reconnect(self, callback: Callable[[], Any]) -> None:
        """Register a callback for when the venue connection comes back.

        A silent order stream is indistinguishable from a dead socket, so the
        reconnect is TD's cue to re-run recon and rebuild state from the venue
        rather than trusting what it had. Adapters without reconnection ignore
        this.
        """
        return None

    @abstractmethod
    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        """List open orders, optionally filtered by symbol."""

    @abstractmethod
    async def fetch_balances(self) -> list[Balance]:
        """Snapshot account balances."""

    async def fetch_positions(self) -> list[Position]:
        """Snapshot positions when the venue supports it; default empty."""
        return []

    # --- streams -----------------------------------------------------------

    @abstractmethod
    def stream_orders(self) -> AsyncIterator[Order]:
        """Push order lifecycle updates."""

    @abstractmethod
    def stream_fills(self) -> AsyncIterator[Fill]:
        """Push fill / execution reports."""

    @abstractmethod
    def stream_balances(self) -> AsyncIterator[Balance]:
        """Push balance updates after fills / locks."""
