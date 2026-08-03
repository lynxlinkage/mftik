"""Abstract exchange connectivity — public/private, stream + req-reply."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from decimal import Decimal
from typing import Any

from mft.exchange.errors import ExchangeNotConnectedError
from mft.exchange.models import (
    Balance,
    BestQuote,
    Fill,
    Instrument,
    Kline,
    Order,
    OrderBook,
    OrderType,
    PlaceOrderRequest,
    Side,
    Ticker,
    Trade,
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


class PublicClient(BaseClient):
    """Public market-data API: request-reply + push streams."""

    # --- request-reply -----------------------------------------------------

    @abstractmethod
    async def fetch_instruments(self) -> list[Instrument]:
        """List tradeable instruments."""

    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Ticker:
        """Snapshot ticker for ``symbol``."""

    @abstractmethod
    async def fetch_order_book(self, symbol: str, *, depth: int = 10) -> OrderBook:
        """Snapshot order book for ``symbol``."""

    # --- streams -----------------------------------------------------------

    @abstractmethod
    def stream_ticker(self, symbol: str) -> AsyncIterator[Ticker]:
        """Push ticker updates for ``symbol``."""

    @abstractmethod
    def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        """Push public trades for ``symbol``."""

    @abstractmethod
    def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        """Push full order-book snapshots for ``symbol``.

        Every item is a self-contained book. Venues that only push depth diffs
        must fold them onto a snapshot inside the adapter — a consumer of this
        stream never has to sequence anything.
        """

    # Optional streams: not every venue publishes these, and the ones that do
    # do not all agree on the shape, so they default to unsupported rather
    # than forcing every adapter to carry a stub.

    def stream_kline(self, symbol: str, interval: str) -> AsyncIterator[Kline]:
        """Push candles for ``symbol`` at ``interval`` (venue interval spelling)."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support stream_kline"
        )

    def stream_best_quote(self, symbol: str) -> AsyncIterator[BestQuote]:
        """Push top-of-book quote changes for ``symbol``."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support stream_best_quote"
        )


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
