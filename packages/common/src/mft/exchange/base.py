"""Abstract exchange connectivity — public/private, stream + req-reply."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from decimal import Decimal

from mft.exchange.errors import ExchangeNotConnectedError
from mft.exchange.models import (
    Balance,
    Fill,
    Instrument,
    Order,
    OrderBook,
    OrderType,
    PlaceOrderRequest,
    Side,
    Ticker,
    Trade,
)


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
        """Push order-book snapshots/diffs for ``symbol``."""


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
        """Cancel an open order."""

    @abstractmethod
    async def fetch_order(self, order_id: str) -> Order:
        """Fetch a single order by id."""

    @abstractmethod
    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        """List open orders, optionally filtered by symbol."""

    @abstractmethod
    async def fetch_balances(self) -> list[Balance]:
        """Snapshot account balances."""

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
