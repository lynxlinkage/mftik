"""Paper exchange private client — trading req-reply + account streams."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from mft.exchange.base import PrivateClient
from mft.exchange.errors import ExchangeError
from mft.exchange.models import Balance, Fill, Order, PlaceOrderRequest
from mft.exchange.stream import EventStream

if TYPE_CHECKING:
    from mft.exchange.paper.engine import PaperExchange


class PaperPrivateClient(PrivateClient):
    """Fake private venue client authenticated by api_key / api_secret.

    Each distinct ``api_key`` maps to an isolated paper account on the shared
    :class:`PaperExchange` engine.
    """

    name = "paper"

    def __init__(
        self,
        exchange: PaperExchange,
        *,
        api_key: str,
        api_secret: str,
        passphrase: str | None = None,
    ) -> None:
        super().__init__()
        if not api_key:
            raise ValueError("api_key is required")
        if not api_secret:
            raise ValueError("api_secret is required")
        self._exchange = exchange
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self._streams: list[EventStream[Any]] = []

    @property
    def account(self) -> str:
        """Account id on the paper engine (== api_key)."""
        return self.api_key

    async def connect(self) -> None:
        await self._exchange.start()
        self._exchange.authenticate(
            self.api_key,
            self.api_secret,
            passphrase=self.passphrase,
        )
        self._connected = True

    async def close(self) -> None:
        for stream in self._streams:
            await stream.aclose()
        self._streams.clear()
        self._connected = False

    # --- request-reply -----------------------------------------------------

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        self._ensure_connected()
        return await self._exchange.place_order(self.api_key, request)

    async def cancel_order(self, order_id: str) -> Order:
        self._ensure_connected()
        return await self._exchange.cancel_order(self.api_key, order_id)

    async def fetch_order(self, order_id: str) -> Order:
        self._ensure_connected()
        return self._exchange.get_order(order_id)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        self._ensure_connected()
        return self._exchange.list_open_orders(self.api_key, symbol)

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        return self._exchange.list_balances(self.api_key)

    # --- streams -----------------------------------------------------------

    def stream_orders(self) -> AsyncIterator[Order]:
        self._ensure_connected()
        stream = self._exchange.subscribe_orders(self.api_key)
        self._streams.append(stream)
        return stream

    def stream_fills(self) -> AsyncIterator[Fill]:
        self._ensure_connected()
        stream = self._exchange.subscribe_fills(self.api_key)
        self._streams.append(stream)
        return stream

    def stream_balances(self) -> AsyncIterator[Balance]:
        self._ensure_connected()
        stream = self._exchange.subscribe_balances(self.api_key)
        self._streams.append(stream)
        return stream


class PaperAuthError(ExchangeError):
    """Raised when paper api_key / api_secret do not match."""
