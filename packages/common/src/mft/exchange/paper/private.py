"""Paper exchange private client — trading req-reply + account streams."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from mft.exchange.base import PrivateClient
from mft.exchange.models import Balance, Fill, Order, PlaceOrderRequest
from mft.exchange.stream import EventStream

if TYPE_CHECKING:
    from mft.exchange.paper.engine import PaperExchange


class PaperPrivateClient(PrivateClient):
    """Fake private venue client backed by :class:`PaperExchange`."""

    name = "paper"

    def __init__(self, exchange: PaperExchange, *, account: str = "default") -> None:
        super().__init__()
        self._exchange = exchange
        self.account = account
        self._streams: list[EventStream[Any]] = []

    async def connect(self) -> None:
        await self._exchange.start()
        self._connected = True

    async def close(self) -> None:
        for stream in self._streams:
            await stream.aclose()
        self._streams.clear()
        self._connected = False

    # --- request-reply -----------------------------------------------------

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        self._ensure_connected()
        return await self._exchange.place_order(self.account, request)

    async def cancel_order(self, order_id: str) -> Order:
        self._ensure_connected()
        return await self._exchange.cancel_order(self.account, order_id)

    async def fetch_order(self, order_id: str) -> Order:
        self._ensure_connected()
        return self._exchange.get_order(order_id)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        self._ensure_connected()
        return self._exchange.list_open_orders(self.account, symbol)

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        return self._exchange.list_balances(self.account)

    # --- streams -----------------------------------------------------------

    def stream_orders(self) -> AsyncIterator[Order]:
        self._ensure_connected()
        stream = self._exchange.subscribe_orders(self.account)
        self._streams.append(stream)
        return stream

    def stream_fills(self) -> AsyncIterator[Fill]:
        self._ensure_connected()
        stream = self._exchange.subscribe_fills(self.account)
        self._streams.append(stream)
        return stream

    def stream_balances(self) -> AsyncIterator[Balance]:
        self._ensure_connected()
        stream = self._exchange.subscribe_balances(self.account)
        self._streams.append(stream)
        return stream
