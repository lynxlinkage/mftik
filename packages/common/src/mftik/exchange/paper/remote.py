"""Redis-backed paper private client — talks to the paper-engine service."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from mftik.broker import Broker
from mftik.exchange.base import BaseClient
from mftik.exchange.errors import (
    ExchangeError,
    InsufficientBalanceError,
    OrderError,
)
from mftik.exchange.models import Balance, Fill, Order, PlaceOrderRequest
from mftik.exchange.paper.private import PaperAuthError
from mftik.exchange.tickers import Category
from mftik.protocol import (
    PAPER_AUTH,
    PAPER_BALANCE,
    PAPER_CANCEL_BY_CLIENT_ORDER_ID,
    PAPER_CANCEL_ORDER,
    PAPER_ERROR,
    PAPER_FETCH_BALANCES,
    PAPER_FETCH_OPEN_ORDERS,
    PAPER_FETCH_ORDER,
    PAPER_FILL,
    PAPER_ORDER,
    PAPER_PLACE_ORDER,
    Envelope,
    PaperCancelRequest,
    PaperCredentials,
    PaperFetchBalancesRequest,
    PaperFetchOpenOrdersRequest,
    PaperFetchOrderRequest,
    PaperPlaceOrderRequest,
    Topics,
    UntypedEnvelope,
)


class PaperRemotePrivateClient(BaseClient):
    """The paper trading connector for other processes: engine RPC + streams.

    No ``fetch_order_by_client_order_id``: the engine answers by venue order
    id, and TD leaves an order it cannot resolve ``UNKNOWN`` for recon to
    settle. Absent rather than raising, so the caller can tell before asking.
    """

    name = "Paper"
    category = Category.SPOT

    def __init__(
        self,
        broker: Broker,
        *,
        api_key: str,
        api_secret: str,
        passphrase: str | None = None,
    ) -> None:
        super().__init__()
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret are required")
        self._broker = broker
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self._creds = PaperCredentials(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
        )
        self._stream_stops: list[Any] = []

    async def connect(self) -> None:
        reply = await self._rpc(
            PAPER_AUTH,
            Envelope[PaperCredentials].wrap(
                self._creds, type=PAPER_AUTH, source="td"
            ),
        )
        if reply.type == PAPER_ERROR:
            raise PaperAuthError(str(reply.payload.get("message", "auth failed")))
        self._connected = True

    async def close(self) -> None:
        for stop in self._stream_stops:
            stop.set()
        self._stream_stops.clear()
        self._connected = False

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        self._ensure_connected()
        reply = await self._rpc(
            PAPER_PLACE_ORDER,
            Envelope[PaperPlaceOrderRequest].wrap(
                PaperPlaceOrderRequest(
                    credentials=self._creds,
                    universal_ticker=request.universal_ticker,
                    side=request.side,
                    type=request.type,
                    qty=request.qty,
                    quote_qty=request.quote_qty,
                    price=request.price,
                    tif=request.tif,
                    client_order_id=request.client_order_id,
                ),
                type=PAPER_PLACE_ORDER,
                source="td",
            ),
        )
        self._raise_if_error(reply)
        return Order.model_validate(reply.payload)

    async def cancel_order(self, order_id: str) -> Order:
        self._ensure_connected()
        reply = await self._rpc(
            PAPER_CANCEL_ORDER,
            Envelope[PaperCancelRequest].wrap(
                PaperCancelRequest(credentials=self._creds, order_id=order_id),
                type=PAPER_CANCEL_ORDER,
                source="td",
            ),
        )
        self._raise_if_error(reply)
        return Order.model_validate(reply.payload)

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        self._ensure_connected()
        reply = await self._rpc(
            PAPER_CANCEL_BY_CLIENT_ORDER_ID,
            Envelope[PaperCancelRequest].wrap(
                PaperCancelRequest(
                    credentials=self._creds, client_order_id=client_order_id
                ),
                type=PAPER_CANCEL_BY_CLIENT_ORDER_ID,
                source="td",
            ),
        )
        self._raise_if_error(reply)
        return Order.model_validate(reply.payload)

    async def fetch_order(self, order_id: str) -> Order:
        self._ensure_connected()
        reply = await self._rpc(
            PAPER_FETCH_ORDER,
            Envelope[PaperFetchOrderRequest].wrap(
                PaperFetchOrderRequest(
                    credentials=self._creds, order_id=order_id
                ),
                type=PAPER_FETCH_ORDER,
                source="td",
            ),
        )
        self._raise_if_error(reply)
        return Order.model_validate(reply.payload)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        self._ensure_connected()
        reply = await self._rpc(
            PAPER_FETCH_OPEN_ORDERS,
            Envelope[PaperFetchOpenOrdersRequest].wrap(
                PaperFetchOpenOrdersRequest(
                    credentials=self._creds, symbol=symbol
                ),
                type=PAPER_FETCH_OPEN_ORDERS,
                source="td",
            ),
        )
        self._raise_if_error(reply)
        rows = reply.payload.get("orders", reply.payload)
        if isinstance(rows, dict):
            rows = rows.get("orders", [])
        return [Order.model_validate(r) for r in rows]

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        reply = await self._rpc(
            PAPER_FETCH_BALANCES,
            Envelope[PaperFetchBalancesRequest].wrap(
                PaperFetchBalancesRequest(credentials=self._creds),
                type=PAPER_FETCH_BALANCES,
                source="td",
            ),
        )
        self._raise_if_error(reply)
        rows = reply.payload.get("balances", reply.payload)
        if isinstance(rows, dict):
            rows = rows.get("balances", [])
        return [Balance.model_validate(r) for r in rows]

    def stream_orders(self) -> AsyncIterator[Order]:
        self._ensure_connected()
        return self._stream(Topics.paper_orders(self.api_key), Order)

    def stream_fills(self) -> AsyncIterator[Fill]:
        self._ensure_connected()
        return self._stream(Topics.paper_fills(self.api_key), Fill)

    def stream_balances(self) -> AsyncIterator[Balance]:
        self._ensure_connected()
        return self._stream(Topics.paper_balances(self.api_key), Balance)

    async def _stream(self, topic: str, model: type[Any]) -> AsyncIterator[Any]:
        import asyncio

        stop = asyncio.Event()
        self._stream_stops.append(stop)
        async for env in self._broker.subscribe(topic, stop=stop):
            if env.type in (PAPER_ORDER, PAPER_FILL, PAPER_BALANCE):
                yield model.model_validate(env.payload)

    async def _rpc(self, _type: str, envelope: Envelope[Any]) -> UntypedEnvelope:
        return await self._broker.request(Topics.PAPER, envelope)

    def _raise_if_error(self, reply: UntypedEnvelope) -> None:
        if reply.type != PAPER_ERROR:
            return
        code = str(reply.payload.get("code", "error"))
        message = str(reply.payload.get("message", "paper engine error"))
        if code == "auth":
            raise PaperAuthError(message)
        # The engine distinguishes these, so the client raises them apart too:
        # collapsing both onto ``OrderError`` would cost the caller — and TD's
        # error normalization — the one thing the wire still knows.
        if code == "insufficient_balance":
            raise InsufficientBalanceError(message)
        if code == "order":
            raise OrderError(message)
        raise ExchangeError(message)
