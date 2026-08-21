"""Gate USDT-perpetual trading connector.

WebSocket for order entry and account pushes; REST for leverage, the account
snapshot, and history. Positions exist here, which spot does not have.

One-way mode is assumed. ``qty`` crosses this boundary in base and is
converted to signed contracts on the way out.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from decimal import Decimal

from mftik.exchange.base import BaseClient
from mftik.exchange.errors import ExchangeError, OrderError
from mftik.exchange.gate.future.client import (
    GATE_FUTURES_WS_URL,
    GateFuturesWebSocket,
)
from mftik.exchange.gate.future.models import (
    GateFuturesOrder,
    base_to_contracts,
    signed_contracts,
    to_text,
)
from mftik.exchange.gate.future.protocol import GateApiError, GateRestError
from mftik.exchange.gate.future.rest import GATE_FUTURES_REST_URL, GateFuturesRest
from mftik.exchange.models import (
    TERMINAL_STATUSES,
    Balance,
    Fill,
    Order,
    OrderType,
    PlaceOrderRequest,
    TimeInForce,
)
from mftik.exchange.oms import Position
from mftik.exchange.order_check import require_legal
from mftik.exchange.symbols import SymbolResolver, check_venue
from mftik.exchange.tickers import Category, UniversalTicker

logger = logging.getLogger(__name__)

_RESERVED_PARAMS = (
    "contract",
    "symbol",
    "size",
    "price",
    "tif",
    "text",
    "reduce_only",
    "side",
)

_TIF: dict[TimeInForce, str] = {
    TimeInForce.GTC: "gtc",
    TimeInForce.IOC: "ioc",
    TimeInForce.FOK: "fok",
    TimeInForce.POST_ONLY: "poc",
}


class GateFuturesPrivateClient(BaseClient):
    """Gate USDT-perpetual trading account for TD."""

    name = "GateFutures"
    category = Category.PERP

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        symbols: SymbolResolver,
        ws_url: str = GATE_FUTURES_WS_URL,
        rest_url: str = GATE_FUTURES_REST_URL,
        ws: GateFuturesWebSocket | None = None,
        rest: GateFuturesRest | None = None,
    ) -> None:
        super().__init__()
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret are required")
        self.api_key = api_key
        self.api_secret = api_secret
        self.ws = ws or GateFuturesWebSocket(
            api_key=api_key, api_secret=api_secret, url=ws_url
        )
        self.rest = rest or GateFuturesRest(
            api_key=api_key, api_secret=api_secret, base_url=rest_url
        )
        self.symbols = symbols
        self._contracts: dict[str, str] = {}

    async def connect(self) -> None:
        await self.ws.connect()
        await self.rest.connect()
        self._connected = True
        logger.info("GateFutures connected key=%s…", self.api_key[:6])

    async def close(self) -> None:
        self._connected = False
        self._contracts.clear()
        await self.ws.close()
        await self.rest.close()

    def on_reconnect(self, callback) -> None:
        self.ws.on_reconnect(callback)

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        self._ensure_connected()
        require_legal(request)
        extras = dict(request.params or {})
        for reserved in _RESERVED_PARAMS:
            if extras.pop(reserved, None) is not None:
                logger.warning(
                    "GateFutures ignoring params[%r]; set it on the request",
                    reserved,
                )
        ticker = request.ticker
        check_venue(ticker, self.name, {self.category})
        contract = await self.symbols.exch_ticker(ticker)
        multiplier = await self._multiplier(ticker)
        if request.qty is None:
            raise OrderError("GateFutures orders size in base; set qty")
        size = signed_contracts(
            request.side, base_to_contracts(request.qty, multiplier)
        )
        if request.type is OrderType.MARKET:
            price: Decimal | str | None = "0"
            default_tif = _TIF.get(request.tif, "ioc") if request.tif else "ioc"
            tif = extras.pop("tif", default_tif)
        else:
            price = request.price
            tif = extras.pop(
                "tif",
                _TIF[request.tif] if request.tif else _TIF[TimeInForce.GTC],
            )
        try:
            ack = await self.ws.place_order(
                contract=contract,
                size=size,
                price=price,
                tif=tif,
                text=request.client_order_id,
                reduce_only=request.reduce_only,
                **extras,
            )
        except GateApiError as exc:
            raise OrderError(str(exc)) from exc
        order = ack.to_order(ticker, multiplier)
        self._remember(order, contract)
        return order

    async def cancel_order(self, order_id: str) -> Order:
        self._ensure_connected()
        return await self._cancel(order_id)

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        self._ensure_connected()
        return await self._cancel(to_text(client_order_id))

    async def _cancel(self, order_id: str) -> Order:
        contract = await self._contract_for(order_id)
        try:
            ack = await self.ws.cancel_order(order_id, contract=contract)
        except GateApiError as exc:
            raise OrderError(str(exc)) from exc
        return await self._inbound(ack, contract)

    async def fetch_order(self, order_id: str) -> Order:
        self._ensure_connected()
        contract = await self._contract_for(order_id)
        try:
            ack = await self.ws.fetch_order(order_id, contract=contract)
        except GateApiError:
            ack = await self.rest.fetch_order(order_id, contract=contract)
        return await self._inbound(ack, contract)

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, ticker: UniversalTicker | None = None
    ) -> Order | None:
        self._ensure_connected()
        text = to_text(client_order_id)
        contract = self._contracts.get(text)
        if contract is None and ticker is not None:
            contract = await self.symbols.exch_ticker(ticker)
        if contract is None:
            raise OrderError(
                f"cannot resolve GateFutures order {client_order_id!r} "
                "without its symbol"
            )
        try:
            ack = await self.ws.fetch_order(text, contract=contract)
        except (GateApiError, GateRestError) as exc:
            if "NOT_FOUND" in str(exc).upper():
                return None
            try:
                ack = await self.rest.fetch_order(text, contract=contract)
            except GateRestError as rest_exc:
                if "NOT_FOUND" in str(rest_exc).upper():
                    return None
                raise OrderError(str(rest_exc)) from rest_exc
        return await self._inbound(ack, contract)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        self._ensure_connected()
        contract = await self._venue_symbol(symbol) if symbol else None
        try:
            rows = await self.ws.list_orders(contract=contract, status="open")
        except (GateApiError, ExchangeError):
            rows = await self.rest.fetch_open_orders(contract)
        out: list[Order] = []
        for row in rows:
            native = row.contract or contract
            if not native:
                continue
            out.append(await self._inbound(row, native))
        return out

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        return await self.rest.fetch_balances()

    async def fetch_positions(self) -> list[Position]:
        self._ensure_connected()
        out: list[Position] = []
        for row in await self.rest.fetch_positions():
            ticker = await self._resolve(row.contract)
            out.append(row.to_position(ticker, await self._multiplier(ticker)))
        return out

    async def fetch_leverage(self, ticker: UniversalTicker) -> Decimal:
        self._ensure_connected()
        check_venue(ticker, self.name, {self.category})
        return await self.rest.fetch_leverage(await self.symbols.exch_ticker(ticker))

    def stream_orders(self) -> AsyncIterator[Order]:
        self._ensure_connected()
        return self._orders()

    def stream_fills(self) -> AsyncIterator[Fill]:
        self._ensure_connected()
        return self._fills()

    def stream_balances(self) -> AsyncIterator[Balance]:
        self._ensure_connected()
        return self._balances()

    def stream_positions(self) -> AsyncIterator[Position]:
        self._ensure_connected()
        return self._positions()

    async def _orders(self) -> AsyncIterator[Order]:
        stream = await self.ws.subscribe_orders()
        async for update in stream:
            ticker = await self._resolve(update.contract)
            order = update.to_order(ticker, await self._multiplier(ticker))
            self._remember(order, update.contract)
            yield order

    async def _fills(self) -> AsyncIterator[Fill]:
        stream = await self.ws.subscribe_user_trades()
        async for trade in stream:
            ticker = await self._resolve(trade.contract)
            yield trade.to_fill(ticker, await self._multiplier(ticker))

    async def _balances(self) -> AsyncIterator[Balance]:
        stream = await self.ws.subscribe_balances()
        async for balance in stream:
            yield balance.to_balance()

    async def _positions(self) -> AsyncIterator[Position]:
        stream = await self.ws.subscribe_positions()
        async for row in stream:
            ticker = await self._resolve(row.contract)
            yield row.to_position(ticker, await self._multiplier(ticker))

    def _ticker(self, symbol: str) -> UniversalTicker:
        return UniversalTicker.of(self.name, self.category, symbol)

    async def _venue_symbol(self, symbol: str) -> str:
        return await self.symbols.exch_ticker(self._ticker(symbol))

    async def _resolve(self, contract: str) -> UniversalTicker:
        return await self.symbols.symbol_for(
            self.name, contract, category=self.category
        )

    async def _multiplier(self, ticker: UniversalTicker) -> Decimal:
        size = await self.symbols.contract_size(ticker)
        if size is None or size <= 0:
            raise OrderError(f"no contract_size for {ticker}; refuse to guess")
        return size

    async def _contract_for(self, order_id: str) -> str:
        found = self._contracts.get(order_id)
        if found is not None:
            return found
        await self.fetch_open_orders()
        found = self._contracts.get(order_id)
        if found is None:
            raise OrderError(f"no open GateFutures order for id {order_id!r}")
        return found

    async def _inbound(self, ack: GateFuturesOrder, contract: str) -> Order:
        native = ack.contract or contract
        ticker = await self._resolve(native)
        order = ack.to_order(ticker, await self._multiplier(ticker))
        self._remember(order, native)
        return order

    def _remember(self, order: Order, native: str) -> None:
        keys = [order.order_id]
        if order.client_order_id:
            keys += [order.client_order_id, to_text(order.client_order_id)]
        if order.status in TERMINAL_STATUSES:
            for key in keys:
                self._contracts.pop(key, None)
            return
        for key in keys:
            if key:
                self._contracts[key] = native


__all__ = ["GateFuturesPrivateClient"]
