"""The OKX trading connector.

Composes two transports — there is no third, because OKX has no trade socket:

* :class:`~mftik.exchange.okx.account.OkxPrivateStream` — what happened.
  Order updates and fills arrive here or nowhere.
* :class:`~mftik.exchange.okx.rest.OkxRest` — order entry and recon.

Shapes that drive most of what follows:

* **A spot market buy sizes in the quote currency by default.** ``sz=0.5``
  on BTC-USDT means half a dollar unless ``tgtCcy`` says ``base_ccy``. The
  shared model sizes in base (``qty``) or quote (``quote_qty``), so this
  connector sends the flag to match.
* **Post-only is an order type**, spelled ``post_only``. IOC and FOK are
  too — OKX has no separate time-in-force field on a limit.
* **The order ack carries no status.** OKX acknowledges receipt with an id
  and an ``sCode``; the ``orders`` channel says what became of it. So
  :meth:`place_order` reports ``PENDING_NEW``.
* **The category is not on the order.** One credential trades every book;
  which book an order is for is ``instId`` / ``instType``. The private
  stream is unscoped: a session that hid half the account would be
  reporting a balance sheet it could not explain.

Spot orders go out with ``tdMode=cash``; SWAP with ``tdMode=cross`` and
``posSide=net``. Hedge mode is not modelled.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from decimal import Decimal

from mftik.exchange.base import BaseClient
from mftik.exchange.errors import ExchangeError, OrderError
from mftik.exchange.models import (
    TERMINAL_STATUSES,
    Balance,
    Fill,
    Order,
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    TimeInForce,
)
from mftik.exchange.okx.account import OkxPrivateStream
from mftik.exchange.okx.models import (
    OkxOrderUpdate,
    base_to_contracts,
    category_of,
)
from mftik.exchange.okx.protocol import (
    OKX_REST_URL,
    SPOT,
    SWAP,
    OkxAuthError,
    OkxError,
    private_url,
    product_of,
)
from mftik.exchange.okx.rest import OkxRest
from mftik.exchange.oms import Position
from mftik.exchange.order_check import require_legal, sized_amount
from mftik.exchange.symbols import SymbolResolver, check_venue
from mftik.exchange.tickers import Category, UniversalTicker

logger = logging.getLogger(__name__)

TERMINAL = TERMINAL_STATUSES

_RESERVED_PARAMS = (
    "instId",
    "tdMode",
    "side",
    "ordType",
    "sz",
    "px",
    "clOrdId",
    "tgtCcy",
    "reduceOnly",
    "posSide",
)

_ORD_TYPE: dict[TimeInForce, str] = {
    TimeInForce.GTC: "limit",
    TimeInForce.IOC: "ioc",
    TimeInForce.FOK: "fok",
    TimeInForce.POST_ONLY: "post_only",
}

_BASE_CCY = "base_ccy"
_QUOTE_CCY = "quote_ccy"
_CASH = "cash"
_CROSS = "cross"
_NET = "net"


class OkxPrivateClient(BaseClient):
    """OKX trading account for TD, on every book the credential trades."""

    name = "Okx"

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        passphrase: str,
        symbols: SymbolResolver,
        category: Category = Category.SPOT,
        rest_url: str = OKX_REST_URL,
        private_url_: str | None = None,
        demo: bool = False,
        stream: OkxPrivateStream | None = None,
        rest: OkxRest | None = None,
    ) -> None:
        super().__init__()
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret are required")
        if not passphrase:
            raise OkxAuthError("OKX requires a passphrase")
        self.api_key = api_key
        self.passphrase = passphrase
        self.category = category
        self.product = product_of(category)
        self.symbols = symbols
        self.demo = demo
        self.stream = stream or OkxPrivateStream(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            url=private_url_ or private_url(demo=demo),
        )
        self.rest = rest or OkxRest(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            base_url=rest_url,
            demo=demo,
        )
        self._venue_symbols: dict[str, str] = {}
        self._last: dict[str, Order] = {}
        self._quote_sized: dict[str, bool] = {}

    async def connect(self) -> None:
        await self.stream.connect()
        try:
            await self.rest.connect()
        except Exception:
            await self.stream.close()
            raise
        self._connected = True
        logger.info("OKX connected key=%s…", self.api_key[:6])

    async def close(self) -> None:
        self._connected = False
        self._venue_symbols.clear()
        self._quote_sized.clear()
        await self.stream.close()
        await self.rest.close()

    def on_reconnect(self, callback) -> None:
        self.stream.on_reconnect(callback)

    # --- order entry -------------------------------------------------------

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        self._ensure_connected()
        require_legal(request)

        extras = dict(request.params or {})
        for reserved in _RESERVED_PARAMS:
            if extras.pop(reserved, None) is not None:
                logger.warning(
                    "OKX ignoring params[%r]; set it on the request", reserved
                )

        ticker = request.ticker
        check_venue(ticker, self.name)
        product = product_of(ticker.category)
        inst_id = await self.symbols.exch_ticker(ticker)
        args: dict[str, object] = {
            "instId": inst_id,
            "tdMode": extras.pop("tdMode", _CASH if product == SPOT else _CROSS),
            "side": request.side.value,
            "ordType": extras.pop("ordType", self._ord_type(request)),
            "sz": await self._size(request, ticker),
        }
        if request.price is not None:
            args["px"] = request.price
        if request.client_order_id:
            args["clOrdId"] = request.client_order_id
        tgt = extras.pop("tgtCcy", self._tgt_ccy(request, product))
        if tgt is not None:
            args["tgtCcy"] = tgt
        if request.reduce_only:
            args["reduceOnly"] = True
        if product == SWAP:
            args["posSide"] = extras.pop("posSide", _NET)
        args.update(extras)

        try:
            ack = await self.rest.place_order(args)
        except OkxError as exc:
            raise OrderError(str(exc)) from exc

        order = Order(
            universal_ticker=str(ticker),
            order_id=ack.ord_id,
            client_order_id=ack.client_order_id or request.client_order_id,
            side=request.side,
            type=request.type,
            status=OrderStatus.PENDING_NEW,
            qty=request.qty if request.qty is not None else Decimal("0"),
            quote_qty=request.quote_qty,
            price=request.price,
        )
        self._remember(order, inst_id)
        return order

    def _ord_type(self, request: PlaceOrderRequest) -> str:
        if request.type is OrderType.MARKET:
            return "market"
        return _ORD_TYPE[request.tif] if request.tif else _ORD_TYPE[TimeInForce.GTC]

    def _tgt_ccy(self, request: PlaceOrderRequest, product: str) -> str | None:
        if product != SPOT or request.type is not OrderType.MARKET:
            return None
        return _QUOTE_CCY if request.quote_qty is not None else _BASE_CCY

    async def cancel_order(self, order_id: str) -> Order:
        self._ensure_connected()
        known = await self._known(order_id)
        return await self._cancel(known, ord_id=order_id)

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        self._ensure_connected()
        known = await self._known(client_order_id)
        return await self._cancel(known, cl_ord_id=client_order_id)

    async def _cancel(
        self,
        known: Order,
        *,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ) -> Order:
        inst_id = self._venue_symbols[ord_id or cl_ord_id or ""]
        try:
            await self.rest.cancel_order(
                inst_id=inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id
            )
        except OkxError as exc:
            raise OrderError(str(exc)) from exc
        return known.model_copy(update={"status": OrderStatus.PENDING_CANCEL})

    async def _known(self, key: str) -> Order:
        found = self._last.get(key)
        if found is not None:
            return found
        await self.fetch_open_orders()
        found = self._last.get(key)
        if found is None:
            raise OrderError(f"no open OKX order for id {key!r}")
        return found

    # --- recon -------------------------------------------------------------

    async def fetch_order(self, order_id: str) -> Order:
        self._ensure_connected()
        known = self._last.get(order_id)
        inst_id = self._venue_symbols.get(order_id)
        product = product_of(known.category) if known else self.product
        row = await self.rest.fetch_order(
            inst_id=inst_id, product=product, ord_id=order_id
        )
        if row is None:
            raise OrderError(f"no OKX order for id {order_id!r}")
        return await self._inbound(row)

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, ticker: UniversalTicker | None = None
    ) -> Order | None:
        self._ensure_connected()
        known = self._last.get(client_order_id)
        if known is not None:
            ticker = known.ticker
        inst_id = self._venue_symbols.get(client_order_id)
        if inst_id is None and ticker is not None:
            inst_id = await self.symbols.exch_ticker(ticker)
        product = product_of(ticker.category) if ticker else self.product
        row = await self.rest.fetch_order(
            inst_id=inst_id, product=product, cl_ord_id=client_order_id
        )
        if row is None:
            return None
        return await self._inbound(row)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        self._ensure_connected()
        if symbol is not None:
            native = await self._venue_symbol(symbol)
            rows = await self.rest.fetch_open_orders(self.product, native)
            return [await self._inbound(row) for row in rows]

        out: list[Order] = []
        for product in (SPOT, SWAP):
            for row in await self.rest.fetch_open_orders(product):
                out.append(await self._inbound(row))
        return out

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        return await self.rest.fetch_balances()

    async def fetch_positions(self) -> list[Position]:
        self._ensure_connected()
        rows = await self.rest.fetch_position_rows(SWAP)
        out: list[Position] = []
        for row in rows:
            ticker = await self._resolve(row.symbol, row.inst_type or SWAP)
            out.append(
                row.to_position(
                    ticker, contract_size=await self._multiplier(ticker)
                )
            )
        return out

    async def fetch_leverage(self, ticker: UniversalTicker) -> Decimal:
        self._ensure_connected()
        check_venue(ticker, self.name, {Category.SPOT, Category.PERP})
        if ticker.category is not Category.PERP:
            raise ExchangeError(
                f"OKX leverage is only defined on perps, got {ticker}"
            )
        native = await self.symbols.exch_ticker(ticker)
        row = await self.rest.fetch_leverage_row(native)
        if row.lever is None or row.lever <= 0:
            raise ExchangeError(
                f"OKX leverage-info for {native} has no leverage"
            )
        return row.lever

    # --- streams -----------------------------------------------------------

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
        stream = await self.stream.subscribe_orders()
        try:
            async for row in stream:
                yield await self._inbound(row)
        finally:
            stream.close()

    async def _fills(self) -> AsyncIterator[Fill]:
        stream = await self.stream.subscribe_fills()
        try:
            async for row in stream:
                if not row.is_fill:
                    continue
                ticker = await self._resolve(row.symbol, row.inst_type)
                yield row.to_fill(
                    ticker, contract_size=await self._multiplier(ticker)
                )
        finally:
            stream.close()

    async def _positions(self) -> AsyncIterator[Position]:
        stream = await self.stream.subscribe_positions()
        try:
            async for row in stream:
                ticker = await self._resolve(row.symbol, row.inst_type)
                yield row.to_position(
                    ticker, contract_size=await self._multiplier(ticker)
                )
        finally:
            stream.close()

    async def _balances(self) -> AsyncIterator[Balance]:
        stream = await self.stream.subscribe_account()
        try:
            async for wallet in stream:
                for balance in wallet.to_balances():
                    yield balance
        finally:
            stream.close()

    # --- symbols -----------------------------------------------------------

    def _ticker(self, symbol: str) -> UniversalTicker:
        return UniversalTicker.of(self.name, self.category, symbol)

    async def _venue_symbol(self, symbol: str) -> str:
        return await self.symbols.exch_ticker(self._ticker(symbol))

    async def _size(
        self, request: PlaceOrderRequest, ticker: UniversalTicker
    ) -> Decimal:
        """Venue ``sz``: base on spot, contracts on SWAP."""
        scale = await self._multiplier(ticker)
        if scale is None:
            return sized_amount(request)
        if request.qty is None:
            raise OrderError("OKX SWAP orders size in base; set qty")
        return base_to_contracts(request.qty, scale)

    async def _multiplier(self, ticker: UniversalTicker) -> Decimal | None:
        if ticker.category is not Category.PERP:
            return None
        size = await self.symbols.contract_size(ticker)
        if size is None or size <= 0:
            raise OrderError(f"no contract_size for {ticker}; refuse to guess")
        return size

    async def _resolve(
        self, native_symbol: str, inst_type: str = ""
    ) -> UniversalTicker:
        return await self.symbols.symbol_for(
            self.name,
            native_symbol,
            category=category_of(inst_type, self.category),
        )

    async def _inbound(self, row: OkxOrderUpdate) -> Order:
        ticker = await self._resolve(row.symbol, row.inst_type)
        order = row.to_order(
            ticker, contract_size=await self._multiplier(ticker)
        )
        if order.quote_qty is None and self._was_quote_sized(order):
            order = order.model_copy(
                update={
                    "qty": row.acc_fill_sz,
                    "quote_qty": row.sz,
                    "filled_qty": row.acc_fill_sz,
                }
            )
        self._remember(order, row.symbol)
        return order

    def _was_quote_sized(self, order: Order) -> bool:
        for key in (order.order_id, order.client_order_id):
            if key and self._quote_sized.get(key):
                return True
        return False

    def _remember(self, order: Order, native_symbol: str) -> None:
        keys = [order.order_id]
        if order.client_order_id:
            keys.append(order.client_order_id)
        quote_sized = order.quote_qty is not None
        if order.status in TERMINAL:
            for key in keys:
                self._venue_symbols.pop(key, None)
                self._last.pop(key, None)
                self._quote_sized.pop(key, None)
            return
        for key in keys:
            if key:
                self._venue_symbols[key] = native_symbol
                self._last[key] = order
                self._quote_sized[key] = quote_sized


__all__ = ["OkxPrivateClient"]
