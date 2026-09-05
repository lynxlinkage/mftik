"""The Bitget trading connector.

One private client holds the UTA credential. Attach reads
``GET /api/v3/account/settings`` first (V8): ``unified`` and ``hybrid``
proceed; ``upgrading`` / ``switching`` (and anything else) raise before
any order, naming the mode. ``holdMode`` is cached (V7) so place can
branch on hedge vs one-way (I8).

A spot market buy that the strategy sized in base is refused — Bitget's
wire ``qty`` on that path is quote (V6 / I9). Perp ``qty`` is base on
both linear books. ``product_of(request.ticker)`` chooses the wire
category so a USDC perp never rides ``USDT-FUTURES``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from decimal import Decimal

from mftik.exchange.base import BaseClient
from mftik.exchange.bitget.account import BitgetPrivateStream
from mftik.exchange.bitget.models import BitgetOrderUpdate, BitgetSettings
from mftik.exchange.bitget.protocol import (
    BITGET_REST_URL,
    PRODUCTS,
    SPOT,
    USDC_FUTURES,
    USDT_FUTURES,
    BitgetAccountModeError,
    BitgetAuthError,
    BitgetError,
    category_of,
    private_url,
    product_of,
)
from mftik.exchange.bitget.rest import BitgetRest
from mftik.exchange.errors import OrderError
from mftik.exchange.models import (
    TERMINAL_STATUSES,
    Balance,
    Fill,
    Order,
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
    TimeInForce,
)
from mftik.exchange.oms import Position
from mftik.exchange.order_check import require_legal, sized_amount
from mftik.exchange.symbols import SymbolResolver, check_venue
from mftik.exchange.tickers import Category, UniversalTicker

logger = logging.getLogger(__name__)

TERMINAL = TERMINAL_STATUSES

ACCEPTED_ACCOUNT_MODES = frozenset({"unified", "hybrid"})
REFUSED_ACCOUNT_MODES = frozenset({"upgrading", "switching"})
HEDGE_MODE = "hedge_mode"
ONE_WAY_MODE = "one_way_mode"

#: Body fields the request itself owns. ``params`` may not restate them.
#:
#: ``posSide`` is deliberately *not* here: hedge mode needs it and
#: :class:`~mftik.exchange.models.PlaceOrderRequest` has no field for it, so
#: ``params`` is the only channel — see :meth:`BitgetPrivateClient._pos_side`.
_RESERVED_PARAMS = (
    "category",
    "symbol",
    "qty",
    "side",
    "orderType",
    "price",
    "clientOid",
    "reduceOnly",
    "timeInForce",
)

_TIF: dict[TimeInForce, str] = {
    TimeInForce.GTC: "gtc",
    TimeInForce.IOC: "ioc",
    TimeInForce.FOK: "fok",
    TimeInForce.POST_ONLY: "post_only",
}

_FUTURES = (USDT_FUTURES, USDC_FUTURES)


class BitgetPrivateClient(BaseClient):
    """Bitget UTA trading account for TD, on every book the credential trades."""

    name = "Bitget"

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        passphrase: str,
        symbols: SymbolResolver,
        category: Category = Category.SPOT,
        rest_url: str = BITGET_REST_URL,
        private_url_: str | None = None,
        demo: bool = False,
        stream: BitgetPrivateStream | None = None,
        rest: BitgetRest | None = None,
    ) -> None:
        super().__init__()
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret are required")
        if not passphrase:
            raise BitgetAuthError("Bitget passphrase is required")
        self.api_key = api_key
        self.passphrase = passphrase
        self.category = category
        self.symbols = symbols
        self.demo = demo
        self.stream = stream or BitgetPrivateStream(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            url=private_url_ or private_url(demo=demo),
        )
        self.rest = rest or BitgetRest(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            base_url=rest_url,
            demo=demo,
        )
        self._venue_symbols: dict[str, str] = {}
        self._venue_products: dict[str, str] = {}
        self._last: dict[str, Order] = {}
        self._quote_sized: dict[str, bool] = {}
        self._hold_mode: str | None = None
        self._account_mode: str | None = None

    async def connect(self) -> None:
        await self.stream.connect()
        try:
            await self.rest.connect()
            settings = await self.rest.fetch_settings()
            self._apply_settings(settings)
        except Exception:
            await self.stream.close()
            raise
        self._connected = True
        logger.info(
            "Bitget connected key=%s… accountMode=%s holdMode=%s",
            self.api_key[:6],
            self._account_mode,
            self._hold_mode,
        )

    def _apply_settings(self, settings: BitgetSettings) -> None:
        mode = (settings.account_mode or "").strip()
        refused = not mode or mode in REFUSED_ACCOUNT_MODES
        if refused or mode not in ACCEPTED_ACCOUNT_MODES:
            raise BitgetAccountModeError(
                f"Bitget accountMode={mode or 'unknown'!r} cannot trade; "
                f"UTA required (accepted: {', '.join(sorted(ACCEPTED_ACCOUNT_MODES))})"
            )
        hold = (settings.hold_mode or "").strip()
        if not hold:
            raise BitgetAccountModeError(
                "Bitget settings has no holdMode; refuse to guess (V7)"
            )
        self._account_mode = mode
        self._hold_mode = hold

    async def close(self) -> None:
        self._connected = False
        self._venue_symbols.clear()
        self._venue_products.clear()
        self._quote_sized.clear()
        self._hold_mode = None
        self._account_mode = None
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
                    "Bitget ignoring params[%r]; set it on the request", reserved
                )

        ticker = request.ticker
        check_venue(ticker, self.name)
        product = product_of(ticker)
        native = await self.symbols.exch_ticker(ticker)
        qty, quote_sized = self._qty(request, product)
        args: dict[str, object] = {
            "category": product,
            "symbol": native,
            "side": request.side.value,
            "orderType": self._order_type(request),
            "qty": qty,
        }
        if request.price is not None:
            args["price"] = request.price
        if request.client_order_id:
            args["clientOid"] = request.client_order_id
        tif = self._tif(request)
        if tif is not None:
            args["timeInForce"] = tif
        if request.reduce_only:
            args["reduceOnly"] = "YES"
        pos_side = self._pos_side(extras)
        if pos_side is not None:
            args["posSide"] = pos_side
        args.update(extras)

        try:
            ack = await self.rest.place_order(args)
        except BitgetError as exc:
            raise OrderError(str(exc)) from exc

        order = Order(
            universal_ticker=str(ticker),
            order_id=ack.order_id,
            client_order_id=ack.client_order_id or request.client_order_id,
            side=request.side,
            type=request.type,
            status=OrderStatus.PENDING_NEW,
            qty=request.qty if request.qty is not None else Decimal("0"),
            quote_qty=request.quote_qty,
            price=request.price,
        )
        self._remember(order, native, product, quote_sized=quote_sized)
        return order

    def _order_type(self, request: PlaceOrderRequest) -> str:
        if request.type is OrderType.MARKET:
            return "market"
        if request.tif is TimeInForce.POST_ONLY:
            return "limit"
        return "limit"

    def _tif(self, request: PlaceOrderRequest) -> str | None:
        if request.type is OrderType.MARKET:
            return None
        if request.tif:
            return _TIF[request.tif]
        return _TIF[TimeInForce.GTC]

    def _qty(self, request: PlaceOrderRequest, product: str) -> tuple[Decimal, bool]:
        """Wire ``qty`` and whether it was quote-sized.

        A spot market buy that the strategy sized in base is refused —
        Bitget would treat that number as USDT (V6 / I9).
        """
        if (
            product == SPOT
            and request.type is OrderType.MARKET
            and request.side is Side.BUY
        ):
            if request.quote_qty is None:
                raise OrderError(
                    "Bitget spot market buy sizes qty in quote; set quote_qty "
                    "(a base qty would be treated as USDT)"
                )
            return request.quote_qty, True
        return sized_amount(request), request.quote_qty is not None

    def _pos_side(self, extras: dict[str, object]) -> str | None:
        """``posSide`` for this order, consumed out of ``extras`` (I8).

        Hedge mode requires it and refuses locally when it is missing.
        One-way mode omits the field, and drops any value the caller sent so
        it cannot reach the body through ``args.update(extras)``.
        """
        pos_side = extras.pop("posSide", None)
        if self._hold_mode == HEDGE_MODE:
            if not pos_side:
                raise OrderError(
                    "Bitget hedge_mode requires posSide; the order was not sent"
                )
            return str(pos_side)
        if pos_side:
            logger.warning(
                "Bitget %s ignores params['posSide']=%r; it is a hedge-mode "
                "field",
                self._hold_mode or ONE_WAY_MODE,
                pos_side,
            )
        return None

    async def cancel_order(self, order_id: str) -> Order:
        self._ensure_connected()
        known = await self._known(order_id)
        return await self._cancel(known, order_id=order_id)

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        self._ensure_connected()
        known = await self._known(client_order_id)
        return await self._cancel(known, client_oid=client_order_id)

    async def _cancel(
        self,
        known: Order,
        *,
        order_id: str | None = None,
        client_oid: str | None = None,
    ) -> Order:
        key = order_id or client_oid or ""
        native = self._venue_symbols[key]
        product = self._venue_products[key]
        try:
            await self.rest.cancel_order(
                category=product,
                symbol=native,
                order_id=order_id,
                client_oid=client_oid,
            )
        except BitgetError as exc:
            raise OrderError(str(exc)) from exc
        return known.model_copy(update={"status": OrderStatus.PENDING_CANCEL})

    async def _known(self, key: str) -> Order:
        found = self._last.get(key)
        if found is not None:
            return found
        await self.fetch_open_orders()
        found = self._last.get(key)
        if found is None:
            raise OrderError(f"no open Bitget order for id {key!r}")
        return found

    # --- recon -------------------------------------------------------------

    async def fetch_order(self, order_id: str) -> Order:
        self._ensure_connected()
        known = self._last.get(order_id)
        product = self._venue_products.get(order_id) or product_of(
            known.ticker if known else self._ticker("BTCUSDT")
        )
        row = await self.rest.fetch_order(category=product, order_id=order_id)
        if row is None:
            raise OrderError(f"no Bitget order for id {order_id!r}")
        return await self._inbound(row)

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, ticker: UniversalTicker | None = None
    ) -> Order | None:
        self._ensure_connected()
        known = self._last.get(client_order_id)
        if known is not None:
            ticker = known.ticker
        product = (
            self._venue_products.get(client_order_id)
            or (product_of(ticker) if ticker else SPOT)
        )
        row = await self.rest.fetch_order(
            category=product, client_oid=client_order_id
        )
        if row is None:
            return None
        return await self._inbound(row)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        self._ensure_connected()
        if symbol is not None:
            native = await self._venue_symbol(symbol)
            product = product_of(self._ticker(symbol))
            rows = await self.rest.fetch_open_orders(product, native)
            return [await self._inbound(row) for row in rows]

        out: list[Order] = []
        for product in PRODUCTS:
            for row in await self.rest.fetch_open_orders(product):
                out.append(await self._inbound(row))
        return out

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        return await self.rest.fetch_balances()

    async def fetch_positions(self) -> list[Position]:
        self._ensure_connected()
        out: list[Position] = []
        for product in _FUTURES:
            for row in await self.rest.fetch_position_rows(product):
                ticker = await self._resolve(row.symbol, row.category or product)
                out.append(row.to_position(ticker))
        return out

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
                ticker = await self._resolve(row.symbol, row.category)
                yield row.to_fill(ticker)
        finally:
            stream.close()

    async def _positions(self) -> AsyncIterator[Position]:
        stream = await self.stream.subscribe_positions()
        try:
            async for row in stream:
                ticker = await self._resolve(row.symbol, row.category)
                if ticker.category is not Category.PERP:
                    continue
                yield row.to_position(ticker)
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

    async def _resolve(
        self, native_symbol: str, product: str = ""
    ) -> UniversalTicker:
        return await self.symbols.symbol_for(
            self.name,
            native_symbol,
            category=category_of(product, self.category),
        )

    async def _inbound(self, row: BitgetOrderUpdate) -> Order:
        ticker = await self._resolve(row.symbol, row.category)
        order = row.to_order(ticker)
        if order.quote_qty is None and self._was_quote_sized(order):
            order = order.model_copy(
                update={
                    "qty": row.acc_base,
                    "quote_qty": row.qty,
                    "filled_qty": row.acc_base,
                }
            )
        product = row.category or product_of(ticker)
        self._remember(
            order,
            row.symbol,
            product,
            quote_sized=self._was_quote_sized(order) or order.quote_qty is not None,
        )
        return order

    def _was_quote_sized(self, order: Order) -> bool:
        for key in (order.order_id, order.client_order_id):
            if key and self._quote_sized.get(key):
                return True
        return False

    def _remember(
        self,
        order: Order,
        native_symbol: str,
        product: str,
        *,
        quote_sized: bool,
    ) -> None:
        keys = [order.order_id]
        if order.client_order_id:
            keys.append(order.client_order_id)
        if order.status in TERMINAL:
            for key in keys:
                self._venue_symbols.pop(key, None)
                self._venue_products.pop(key, None)
                self._last.pop(key, None)
                self._quote_sized.pop(key, None)
            return
        for key in keys:
            if key:
                self._venue_symbols[key] = native_symbol
                self._venue_products[key] = product
                self._last[key] = order
                self._quote_sized[key] = quote_sized


__all__ = [
    "ACCEPTED_ACCOUNT_MODES",
    "HEDGE_MODE",
    "ONE_WAY_MODE",
    "REFUSED_ACCOUNT_MODES",
    "BitgetPrivateClient",
]
