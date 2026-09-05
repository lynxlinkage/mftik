"""The Deribit trading connector.

One private client holds the HMAC credential. Connect reads
``private/get_account_summaries`` (V8 / V9): every margin model proceeds;
the model is logged, not used as a gate. Deribit is one-way net; there
is no hedge ``posSide``.

v1 places and cancels on the authenticated socket. Spot and linear perp
``qty`` is base (V6); ``quote_qty`` is refused. ``post_only`` defaults
true on the venue — every non-``POST_ONLY`` order sends
``post_only=false`` (V7).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

from mftik.exchange.base import BaseClient
from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.account import DeribitPrivateStream
from mftik.exchange.deribit.models import (
    DeribitAccountSummaries,
    DeribitOrderAck,
    DeribitOrderUpdate,
    DeribitPosition,
)
from mftik.exchange.deribit.protocol import (
    DERIBIT_WS_URL,
    KIND_FUTURE,
    MARGIN_MODELS,
    DeribitAuthError,
    DeribitError,
    category_from_instrument,
    is_linear_perp_name,
)
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

_RESERVED_PARAMS = (
    "instrument_name",
    "amount",
    "type",
    "label",
    "price",
    "post_only",
    "reject_post_only",
    "time_in_force",
    "reduce_only",
)

_TIF: dict[TimeInForce, str] = {
    TimeInForce.GTC: "good_til_cancelled",
    TimeInForce.IOC: "immediate_or_cancel",
    TimeInForce.FOK: "fill_or_kill",
    TimeInForce.POST_ONLY: "good_til_cancelled",
}

_LABEL_MAX = 64


class DeribitPrivateClient(BaseClient):
    """Deribit trading account for TD, on spot and the linear perpetual books."""

    name = "Deribit"

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        symbols: SymbolResolver,
        category: Category = Category.SPOT,
        private_url: str | None = None,
        stream: DeribitPrivateStream | None = None,
    ) -> None:
        super().__init__()
        if not api_key or not api_secret:
            raise DeribitAuthError("api_key and api_secret are required")
        self.api_key = api_key
        self.category = category
        self.symbols = symbols
        self.stream = stream or DeribitPrivateStream(
            api_key=api_key,
            api_secret=api_secret,
            url=private_url or DERIBIT_WS_URL,
        )
        self._venue_symbols: dict[str, str] = {}
        self._last: dict[str, Order] = {}
        self._margin_model: str | None = None
        self._account_type: str | None = None
        self._portfolio_ccys: set[str] = set()

    async def connect(self) -> None:
        await self.stream.connect()
        try:
            raw = await self.stream.rpc(
                ch.PRIVATE_GET_ACCOUNT_SUMMARIES, {"extended": True}
            )
            summaries = self._parse_summaries(raw)
            self._apply_summaries(summaries)
            await self.stream.watch_portfolios(self._portfolio_ccys)
        except Exception:
            await self.stream.close()
            raise
        self._connected = True
        logger.info(
            "Deribit connected key=%s… margin_model=%s type=%s",
            self.api_key[:6],
            self._margin_model,
            self._account_type,
        )

    def _parse_summaries(self, raw: Any) -> DeribitAccountSummaries:
        if isinstance(raw, list):
            return DeribitAccountSummaries.model_validate({"summaries": raw})
        if isinstance(raw, dict):
            return DeribitAccountSummaries.model_validate(raw)
        return DeribitAccountSummaries()

    def _apply_summaries(self, summaries: DeribitAccountSummaries) -> None:
        model = summaries.margin_model()
        if model and model not in MARGIN_MODELS:
            logger.warning("Deribit unknown margin_model=%r; accepting (V8)", model)
        self._margin_model = model or None
        self._account_type = summaries.type or None
        self._portfolio_ccys = {
            row.currency.upper()
            for row in summaries.summaries
            if row.currency
            and (row.equity or row.balance or row.available_funds)
        }

    async def close(self) -> None:
        self._connected = False
        self._venue_symbols.clear()
        self._last.clear()
        self._margin_model = None
        self._account_type = None
        self._portfolio_ccys.clear()
        await self.stream.close()

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
                    "Deribit ignoring params[%r]; set it on the request", reserved
                )

        ticker = request.ticker
        check_venue(ticker, self.name)
        native = await self.symbols.exch_ticker(ticker)
        label = request.client_order_id
        if label and len(label) > _LABEL_MAX:
            raise OrderError(
                f"Deribit label is at most {_LABEL_MAX} characters"
            )

        method = ch.PRIVATE_BUY if request.side is Side.BUY else ch.PRIVATE_SELL
        post_only = request.tif is TimeInForce.POST_ONLY
        args: dict[str, Any] = {
            "instrument_name": native,
            "amount": float(sized_amount(request)),
            "type": "market" if request.type is OrderType.MARKET else "limit",
            "post_only": post_only,
        }
        if request.type is OrderType.LIMIT and request.price is not None:
            args["price"] = float(request.price)
        if label:
            args["label"] = label
        if request.type is OrderType.LIMIT:
            tif = request.tif or TimeInForce.GTC
            args["time_in_force"] = _TIF[tif]
        if post_only:
            args["reject_post_only"] = True
        if request.reduce_only:
            args["reduce_only"] = True
        args.update(extras)

        try:
            raw = await self.stream.rpc(method, args)
        except DeribitError as exc:
            raise OrderError(str(exc)) from exc

        ack = DeribitOrderAck.model_validate(raw if isinstance(raw, dict) else {})
        placed = ack.placed()
        if placed.instrument_name:
            order = placed.to_order(ticker)
            if order.status is OrderStatus.UNKNOWN:
                order = order.model_copy(update={"status": OrderStatus.PENDING_NEW})
        else:
            order = Order(
                universal_ticker=str(ticker),
                order_id=ack.order_id or placed.order_id,
                client_order_id=ack.client_order_id or label,
                side=request.side,
                type=request.type,
                status=OrderStatus.PENDING_NEW,
                qty=request.qty if request.qty is not None else Decimal("0"),
                price=request.price,
            )
        self._remember(order, native)
        return order

    async def cancel_order(self, order_id: str) -> Order:
        self._ensure_connected()
        known = await self._known(order_id)
        return await self._cancel(known, order_id=order_id)

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        self._ensure_connected()
        known = await self._known(client_order_id)
        return await self._cancel(known, label=client_order_id)

    async def _cancel(
        self,
        known: Order,
        *,
        order_id: str | None = None,
        label: str | None = None,
    ) -> Order:
        try:
            if label:
                await self.stream.rpc(ch.PRIVATE_CANCEL_BY_LABEL, {"label": label})
            else:
                await self.stream.rpc(ch.PRIVATE_CANCEL, {"order_id": order_id})
        except DeribitError as exc:
            raise OrderError(str(exc)) from exc
        return known.model_copy(update={"status": OrderStatus.PENDING_CANCEL})

    async def _known(self, key: str) -> Order:
        found = self._last.get(key)
        if found is not None:
            return found
        await self.fetch_open_orders()
        found = self._last.get(key)
        if found is None:
            raise OrderError(f"no open Deribit order for id {key!r}")
        return found

    # --- recon -------------------------------------------------------------

    async def fetch_order(self, order_id: str) -> Order:
        self._ensure_connected()
        raw = await self.stream.rpc(ch.PRIVATE_GET_ORDER_STATE, {"order_id": order_id})
        row = self._as_order_row(raw)
        if row is None:
            raise OrderError(f"no Deribit order for id {order_id!r}")
        return await self._inbound(row)

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, ticker: UniversalTicker | None = None
    ) -> Order | None:
        self._ensure_connected()
        raw = await self.stream.rpc(
            ch.PRIVATE_GET_ORDER_STATE_BY_LABEL, {"label": client_order_id}
        )
        row = self._as_order_row(raw)
        if row is None:
            return None
        return await self._inbound(row)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        self._ensure_connected()
        params: dict[str, Any] = {"currency": "any"}
        if symbol is not None:
            native = await self._venue_symbol(symbol)
            raw = await self.stream.rpc(
                ch.PRIVATE_GET_OPEN_ORDERS,
                {"currency": "any", "instrument_name": native},
            )
        else:
            raw = await self.stream.rpc(ch.PRIVATE_GET_OPEN_ORDERS, params)
        rows = raw if isinstance(raw, list) else []
        out: list[Order] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            out.append(await self._inbound(DeribitOrderUpdate.model_validate(item)))
        return out

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        raw = await self.stream.rpc(
            ch.PRIVATE_GET_ACCOUNT_SUMMARIES, {"extended": True}
        )
        summaries = self._parse_summaries(raw)
        self._apply_summaries(summaries)
        await self.stream.watch_portfolios(self._portfolio_ccys)
        return summaries.to_balances()

    async def fetch_positions(self) -> list[Position]:
        self._ensure_connected()
        raw = await self.stream.rpc(
            ch.PRIVATE_GET_POSITIONS, {"currency": "any", "kind": KIND_FUTURE}
        )
        rows = raw if isinstance(raw, list) else []
        out: list[Position] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            row = DeribitPosition.model_validate(item)
            if not row.is_linear_perp and not is_linear_perp_name(row.instrument_name):
                continue
            ticker = await self._resolve(row.instrument_name)
            if ticker is None or ticker.category is not Category.PERP:
                continue
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

    async def _orders(self) -> AsyncIterator[Order]:
        stream = await self.stream.subscribe_orders()
        try:
            async for row in stream:
                inbound = await self._inbound(row)
                if inbound is not None:
                    yield inbound
        finally:
            stream.close()

    async def _fills(self) -> AsyncIterator[Fill]:
        stream = await self.stream.subscribe_fills()
        try:
            async for row in stream:
                if not row.is_fill:
                    continue
                ticker = await self._resolve(row.instrument_name)
                if ticker is None:
                    continue
                yield row.to_fill(ticker)
        finally:
            stream.close()

    async def _balances(self) -> AsyncIterator[Balance]:
        stream = await self.stream.subscribe_account(self._portfolio_ccys)
        try:
            async for row in stream:
                balance = row.to_balance()
                if balance is None:
                    continue
                if row.currency:
                    self._portfolio_ccys.add(row.currency.upper())
                yield balance
        finally:
            stream.close()

    # --- symbols -----------------------------------------------------------

    def _ticker(self, symbol: str) -> UniversalTicker:
        return UniversalTicker.of(self.name, self.category, symbol)

    async def _venue_symbol(self, symbol: str) -> str:
        return await self.symbols.exch_ticker(self._ticker(symbol))

    async def _resolve(self, instrument_name: str) -> UniversalTicker | None:
        if not instrument_name:
            return None
        if instrument_name.endswith("-PERPETUAL") and not is_linear_perp_name(
            instrument_name
        ):
            return None
        category = category_from_instrument(instrument_name)
        try:
            return await self.symbols.symbol_for(
                self.name, instrument_name, category=category
            )
        except Exception:
            logger.debug(
                "Deribit dropping unresolved instrument %s", instrument_name
            )
            return None

    def _as_order_row(self, raw: Any) -> DeribitOrderUpdate | None:
        if isinstance(raw, list):
            raw = next((item for item in raw if isinstance(item, dict)), None)
        if not isinstance(raw, dict):
            return None
        if "order" in raw and isinstance(raw["order"], dict):
            raw = raw["order"]
        return DeribitOrderUpdate.model_validate(raw)

    async def _inbound(self, row: DeribitOrderUpdate) -> Order:
        ticker = await self._resolve(row.instrument_name)
        if ticker is None:
            ticker = self._ticker("UNKNOWN")
        order = row.to_order(ticker)
        self._remember(order, row.instrument_name)
        return order

    def _remember(self, order: Order, native_symbol: str) -> None:
        keys = [order.order_id]
        if order.client_order_id:
            keys.append(order.client_order_id)
        if order.status in TERMINAL:
            for key in keys:
                self._venue_symbols.pop(key, None)
                self._last.pop(key, None)
            return
        for key in keys:
            if key:
                self._venue_symbols[key] = native_symbol
                self._last[key] = order


__all__ = ["DeribitPrivateClient"]
