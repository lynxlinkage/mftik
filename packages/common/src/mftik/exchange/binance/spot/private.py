"""The Binance spot trading connector.

Not an implementation of a shared trading interface — there is none; see
:mod:`mftik.exchange.base`. It resembles Gate's connector because the same
questions get asked of every venue, not because anything enforces it.

Composes exactly one transport: the WebSocket API. Order entry, cancels, order
and account recon reads, and the account push streams all live on the same
socket, so unlike Gate there is no REST half here and nothing to keep in sync
between two error vocabularies.

Three Binance shapes drive most of what follows:

* **Post-only is an order type, not a time-in-force.** Binance spells it
  ``LIMIT_MAKER`` and refuses a ``timeInForce`` alongside it, so
  :attr:`~mftik.exchange.models.TimeInForce.POST_ONLY` is translated into the
  type here — which is the case :class:`~mftik.exchange.models.TimeInForce`
  anticipates.
* **A market buy sizes in base currency.** ``quantity`` means the same thing
  for every order type, so a market buy needs no conversion and no refusal —
  the thing Gate cannot do.
* **Fills come from the order stream.** There is no user-trade channel;
  ``executionReport`` with ``x == "TRADE"`` is the fill, and its ``l``/``L``
  are that execution while ``z``/``Z`` are the order's totals.

Symbols are translated through a :class:`~mftik.exchange.symbols.SymbolResolver`
(the symbol plane), never by string surgery — see that module for why.

This is the one place in the Binance adapter that *is* shaped by the shared
interface, because TD's ``Session`` drives it. The venue-native surface stays
reachable underneath as ``self.api``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

from mftik.exchange.base import BaseClient
from mftik.exchange.binance.spot.client import BinanceSpotWsApi
from mftik.exchange.binance.spot.models import BinanceOrderAck
from mftik.exchange.binance.spot.protocol import (
    BINANCE_SPOT_WS_API_URL,
    BinanceWsError,
)
from mftik.exchange.errors import OrderError
from mftik.exchange.models import (
    TERMINAL_STATUSES,
    Balance,
    Fill,
    Order,
    OrderType,
    PlaceOrderRequest,
    TimeInForce,
)
from mftik.exchange.symbols import SymbolResolver, check_venue
from mftik.exchange.tickers import Category, UniversalTicker

logger = logging.getLogger(__name__)

TERMINAL = TERMINAL_STATUSES

#: ``params`` keys that would collide with a field the request already carries.
_RESERVED_PARAMS = (
    "symbol",
    "side",
    "type",
    "quantity",
    "price",
    "newClientOrderId",
)

#: Canonical time-in-force → Binance's spelling. ``POST_ONLY`` is absent on
#: purpose: Binance has no such value in this field and expresses it as the
#: ``LIMIT_MAKER`` order type instead, which :meth:`_order_shape` handles.
_TIF: dict[TimeInForce, str] = {
    TimeInForce.GTC: "GTC",
    TimeInForce.IOC: "IOC",
    TimeInForce.FOK: "FOK",
}

#: Binance's order types, as this connector emits them.
_MARKET = "MARKET"
_LIMIT = "LIMIT"
_LIMIT_MAKER = "LIMIT_MAKER"

#: Codes that mean "there is no such order", as opposed to a call that failed.
#: ``-2013`` is the query answer, ``-2011`` the cancel answer for the same
#: fact.
_NOT_FOUND_CODES = frozenset({-2011, -2013})


class BinanceSpotPrivateClient(BaseClient):
    """Binance spot trading account for TD.

    Symbols cross this boundary in canonical form (``BTCUSDT``) and are
    resolved to Binance's spelling on the wire; everything coming back is
    resolved home again. Venue-only order options ride in
    ``PlaceOrderRequest.params``.
    """

    name = "Binance"
    #: Binance's spot plane is a venue of its own here — one credential, one
    #: market — so every instrument this connector touches is on this category,
    #: and it builds the ticker rather than being told it. A unified-account
    #: venue could not, which is why the order path carries the ticker there.
    category = Category.SPOT

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        symbols: SymbolResolver,
        ws_url: str = BINANCE_SPOT_WS_API_URL,
        api: BinanceSpotWsApi | None = None,
        recv_window: int | None = None,
    ) -> None:
        super().__init__()
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret are required")
        self.api_key = api_key
        self.api = api or BinanceSpotWsApi(
            api_key=api_key,
            api_secret=api_secret,
            url=ws_url,
            recv_window=recv_window,
        )
        self.symbols = symbols
        # order_id / client_order_id → venue symbol. Binance needs the symbol
        # to cancel or query, but the shared interface addresses an order by
        # id alone.
        self._venue_symbols: dict[str, str] = {}

    # --- lifecycle ---------------------------------------------------------

    async def connect(self) -> None:
        await self.api.connect()
        self._connected = True
        logger.info("Binance connected key=%s…", self.api_key[:6])

    async def close(self) -> None:
        self._connected = False
        self._venue_symbols.clear()
        await self.api.close()

    def on_reconnect(self, callback) -> None:
        self.api.on_reconnect(callback)

    # --- order entry -------------------------------------------------------

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        self._ensure_connected()
        if request.type is OrderType.LIMIT and request.price is None:
            raise OrderError("limit order requires a price")

        extras = dict(request.params or {})
        # params comes from strategy code; a key that shadows a field the
        # request already carries would be a silent contradiction, so drop it
        # rather than let it win.
        for reserved in _RESERVED_PARAMS:
            if extras.pop(reserved, None) is not None:
                logger.warning(
                    "Binance ignoring params[%r]; set it on the request", reserved
                )

        order_type, tif = self._order_shape(request)
        ticker = request.ticker
        check_venue(ticker, self.name, {self.category})
        symbol = await self.symbols.exch_ticker(ticker)
        try:
            ack = await self.api.place_order(
                symbol=symbol,
                side=request.side,
                type=order_type,
                quantity=request.qty,
                price=request.price,
                time_in_force=extras.pop("timeInForce", tif),
                client_order_id=request.client_order_id,
                **extras,
            )
        except BinanceWsError as exc:
            # Surface as OrderError so TD publishes an order reject instead of
            # treating a venue rejection as a transport failure.
            raise OrderError(str(exc)) from exc
        # No lookup on this path: the ticker is the one we sent the order
        # under, so the reply needs no resolving and an ack that omits the
        # symbol has nothing to reconcile.
        order = ack.to_order(ticker)
        self._remember(order, symbol)
        return order

    def _order_shape(self, request: PlaceOrderRequest) -> tuple[str, str | None]:
        """``(Binance order type, timeInForce)`` for one request.

        The two are decided together because Binance couples them: a market
        order takes no ``timeInForce`` at all (it cannot rest, so there is
        nothing to say about how long it may), and ``LIMIT_MAKER`` — the only
        way to spell post-only — is refused if one is sent alongside it.
        """
        if request.type is OrderType.MARKET:
            return _MARKET, None
        if request.tif is TimeInForce.POST_ONLY:
            return _LIMIT_MAKER, None
        # A plain limit order must say how long it lives; Binance requires the
        # field and has no default.
        return _LIMIT, _TIF[request.tif] if request.tif else _TIF[TimeInForce.GTC]

    async def cancel_order(self, order_id: str) -> Order:
        self._ensure_connected()
        symbol = await self._symbol_for(order_id)
        return await self._cancel(symbol, order_id=order_id)

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        """Binance cancels by ``origClientOrderId`` as readily as by its own id."""
        self._ensure_connected()
        symbol = await self._symbol_for(client_order_id)
        return await self._cancel(symbol, client_order_id=client_order_id)

    async def _cancel(
        self,
        symbol: str,
        *,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> Order:
        try:
            ack = await self.api.cancel_order(
                symbol, order_id=order_id, client_order_id=client_order_id
            )
        except BinanceWsError as exc:
            raise OrderError(str(exc)) from exc
        return await self._inbound(ack)

    # --- recon reads -------------------------------------------------------

    async def fetch_order(self, order_id: str) -> Order:
        self._ensure_connected()
        symbol = await self._symbol_for(order_id)
        ack = await self.api.query_order(symbol, order_id=order_id)
        return await self._inbound(ack)

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, ticker: UniversalTicker | None = None
    ) -> Order | None:
        """Ask what happened to an order the push stream never reported.

        For an order we never got an ack for there is nothing cached to look
        the symbol up by — hence the ``symbol`` hint. ``None`` means Binance
        says no such order exists, which is an answer: the submit never landed.
        """
        self._ensure_connected()
        native = self._venue_symbols.get(client_order_id)
        if native is None and ticker is not None:
            native = await self.symbols.exch_ticker(ticker)
        if native is None:
            raise OrderError(
                f"cannot resolve Binance order {client_order_id!r} "
                "without its symbol"
            )
        try:
            ack = await self.api.query_order(native, client_order_id=client_order_id)
        except BinanceWsError as exc:
            if exc.code in _NOT_FOUND_CODES:
                return None
            raise OrderError(str(exc)) from exc
        return await self._inbound(ack)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        self._ensure_connected()
        native = await self._venue_symbol(symbol) if symbol else None
        acks = await self.api.fetch_open_orders(native)
        return [await self._inbound(ack) for ack in acks]

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        account = await self.api.fetch_account()
        return account.to_balances()

    # --- account streams ---------------------------------------------------

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
        stream = await self.api.subscribe_execution_reports()
        async for report in stream:
            # Also keeps the cancel path from having to ask for a symbol it
            # has already seen.
            ticker = await self._resolve(report.s)
            order = report.to_order(ticker)
            self._remember(order, report.s)
            yield order

    async def _fills(self) -> AsyncIterator[Fill]:
        """Executions, filtered out of the same stream the orders come from.

        A second subscription rather than a shared one: both views read the one
        ``userDataStream.subscribe`` underneath (see
        :class:`~mftik.exchange.binance.spot.client.BinanceSpotWsApi`), so this
        costs a fan-out branch rather than a second stream at the venue.
        """
        stream = await self.api.subscribe_execution_reports()
        async for report in stream:
            if not report.is_fill:
                continue
            yield report.to_fill(await self._resolve(report.s))

    async def _balances(self) -> AsyncIterator[Balance]:
        """One :class:`~mftik.exchange.models.Balance` per asset that moved.

        Binance batches the assets touched by one event into a single push;
        they are flattened here because the shared model states one asset.
        """
        stream = await self.api.subscribe_account_positions()
        async for position in stream:
            for balance in position.to_balances():
                yield balance

    # --- symbols -----------------------------------------------------------

    def _ticker(self, symbol: str) -> UniversalTicker:
        """The universal identity of a symbol on this connector's market."""
        return UniversalTicker.of(self.name, self.category, symbol)

    async def _venue_symbol(self, symbol: str) -> str:
        """Canonical → Binance's spelling, via the plane."""
        return await self.symbols.exch_ticker(self._ticker(symbol))

    async def _resolve(self, native_symbol: str) -> UniversalTicker:
        """Binance's spelling → the universal ticker, on this connector's book."""
        return await self.symbols.symbol_for(
            self.name, native_symbol, category=self.category
        )

    async def _symbol_for(self, order_id: str) -> str:
        """Resolve an id to its venue symbol, refreshing only if unseen."""
        native = self._venue_symbols.get(order_id)
        if native is not None:
            return native
        for ack in await self.api.fetch_open_orders():
            await self._inbound(ack)
        native = self._venue_symbols.get(order_id)
        if native is None:
            raise OrderError(f"no open Binance order for id {order_id!r}")
        return native

    async def _inbound(self, ack: BinanceOrderAck) -> Order:
        """One venue order reply, resolved home and indexed.

        The ticker is looked up *before* the conversion rather than patched on
        after it, so an :class:`~mftik.exchange.models.Order` never exists in a
        state where its identity is Binance's spelling of a symbol.
        """
        ticker = await self._resolve(ack.symbol)
        order = ack.to_order(ticker)
        self._remember(order, ack.symbol)
        return order

    def _remember(self, order: Order, native_symbol: str) -> None:
        """Index an order's venue symbol by every id it can be addressed with."""
        keys = [order.order_id]
        if order.client_order_id:
            keys.append(order.client_order_id)
        if order.status in TERMINAL:
            for key in keys:
                self._venue_symbols.pop(key, None)
            return
        for key in keys:
            if key:
                self._venue_symbols[key] = native_symbol


__all__ = ["BinanceSpotPrivateClient"]
