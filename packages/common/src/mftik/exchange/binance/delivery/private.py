"""The Binance COIN-M trading connector.

Not an implementation of a shared trading interface — there is none; see
:mod:`mftik.exchange.base`. It resembles the USD-M connector because the same
questions get asked of every venue, not because anything is imported from
:mod:`mftik.exchange.binance.future`.

Composes three transports:

* :class:`~mftik.exchange.binance.delivery.client.BinanceDeliveryWsApi` —
  order entry, order queries, balances and positions. One authenticated
  connection, no per-call signature after ``session.logon``.
* :class:`~mftik.exchange.binance.delivery.user.BinanceDeliveryUserStream` —
  what happened. Order updates and account updates arrive here or nowhere,
  on a socket opened with a listen key the WebSocket API hands out.
* :class:`~mftik.exchange.binance.delivery.rest.BinanceDeliveryRest` — one
  read: "what is open right now". dapi has no ``openOrders.status``.

Four shapes drive most of what follows:

* **Post-only is a time-in-force**, spelled ``GTX``.
* **Quantity is a contract count.** ``contractSize`` is USD per contract;
  multiplying by it invents a dollar notional, not BTC.
* **A balance has no free/locked split.** ``free`` is ``availableBalance``
  (or older ``withdrawAvailable``); ``locked`` is the rest of ``balance``.
* **One-way mode is assumed.** Every order goes out without a
  ``positionSide``, which Binance reads as ``BOTH``.

Symbols are translated through a :class:`~mftik.exchange.symbols.SymbolResolver`
(the symbol plane), never by string surgery.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

from mftik.exchange.base import BaseClient
from mftik.exchange.binance.delivery.client import BinanceDeliveryWsApi
from mftik.exchange.binance.delivery.models import BinanceDeliveryOrderAck
from mftik.exchange.binance.delivery.protocol import (
    BINANCE_DELIVERY_PRIVATE_STREAM_URL,
    BINANCE_DELIVERY_REST_URL,
    BINANCE_DELIVERY_WS_API_URL,
    BinanceWsError,
)
from mftik.exchange.binance.delivery.rest import BinanceDeliveryRest
from mftik.exchange.binance.delivery.user import BinanceDeliveryUserStream
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
from mftik.exchange.oms import Position
from mftik.exchange.order_check import require_legal
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
    "reduceOnly",
)

_TIF: dict[TimeInForce, str] = {
    TimeInForce.GTC: "GTC",
    TimeInForce.IOC: "IOC",
    TimeInForce.FOK: "FOK",
    TimeInForce.POST_ONLY: "GTX",
}

_MARKET = "MARKET"
_LIMIT = "LIMIT"

#: ``-2013`` is the query answer, ``-2011`` the cancel answer for the same fact.
_NOT_FOUND_CODES = frozenset({-2011, -2013})

#: Books this connector trades. Same set as ``venues.BINANCE_CM``.
#: Inverse first so the common inbound row hits on the first lookup.
_BOOKS = (Category.INVERSE, Category.FUTURE)


class BinanceDeliveryPrivateClient(BaseClient):
    """Binance COIN-M trading account for TD.

    Symbols cross this boundary in canonical form (``BTCUSD``,
    ``BTCUSD260925``) and are resolved to Binance's spelling
    (``BTCUSD_PERP``, ``BTCUSD_260925``) on the wire; everything coming
    back is resolved home again. ``quantity`` stays in contracts.
    """

    name = "BinanceCM"
    #: The default book, used when a caller names a bare symbol. The venue
    #: trades Inverse and dated Future on this one credential — incoming
    #: rows are resolved by looking up the native spelling on each book,
    #: not by assuming this one.
    category = Category.INVERSE

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        symbols: SymbolResolver,
        ws_url: str = BINANCE_DELIVERY_WS_API_URL,
        user_stream_url: str = BINANCE_DELIVERY_PRIVATE_STREAM_URL,
        rest_url: str = BINANCE_DELIVERY_REST_URL,
        api: BinanceDeliveryWsApi | None = None,
        user: BinanceDeliveryUserStream | None = None,
        rest: BinanceDeliveryRest | None = None,
        recv_window: int | None = None,
    ) -> None:
        super().__init__()
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret are required")
        self.api_key = api_key
        self.api = api or BinanceDeliveryWsApi(
            api_key=api_key,
            api_secret=api_secret,
            url=ws_url,
            recv_window=recv_window,
        )
        self.user = user or BinanceDeliveryUserStream(
            start_key=self.api.start_user_stream,
            ping_key=self.api.ping_user_stream,
            base_url=user_stream_url,
        )
        self.rest = rest or BinanceDeliveryRest(
            api_key=api_key,
            api_secret=api_secret,
            base_url=rest_url,
            recv_window=recv_window,
        )
        self.symbols = symbols
        # order_id / client_order_id → venue symbol. Binance needs the symbol
        # to cancel or query, but the shared interface addresses an order by
        # id alone.
        self._venue_symbols: dict[str, str] = {}
        # Venue symbols the plane does not carry, so the warning is logged
        # once per contract rather than once per stream update.
        self._unlisted: set[str] = set()

    # --- lifecycle ---------------------------------------------------------

    async def connect(self) -> None:
        await self.api.connect()
        try:
            await self.user.connect()
            await self.rest.connect()
        except Exception:
            await self.user.close()
            await self.api.close()
            raise
        self._connected = True
        logger.info("BinanceCM connected key=%s…", self.api_key[:6])

    async def close(self) -> None:
        self._connected = False
        self._venue_symbols.clear()
        await self.user.close()
        await self.api.close()
        await self.rest.close()

    def on_reconnect(self, callback) -> None:
        """Hear about a reconnect on either socket.

        Both, because either one dropping means a gap: the API socket losing
        its logon and the user stream losing its listen key both leave TD's
        view of the account older than the account.
        """
        self.api.on_reconnect(callback)
        self.user.on_reconnect(callback)

    # --- order entry -------------------------------------------------------

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        self._ensure_connected()
        require_legal(request)

        extras = dict(request.params or {})
        for reserved in _RESERVED_PARAMS:
            if extras.pop(reserved, None) is not None:
                logger.warning(
                    "BinanceCM ignoring params[%r]; set it on the request",
                    reserved,
                )

        order_type, tif = self._order_shape(request)
        ticker = request.ticker
        check_venue(ticker, self.name, _BOOKS)
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
                # None rather than False when it is off, so an ordinary order
                # goes out exactly as it did before this flag existed.
                reduce_only=request.reduce_only or None,
                **extras,
            )
        except BinanceWsError as exc:
            raise OrderError(str(exc)) from exc
        order = ack.to_order(ticker)
        self._remember(order, symbol)
        return order

    def _order_shape(self, request: PlaceOrderRequest) -> tuple[str, str | None]:
        """``(Binance order type, timeInForce)`` for one request.

        A market order gets no TIF — it cannot rest, and Binance refuses the
        field alongside it. Post-only is ``GTX``.
        """
        if request.type is OrderType.MARKET:
            return _MARKET, None
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
        the symbol up by — hence the ``ticker`` hint. ``None`` means Binance
        says no such order exists, which is an answer: the submit never landed.
        """
        self._ensure_connected()
        native = self._venue_symbols.get(client_order_id)
        if native is None and ticker is not None:
            native = await self.symbols.exch_ticker(ticker)
        if native is None:
            raise OrderError(
                f"cannot resolve BinanceCM order {client_order_id!r} "
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
        """Resting orders, over REST — the one read with no WebSocket method."""
        self._ensure_connected()
        native = await self._venue_symbol(symbol) if symbol else None
        acks = await self.rest.fetch_open_orders(native)
        orders = [await self._inbound_or_skip(ack) for ack in acks]
        return [order for order in orders if order is not None]

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        rows = await self.api.fetch_balances()
        return [row.to_balance() for row in rows]

    async def fetch_positions(self) -> list[Position]:
        """Open contracts on the account.

        Every row Binance returns is kept, flat ones included: a position that
        has just been closed comes back at size zero, and that is how the OMS
        learns to drop it rather than carrying a stale one forever.
        """
        self._ensure_connected()
        rows = await self.api.fetch_positions()
        out: list[Position] = []
        for row in rows:
            ticker = await self._resolve_or_skip(row.symbol)
            if ticker is None:
                continue
            out.append(row.to_position(ticker))
        return out

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

    def stream_positions(self) -> AsyncIterator[Position]:
        self._ensure_connected()
        return self._positions()

    async def _orders(self) -> AsyncIterator[Order]:
        stream = await self.user.subscribe_order_updates()
        try:
            async for update in stream:
                ticker = await self._resolve_or_skip(update.symbol)
                if ticker is None:
                    continue
                order = update.to_order(ticker)
                self._remember(order, update.symbol)
                yield order
        finally:
            stream.close()

    async def _fills(self) -> AsyncIterator[Fill]:
        stream = await self.user.subscribe_order_updates()
        try:
            async for update in stream:
                if not update.is_fill:
                    continue
                ticker = await self._resolve_or_skip(update.symbol)
                if ticker is None:
                    continue
                yield update.to_fill(ticker)
        finally:
            stream.close()

    async def _balances(self) -> AsyncIterator[Balance]:
        stream = await self.user.subscribe_account_updates()
        try:
            async for update in stream:
                for balance in update.to_balances():
                    yield balance
        finally:
            stream.close()

    async def _positions(self) -> AsyncIterator[Position]:
        stream = await self.user.subscribe_account_updates()
        try:
            async for update in stream:
                for row in update.position_rows():
                    ticker = await self._resolve_or_skip(row.symbol)
                    if ticker is None:
                        continue
                    yield row.to_position(ticker)
        finally:
            stream.close()

    # --- symbols -----------------------------------------------------------

    def _ticker(
        self, symbol: str, category: Category | None = None
    ) -> UniversalTicker:
        return UniversalTicker.of(self.name, category or self.category, symbol)

    async def _venue_symbol(self, symbol: str) -> str:
        """Canonical → Binance's spelling, via the plane.

        Tries each book: ``BTCUSD`` is the inverse perpetual,
        ``BTCUSD260925`` the dated future. They are not interchangeable.
        """
        from mftik.symbols import SymbolNotFoundError

        last: SymbolNotFoundError | None = None
        for category in _BOOKS:
            try:
                return await self.symbols.exch_ticker(
                    self._ticker(symbol, category)
                )
            except SymbolNotFoundError as exc:
                last = exc
        assert last is not None
        raise last

    async def _resolve(self, native_symbol: str) -> UniversalTicker:
        """Binance's spelling → the universal ticker.

        Looked up on each book this venue trades, rather than derived:
        stripping a dated suffix would land a quarterly fill on the
        inverse perpetual of the same pair.
        """
        from mftik.symbols import SymbolNotFoundError

        last: SymbolNotFoundError | None = None
        for category in _BOOKS:
            try:
                return await self.symbols.symbol_for(
                    self.name, native_symbol, category=category
                )
            except SymbolNotFoundError as exc:
                last = exc
        assert last is not None
        raise last

    async def _resolve_or_skip(self, native_symbol: str) -> UniversalTicker | None:
        """The canonical ticker, or ``None`` for a contract we do not carry.

        dapi answers for every contract listed on the venue. Dated ones
        resolve once the plane carries them; a row the plane has not
        ingested yet must not abort an account-wide read or tear down a
        stream that is also carrying the instruments we do trade. Reads
        that were asked about one specific order still use
        :meth:`_resolve` and raise: there, answering with nothing would
        be the worse lie.
        """
        # Imported here, not at module scope: the exchange barrel is what
        # ``mftik.symbols`` itself loads, so naming it up top is a cycle.
        from mftik.symbols import SymbolNotFoundError

        try:
            return await self._resolve(native_symbol)
        except SymbolNotFoundError:
            if native_symbol not in self._unlisted:
                self._unlisted.add(native_symbol)
                logger.warning(
                    "BinanceCM symbol plane does not carry %s — "
                    "skipping its rows",
                    native_symbol,
                )
            return None

    async def _symbol_for(self, order_id: str) -> str:
        native = self._venue_symbols.get(order_id)
        if native is not None:
            return native
        for ack in await self.rest.fetch_open_orders():
            await self._inbound_or_skip(ack)
        native = self._venue_symbols.get(order_id)
        if native is None:
            raise OrderError(f"no open BinanceCM order for id {order_id!r}")
        return native

    async def _inbound(self, ack: BinanceDeliveryOrderAck) -> Order:
        ticker = await self._resolve(ack.symbol)
        order = ack.to_order(ticker)
        self._remember(order, ack.symbol)
        return order

    async def _inbound_or_skip(
        self, ack: BinanceDeliveryOrderAck
    ) -> Order | None:
        """:meth:`_inbound` for a listing read, where one bad row is not fatal."""
        ticker = await self._resolve_or_skip(ack.symbol)
        if ticker is None:
            return None
        order = ack.to_order(ticker)
        self._remember(order, ack.symbol)
        return order

    def _remember(self, order: Order, native_symbol: str) -> None:
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


__all__ = ["BinanceDeliveryPrivateClient"]
