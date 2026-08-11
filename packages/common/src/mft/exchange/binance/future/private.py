"""The Binance USDⓈ-M futures trading connector.

Not an implementation of a shared trading interface — there is none; see
:mod:`mft.exchange.base`. It resembles the spot connector because the same
questions get asked of every venue, not because anything enforces it.

Composes three transports, where spot needs one:

* :class:`~mft.exchange.binance.future.client.BinanceFutureWsApi` — order
  entry, order queries, balances and positions. One authenticated connection,
  no per-call signature.
* :class:`~mft.exchange.binance.future.user.BinanceFutureUserStream` — what
  happened. Order updates and account updates arrive here or nowhere, on a
  socket opened with a listen key the WebSocket API hands out.
* :class:`~mft.exchange.binance.future.rest.BinanceFutureRest` — one read:
  "what is open right now". Futures has no ``openOrders.status``, and recon
  cannot start without the answer.

Four futures shapes drive most of what follows:

* **Post-only is a time-in-force**, spelled ``GTX``. Unlike spot, no order type
  has to be swapped for it — ``LIMIT_MAKER`` does not exist here.
* **Positions exist**, which on spot they do not: a perp account holds signed
  exposure that no balance describes. So this connector serves
  ``fetch_positions`` and ``stream_positions``, which TD picks up by name.
* **A balance has no free/locked split.** Margin is held against the position,
  not against an order, so ``free``/``locked`` are derived from what Binance
  says is still available — see
  :meth:`~mft.exchange.binance.future.models.BinanceFutureBalance.to_balance`.
* **One-way mode is assumed.** Every order goes out without a
  ``positionSide``, which Binance reads as ``BOTH``. A hedge-mode account
  requires ``LONG``/``SHORT`` on every order and would have two positions per
  instrument — which the OMS, keyed by ticker, has nowhere to put. A caller on
  such an account can still pass ``positionSide`` through
  ``PlaceOrderRequest.params``; the position half would need work this adapter
  has not done.

Symbols are translated through a :class:`~mft.exchange.symbols.SymbolResolver`
(the symbol plane), never by string surgery — see that module for why.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from decimal import Decimal

from mft.exchange.base import BaseClient
from mft.exchange.binance.future.client import BinanceFutureWsApi
from mft.exchange.binance.future.models import BinanceFutureOrderAck
from mft.exchange.binance.future.protocol import (
    BINANCE_FUTURE_PRIVATE_STREAM_URL,
    BINANCE_FUTURE_REST_URL,
    BINANCE_FUTURE_WS_API_URL,
    BinanceWsError,
)
from mft.exchange.binance.future.rest import BinanceFutureRest
from mft.exchange.binance.future.user import BinanceFutureUserStream
from mft.exchange.errors import ExchangeError, OrderError
from mft.exchange.models import (
    TERMINAL_STATUSES,
    Balance,
    Fill,
    Order,
    OrderType,
    PlaceOrderRequest,
    TimeInForce,
)
from mft.exchange.oms import Position
from mft.exchange.symbols import SymbolResolver, check_venue
from mft.exchange.tickers import Category, UniversalTicker

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

#: Canonical time-in-force → Binance futures' spelling. All four exist here,
#: post-only included: ``GTX`` is "good till crossing", which is refused rather
#: than filled if it would take. Spot has no such value and expresses the same
#: intent as the ``LIMIT_MAKER`` order type.
_TIF: dict[TimeInForce, str] = {
    TimeInForce.GTC: "GTC",
    TimeInForce.IOC: "IOC",
    TimeInForce.FOK: "FOK",
    TimeInForce.POST_ONLY: "GTX",
}

#: Binance's order types, as this connector emits them.
_MARKET = "MARKET"
_LIMIT = "LIMIT"

#: Codes that mean "there is no such order", as opposed to a call that failed.
#: ``-2013`` is the query answer, ``-2011`` the cancel answer for the same
#: fact.
_NOT_FOUND_CODES = frozenset({-2011, -2013})


class BinanceFuturePrivateClient(BaseClient):
    """Binance USDⓈ-M futures trading account for TD.

    Symbols cross this boundary in canonical form (``BTCUSDT``) and are
    resolved to Binance's spelling on the wire; everything coming back is
    resolved home again. Venue-only order options — ``reduceOnly``,
    ``positionSide``, ``priceMatch`` — ride in ``PlaceOrderRequest.params``.
    """

    name = "BinanceFuture"
    #: This connector's market. Binance's USDⓈ-M plane is a venue of its own
    #: here — one credential, one book — so every instrument it touches is a
    #: perpetual and it builds the ticker rather than being told it.
    category = Category.PERP

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        symbols: SymbolResolver,
        ws_url: str = BINANCE_FUTURE_WS_API_URL,
        user_stream_url: str = BINANCE_FUTURE_PRIVATE_STREAM_URL,
        rest_url: str = BINANCE_FUTURE_REST_URL,
        api: BinanceFutureWsApi | None = None,
        user: BinanceFutureUserStream | None = None,
        rest: BinanceFutureRest | None = None,
        recv_window: int | None = None,
    ) -> None:
        super().__init__()
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret are required")
        self.api_key = api_key
        self.api = api or BinanceFutureWsApi(
            api_key=api_key,
            api_secret=api_secret,
            url=ws_url,
            recv_window=recv_window,
        )
        # The user stream is built from the API's own methods rather than from
        # the credential: the listen key is issued over the authenticated
        # WebSocket API this client already holds, and a second logon would buy
        # nothing but another connection to lose.
        self.user = user or BinanceFutureUserStream(
            start_key=self.api.start_user_stream,
            ping_key=self.api.ping_user_stream,
            base_url=user_stream_url,
        )
        self.rest = rest or BinanceFutureRest(
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

    # --- lifecycle ---------------------------------------------------------

    async def connect(self) -> None:
        await self.api.connect()
        try:
            # After the API socket, never before: the listen key it opens with
            # is issued on that connection.
            await self.user.connect()
            await self.rest.connect()
        except Exception:
            # Half a connector is not a usable one: an order path with no
            # report path would place orders it could never hear about.
            await self.user.close()
            await self.api.close()
            raise
        self._connected = True
        logger.info("BinanceFuture connected key=%s…", self.api_key[:6])

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
        if request.type is OrderType.LIMIT and request.price is None:
            raise OrderError("limit order requires a price")

        extras = dict(request.params or {})
        # params comes from strategy code; a key that shadows a field the
        # request already carries would be a silent contradiction, so drop it
        # rather than let it win.
        for reserved in _RESERVED_PARAMS:
            if extras.pop(reserved, None) is not None:
                logger.warning(
                    "BinanceFuture ignoring params[%r]; set it on the request",
                    reserved,
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

        Simpler than spot's, which has to turn post-only into an order type: on
        futures every time-in-force this platform knows has a spelling, so the
        type is decided by ``request.type`` alone. A market order still gets
        none — it cannot rest, so there is nothing to say about how long it
        may, and Binance refuses the field alongside it.
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
                f"cannot resolve BinanceFuture order {client_order_id!r} "
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
        """Resting orders, over REST — the one read with no WebSocket method.

        A ``BinanceFutureRestError`` is left as it is rather than wrapped:
        it carries the same numeric code the socket errors do, which is what
        TD normalizes on.
        """
        self._ensure_connected()
        native = await self._venue_symbol(symbol) if symbol else None
        acks = await self.rest.fetch_open_orders(native)
        return [await self._inbound(ack) for ack in acks]

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        rows = await self.api.fetch_balances()
        return [row.to_balance() for row in rows]

    async def fetch_positions(self) -> list[Position]:
        """Open contracts on the account — the thing spot has no equivalent of.

        Every row Binance returns is kept, flat ones included: a position that
        has just been closed comes back at size zero, and that is how the OMS
        learns to drop it rather than carrying a stale one forever.
        """
        self._ensure_connected()
        rows = await self.api.fetch_positions()
        out: list[Position] = []
        for row in rows:
            ticker = await self._resolve(row.symbol)
            out.append(row.to_position(ticker))
        return out

    async def fetch_leverage(self, ticker: UniversalTicker) -> Decimal:
        """This account's configured leverage for ``ticker``.

        Uses REST ``symbolConfig`` rather than ``positionRisk`` / WebSocket
        ``account.position``: those only answer for symbols that already have
        a position or resting order, and leverage is needed before the first
        order goes out.
        """
        self._ensure_connected()
        check_venue(ticker, self.name, {self.category})
        native = await self.symbols.exch_ticker(ticker)
        rows = await self.rest.fetch_symbol_config(native)
        for row in rows:
            if row.symbol != native:
                continue
            if row.leverage is None or row.leverage <= 0:
                raise ExchangeError(
                    f"BinanceFuture symbolConfig for {native} has no leverage"
                )
            return row.leverage
        raise ExchangeError(
            f"BinanceFuture symbolConfig returned no row for {native}"
        )

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
                # Also keeps the cancel path from having to ask for a symbol it
                # has already seen.
                ticker = await self._resolve(update.symbol)
                order = update.to_order(ticker)
                self._remember(order, update.symbol)
                yield order
        finally:
            stream.close()

    async def _fills(self) -> AsyncIterator[Fill]:
        """Executions, filtered out of the same stream the orders come from.

        A second view rather than a shared one: both read the one socket the
        listen key opened, so this costs a fan-out branch rather than another
        connection.
        """
        stream = await self.user.subscribe_order_updates()
        try:
            async for update in stream:
                if not update.is_fill:
                    continue
                yield update.to_fill(await self._resolve(update.symbol))
        finally:
            stream.close()

    async def _balances(self) -> AsyncIterator[Balance]:
        """One :class:`~mft.exchange.models.Balance` per asset that moved.

        Binance batches the assets touched by one event into a single
        ``ACCOUNT_UPDATE``; they are flattened here because the shared model
        states one asset. Funding payments arrive on this path too — they move
        the wallet without any order doing anything, which is exactly the case
        a balance feed exists for.
        """
        stream = await self.user.subscribe_account_updates()
        try:
            async for update in stream:
                for balance in update.to_balances():
                    yield balance
        finally:
            stream.close()

    async def _positions(self) -> AsyncIterator[Position]:
        """Position changes, off the same ``ACCOUNT_UPDATE``.

        Binance pushes the whole position when any part of it moves — size,
        entry price or unrealised pnl — so each row can be taken as that
        instrument's new truth rather than a delta to apply. A closed position
        arrives at size zero, which is how the OMS learns to drop it.
        """
        stream = await self.user.subscribe_account_updates()
        try:
            async for update in stream:
                for row in update.position_rows():
                    ticker = await self._resolve(row.symbol)
                    yield row.to_position(ticker)
        finally:
            stream.close()

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
        for ack in await self.rest.fetch_open_orders():
            await self._inbound(ack)
        native = self._venue_symbols.get(order_id)
        if native is None:
            raise OrderError(f"no open BinanceFuture order for id {order_id!r}")
        return native

    async def _inbound(self, ack: BinanceFutureOrderAck) -> Order:
        """One venue order reply, resolved home and indexed.

        The ticker is looked up *before* the conversion rather than patched on
        after it, so an :class:`~mft.exchange.models.Order` never exists in a
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


__all__ = ["BinanceFuturePrivateClient"]
