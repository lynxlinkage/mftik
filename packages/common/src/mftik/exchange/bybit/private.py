"""The Bybit trading connector.

Not an implementation of a shared trading interface — there is none; see
:mod:`mftik.exchange.base`. It resembles Gate's and Binance's connectors because
the same questions get asked of every venue, not because anything enforces it.

Composes all three of Bybit's transports, because the venue splits the job
three ways and no two of them substitute for each other:

* :class:`~mftik.exchange.bybit.trade.BybitTradeSocket` — order entry. Fast, and
  it answers with two ids and no state.
* :class:`~mftik.exchange.bybit.account.BybitPrivateStream` — what happened.
  Order updates and executions arrive here or nowhere.
* :class:`~mftik.exchange.bybit.rest.BybitRest` — recon. "What is open" and
  "what is the balance" exist only over REST on this venue.

Five Bybit shapes drive most of what follows:

* **A spot market buy sizes in the quote currency by default.** ``qty=0.5`` on
  BTCUSDT means half a dollar unless ``marketUnit`` says otherwise. The shared
  model sizes in base (``qty``) or quote (``quote_qty``), so this connector
  sends ``marketUnit: "baseCoin"`` or ``"quoteCoin"`` to match — the single
  most expensive thing to get wrong here.
* **Post-only is a time-in-force**, spelled ``PostOnly``. Unlike Binance, no
  order type has to be swapped for it.
* **The order ack carries no status.** Bybit acknowledges receipt with an id;
  the ``order`` topic says what became of it. So :meth:`place_order` reports
  ``PENDING_NEW`` rather than inventing a state the venue did not report.
* **Fills are their own topic**, with their own fee field, and rows on it that
  are not fills at all (funding, ADL).
* **The category is not on the order.** One credential trades every book, and
  which book an order is for is a parameter — so ``category`` says which book
  this connector *places orders on*.

  It does **not** narrow what the connector reports. A unified account is one
  account: its perp fills and its spot fills move the same wallet, and a
  session that hid half of them would be reporting a balance sheet it could
  not explain. So the private stream subscribes unscoped and every account row
  is resolved on the book it names in its own ``category`` field — which is
  also what lets positions reach a session whose order path is on spot, since
  only the contract books have any.

Symbols are translated through a :class:`~mftik.exchange.symbols.SymbolResolver`
(the symbol plane), never by string surgery — see that module for why.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from decimal import Decimal

from mftik.exchange.base import BaseClient
from mftik.exchange.bybit.account import BybitPrivateStream
from mftik.exchange.bybit.models import BybitOrderUpdate, category_of
from mftik.exchange.bybit.protocol import (
    BYBIT_REST_URL,
    BYBIT_WS_PRIVATE_URL,
    BYBIT_WS_TRADE_URL,
    DEFAULT_RECV_WINDOW_MS,
    LINEAR,
    SPOT,
    BybitError,
    product_of,
)
from mftik.exchange.bybit.rest import UNIFIED, BybitRest
from mftik.exchange.bybit.trade import BybitTradeSocket
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
from mftik.exchange.oms import Position
from mftik.exchange.order_check import require_legal, sized_amount
from mftik.exchange.symbols import SymbolResolver, check_venue
from mftik.exchange.tickers import Category, UniversalTicker

logger = logging.getLogger(__name__)

TERMINAL = TERMINAL_STATUSES

#: ``params`` keys that would collide with a field the request already carries.
_RESERVED_PARAMS = (
    "category",
    "symbol",
    "side",
    "orderType",
    "qty",
    "price",
    "orderLinkId",
    # A request field since reduce_only stopped being a venue-only option.
    # Left open, a params key could contradict the field and win, which on
    # this flag is the difference between closing a position and opening the
    # opposite one.
    "reduceOnly",
)

#: Canonical time-in-force → Bybit's spelling. All four exist on this venue,
#: post-only included, so nothing has to be expressed as an order type here.
_TIF: dict[TimeInForce, str] = {
    TimeInForce.GTC: "GTC",
    TimeInForce.IOC: "IOC",
    TimeInForce.FOK: "FOK",
    TimeInForce.POST_ONLY: "PostOnly",
}

#: Bybit's order types, as this connector emits them.
_MARKET = "Market"
_LIMIT = "Limit"

#: What ``qty`` is denominated in on a spot market order. Bybit's default for a
#: market **buy** is the quote currency; the shared model always means base
#: unless ``quote_qty`` asked for the other unit.
_BASE_UNIT = "baseCoin"
_QUOTE_UNIT = "quoteCoin"


class BybitPrivateClient(BaseClient):
    """Bybit trading account for TD, on one of the venue's books.

    Symbols cross this boundary in canonical form (``BTCUSDT``) and are
    resolved to Bybit's spelling on the wire; everything coming back is
    resolved home again. Venue-only order options ride in
    ``PlaceOrderRequest.params``.

    ``category`` is only a **default**, for a caller that builds a request
    without naming a book. Every order says which instrument it is for, so one
    connector places on spot and perp alike — which is what a unified account
    is, and why a second connector for the second book would be two views of
    the same money rather than two accounts.
    """

    name = "Bybit"

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        symbols: SymbolResolver,
        category: Category = Category.SPOT,
        trade_url: str = BYBIT_WS_TRADE_URL,
        private_url: str = BYBIT_WS_PRIVATE_URL,
        rest_url: str = BYBIT_REST_URL,
        account_type: str = UNIFIED,
        recv_window: int = DEFAULT_RECV_WINDOW_MS,
        trade: BybitTradeSocket | None = None,
        stream: BybitPrivateStream | None = None,
        rest: BybitRest | None = None,
    ) -> None:
        super().__init__()
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret are required")
        self.api_key = api_key
        #: The book this connector trades, in the platform's vocabulary.
        self.category = category
        #: The same book in Bybit's — what goes on the wire.
        self.product = product_of(category)
        self.symbols = symbols
        self.account_type = account_type
        self.trade = trade or BybitTradeSocket(
            api_key=api_key,
            api_secret=api_secret,
            url=trade_url,
            recv_window=recv_window,
        )
        self.stream = stream or BybitPrivateStream(
            api_key=api_key,
            api_secret=api_secret,
            url=private_url,
            # Unscoped on purpose: one credential is one account, and the
            # session reports all of it. See the module docstring.
            product=None,
        )
        self.rest = rest or BybitRest(
            api_key=api_key,
            api_secret=api_secret,
            base_url=rest_url,
            recv_window=recv_window,
        )
        # order_id / client_order_id → venue symbol. Bybit needs the symbol to
        # cancel or query, but the shared interface addresses an order by id
        # alone.
        self._venue_symbols: dict[str, str] = {}
        # The same keys → the last state the venue reported for that order.
        # Bybit's acks carry no state, so this is what a cancel has to answer
        # with. Both maps are pruned together when an order finishes, so
        # neither grows with a long-running session.
        self._last: dict[str, Order] = {}
        # order_id / client_order_id → whether we sized this order in quote.
        # Bybit's ``qty`` is then quote, and must not become Order.qty.
        self._quote_sized: dict[str, bool] = {}

    # --- lifecycle ---------------------------------------------------------

    async def connect(self) -> None:
        await self.trade.connect()
        try:
            await self.stream.connect()
            await self.rest.connect()
        except Exception:
            # Half a connector is not a usable one: an order path with no
            # report path would place orders it could never hear about.
            await self.trade.close()
            await self.stream.close()
            raise
        self._connected = True
        logger.info(
            "Bybit connected key=%s… category=%s", self.api_key[:6], self.product
        )

    async def close(self) -> None:
        self._connected = False
        self._venue_symbols.clear()
        self._quote_sized.clear()
        await self.trade.close()
        await self.stream.close()
        await self.rest.close()

    def on_reconnect(self, callback) -> None:
        """Hear about a reconnect on either socket.

        Both, because either one dropping means a gap: the trade socket losing
        its authentication and the private stream losing its subscriptions both
        leave TD's view of the account older than the account.
        """
        self.trade.on_reconnect(callback)
        self.stream.on_reconnect(callback)

    # --- order entry -------------------------------------------------------

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        self._ensure_connected()
        require_legal(request)

        extras = dict(request.params or {})
        # params comes from strategy code; a key that shadows a field the
        # request already carries would be a silent contradiction, so drop it
        # rather than let it win.
        for reserved in _RESERVED_PARAMS:
            if extras.pop(reserved, None) is not None:
                logger.warning(
                    "Bybit ignoring params[%r]; set it on the request", reserved
                )

        ticker = request.ticker
        check_venue(ticker, self.name)
        # The order's own book, not the connector's: one credential trades
        # both, and the ticker is what says which. ``category`` is only the
        # default for a caller that names none.
        product = product_of(ticker.category)
        symbol = await self.symbols.exch_ticker(ticker)
        order_type = _MARKET if request.type is OrderType.MARKET else _LIMIT
        try:
            ack = await self.trade.place_order(
                category=product,
                symbol=symbol,
                side=request.side,
                order_type=order_type,
                qty=sized_amount(request),
                price=request.price,
                time_in_force=extras.pop("timeInForce", self._tif(request)),
                order_link_id=request.client_order_id,
                market_unit=extras.pop(
                    "marketUnit", self._market_unit(request, product)
                ),
                # None rather than False when off, so an ordinary order goes
                # out exactly as it did before this flag existed.
                reduce_only=request.reduce_only or None,
                **extras,
            )
        except BybitError as exc:
            # Surface as OrderError so TD publishes an order reject instead of
            # treating a venue rejection as a transport failure.
            raise OrderError(str(exc)) from exc

        order = Order(
            universal_ticker=str(ticker),
            order_id=ack.order_id,
            client_order_id=ack.client_order_id or request.client_order_id,
            side=request.side,
            type=request.type,
            # The ack proves the venue took the order, not what became of it —
            # Bybit says that on the ``order`` topic, which is milliseconds
            # away. Claiming NEW here would claim the order is resting, which
            # is exactly what an order that filled on arrival is not.
            status=OrderStatus.PENDING_NEW,
            qty=request.qty if request.qty is not None else Decimal("0"),
            quote_qty=request.quote_qty,
            price=request.price,
        )
        # Indexed here rather than on the first push: a cancel can arrive
        # before the ``order`` topic has said anything, and it needs the
        # venue symbol to go out at all.
        self._remember(order, symbol)
        return order

    def _tif(self, request: PlaceOrderRequest) -> str | None:
        """Bybit's spelling of the request's time-in-force.

        A market order gets none: it cannot rest, so there is nothing to say
        about how long it may, and Bybit fills in IOC itself.
        """
        if request.type is OrderType.MARKET:
            return None
        return _TIF[request.tif] if request.tif else _TIF[TimeInForce.GTC]

    def _market_unit(
        self, request: PlaceOrderRequest, product: str
    ) -> str | None:
        """``baseCoin`` on a spot market order, and nothing anywhere else.

        Bybit reads ``qty`` on a spot market **buy** as an amount of quote
        currency unless told otherwise, so an order for 0.5 BTC would spend 50
        cents. It is sent on market sells too — where the default already is
        base — so that one code path covers both sides and neither depends on
        remembering which one the default was wrong for. The contract books
        have no such parameter and refuse it.
        """
        if product != SPOT or request.type is not OrderType.MARKET:
            return None
        return _QUOTE_UNIT if request.quote_qty is not None else _BASE_UNIT

    async def cancel_order(self, order_id: str) -> Order:
        self._ensure_connected()
        known = await self._known(order_id)
        return await self._cancel(known, order_id=order_id)

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        """Bybit cancels by ``orderLinkId`` as readily as by its own id."""
        self._ensure_connected()
        known = await self._known(client_order_id)
        return await self._cancel(known, client_order_id=client_order_id)

    async def _cancel(
        self,
        known: Order,
        *,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> Order:
        """Cancel one order, on the book it was placed on.

        ``known`` is the last state the venue reported for it, which is where
        both the venue symbol and the category come from: a cancel needs each,
        and an order id alone carries neither.
        """
        try:
            ack = await self.trade.cancel_order(
                category=product_of(known.category),
                symbol=self._venue_symbols[order_id or client_order_id or ""],
                order_id=order_id,
                order_link_id=client_order_id,
            )
        except BybitError as exc:
            raise OrderError(str(exc)) from exc
        # PENDING_CANCEL rather than CANCELED because that is what is true: the
        # cancel is in, and the venue has not yet said whether it won the race
        # against a fill. The ``order`` topic settles it either way — Bybit's
        # cancel ack carries two ids and no state.
        _ = ack
        return known.model_copy(update={"status": OrderStatus.PENDING_CANCEL})

    async def _known(self, key: str) -> Order:
        """The last state the venue reported for one order, refreshed if unseen.

        Both cancel paths need it: Bybit addresses an order by symbol *and*
        category, and an id on its own says neither. An order this connector
        never saw — recon after a restart, or one placed elsewhere — is looked
        up across the account's books.
        """
        found = self._last.get(key)
        if found is not None:
            return found
        await self.fetch_open_orders()
        found = self._last.get(key)
        if found is None:
            raise OrderError(f"no open Bybit order for id {key!r}")
        return found

    # --- recon reads -------------------------------------------------------

    async def fetch_order(self, order_id: str) -> Order:
        """What became of one order, by Bybit's id."""
        self._ensure_connected()
        known = self._last.get(order_id)
        product = product_of(known.category) if known else self.product
        row = await self.rest.fetch_order(
            product, symbol=self._venue_symbols.get(order_id), order_id=order_id
        )
        if row is None:
            raise OrderError(f"no Bybit order for id {order_id!r}")
        return await self._inbound(row)

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, ticker: UniversalTicker | None = None
    ) -> Order | None:
        """Ask what happened to an order the push stream never reported.

        ``None`` means Bybit has no such order in either its open list or its
        history, which is an answer: the submit never landed.

        The ``ticker`` hint is for the case that makes this method exist: an
        order we never got an ack for has nothing cached, and Bybit needs both
        the symbol and the book to look one up.
        """
        self._ensure_connected()
        known = self._last.get(client_order_id)
        if known is not None:
            ticker = known.ticker
        native = self._venue_symbols.get(client_order_id)
        if native is None and ticker is not None:
            native = await self.symbols.exch_ticker(ticker)
        product = product_of(ticker.category) if ticker else self.product
        row = await self.rest.fetch_order(
            product, symbol=native, order_link_id=client_order_id
        )
        if row is None:
            return None
        return await self._inbound(row)

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Resting orders, across every book the account trades.

        ``symbol`` narrows it to one instrument on this connector's default
        book; without it, both books are asked. Recon wants the account, and a
        unified account's orders are not all on one of them.
        """
        self._ensure_connected()
        if symbol is not None:
            native = await self._venue_symbol(symbol)
            rows = await self.rest.fetch_open_orders(self.product, native)
            return [await self._inbound(row) for row in rows]

        out: list[Order] = []
        for product in (SPOT, LINEAR):
            for row in await self.rest.fetch_open_orders(product):
                out.append(await self._inbound(row))
        return out

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        return await self.rest.fetch_balances(account_type=self.account_type)

    async def fetch_positions(self) -> list[Position]:
        """Open contracts on the account, whichever book this session trades.

        Read off the **contract** book regardless of :attr:`category`, because
        that is where a unified account's positions are: a spot-only session
        that reported none would be describing the instrument it trades rather
        than the account it holds, and the account is what recon is for. Spot
        holdings are not positions — :meth:`fetch_balances` reports them.
        """
        self._ensure_connected()
        rows = await self.rest.fetch_position_rows(LINEAR)
        out: list[Position] = []
        for row in rows:
            ticker = await self._resolve(row.symbol, row.category or LINEAR)
            out.append(row.to_position(ticker))
        return out

    async def fetch_leverage(self, ticker: UniversalTicker) -> Decimal:
        """This account's configured leverage for a contract ``ticker``.

        Spot has no leverage — refused before the venue is asked. On linear,
        Bybit answers for a flat book when ``symbol`` is passed, which is what
        lets a strategy warm the figure before its first order.
        """
        self._ensure_connected()
        check_venue(ticker, self.name, {Category.SPOT, Category.PERP})
        if ticker.category is not Category.PERP:
            raise ExchangeError(
                f"Bybit leverage is only defined on perps, got {ticker}"
            )
        native = await self.symbols.exch_ticker(ticker)
        row = await self.rest.fetch_leverage_row(LINEAR, native)
        if row.leverage is None or row.leverage <= 0:
            raise ExchangeError(
                f"Bybit position/list for {native} has no leverage "
                f"(portfolio margin returns an empty figure)"
            )
        return row.leverage

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
        stream = await self.stream.subscribe_orders()
        try:
            async for row in stream:
                # Also keeps the cancel path from having to ask for a symbol it
                # has already seen.
                yield await self._inbound(row)
        finally:
            stream.close()

    async def _fills(self) -> AsyncIterator[Fill]:
        """Executions, from Bybit's own execution topic.

        Filtered on :attr:`~mftik.exchange.bybit.models.BybitExecution.is_fill`:
        funding payments and ADL settlements arrive on this topic too, and
        neither is something an order did.
        """
        stream = await self.stream.subscribe_executions()
        try:
            async for row in stream:
                if not row.is_fill:
                    continue
                yield row.to_fill(
                    await self._resolve(row.symbol, row.category)
                )
        finally:
            stream.close()

    async def _positions(self) -> AsyncIterator[Position]:
        """Position changes, on whichever book the account holds them.

        Bybit pushes a snapshot of the whole position when any part of it
        moves — size, entry price or unrealised pnl — so each row can be taken
        as that instrument's new truth rather than a delta to apply. A closed
        position arrives as ``size: 0``, which is how the OMS learns to drop
        it rather than carrying a stale one forever.
        """
        stream = await self.stream.subscribe_positions()
        try:
            async for row in stream:
                ticker = await self._resolve(row.symbol, row.category)
                yield row.to_position(ticker)
        finally:
            stream.close()

    async def _balances(self) -> AsyncIterator[Balance]:
        """One :class:`~mftik.exchange.models.Balance` per coin that moved.

        Bybit batches every coin touched by one event into a single wallet
        push; they are flattened here because the shared model states one
        asset.
        """
        stream = await self.stream.subscribe_wallets()
        try:
            async for wallet in stream:
                for balance in wallet.to_balances():
                    yield balance
        finally:
            stream.close()

    # --- symbols -----------------------------------------------------------

    def _ticker(self, symbol: str) -> UniversalTicker:
        """The universal identity of a symbol on this connector's market."""
        return UniversalTicker.of(self.name, self.category, symbol)

    async def _venue_symbol(self, symbol: str) -> str:
        """Canonical → Bybit's spelling, via the plane."""
        return await self.symbols.exch_ticker(self._ticker(symbol))

    async def _resolve(
        self, native_symbol: str, category: str = ""
    ) -> UniversalTicker:
        """Bybit's spelling → the universal ticker, on the row's own book.

        ``category`` is the row's, not the connector's: an account row names
        the book it came from, and a perp fill resolved against the spot table
        would either miss or — worse — land on the spot instrument of the same
        name. The connector's own book is the fallback for a row that names
        none.
        """
        return await self.symbols.symbol_for(
            self.name,
            native_symbol,
            category=category_of(category, self.category),
        )

    async def _inbound(self, row: BybitOrderUpdate) -> Order:
        """One venue order row, resolved home and indexed.

        The ticker is looked up *before* the conversion rather than patched on
        after it, so an :class:`~mftik.exchange.models.Order` never exists in a
        state where its identity is Bybit's spelling of a symbol.
        """
        ticker = await self._resolve(row.symbol, row.category)
        order = row.to_order(ticker)
        if order.quote_qty is None and self._was_quote_sized(order):
            # Stream/REST omitted marketUnit; we still know what we sent.
            order = order.model_copy(
                update={
                    "qty": row.cum_exec_qty,
                    "quote_qty": row.qty,
                    "filled_qty": row.cum_exec_qty,
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
        """Index an order by every id it can be addressed with.

        Held under the canonical symbol, because that is the form anything
        reading it back expects; the venue's spelling is kept beside it for the
        calls that need one.
        """
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


__all__ = ["BybitPrivateClient"]
