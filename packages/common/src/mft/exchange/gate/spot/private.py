"""The Gate spot trading connector.

Not an implementation of a shared trading interface — there is none; see
:mod:`mft.exchange.base`. It resembles the other venues' connectors because
the same questions get asked of every venue, not because anything enforces it.

Composes the two transports Gate actually offers:

* **WebSocket** — order entry and the account push streams. Everything on the
  hot path stays here.
* **REST** — the two recon snapshots the spot WebSocket API has no channel for
  (open orders, balances). See :mod:`mft.exchange.gate.spot.rest`.

Symbols are translated through a :class:`~mft.exchange.symbols.SymbolResolver`
(the symbol plane), never by string surgery — see that module for why.

This is the one place in the Gate adapter that *is* shaped by the shared
interface, because TD's ``Session`` drives it. The venue-native surface stays
reachable underneath as ``self.ws`` / ``self.rest``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

from mft.exchange.base import BaseClient
from mft.exchange.errors import OrderError
from mft.exchange.gate.spot.client import GATE_SPOT_WS_URL, GateSpotWebSocket
from mft.exchange.gate.spot.models import GateOrderAck, to_text
from mft.exchange.gate.spot.protocol import GateApiError
from mft.exchange.gate.spot.rest import GATE_SPOT_REST_URL, GateSpotRest
from mft.exchange.models import (
    TERMINAL_STATUSES,
    Balance,
    Fill,
    Order,
    OrderType,
    PlaceOrderRequest,
    Side,
    TimeInForce,
)
from mft.exchange.symbols import SymbolResolver, check_venue
from mft.exchange.tickers import Category, UniversalTicker

logger = logging.getLogger(__name__)

TERMINAL = TERMINAL_STATUSES

#: params keys that would collide with a common field on the wire.
_RESERVED_PARAMS = (
    "currency_pair",
    "symbol",
    "side",
    "amount",
    "price",
    "type",
    "text",
)

#: Canonical time-in-force → Gate's spelling. Gate puts post-only in the same
#: field as the durations and calls it ``poc`` ("pending or cancelled"), so the
#: whole enum maps onto one wire key here. Venues that expose post-only as a
#: separate MakerOnly flag translate it in their own adapter instead.
_TIF: dict[TimeInForce, str] = {
    TimeInForce.GTC: "gtc",
    TimeInForce.IOC: "ioc",
    TimeInForce.FOK: "fok",
    TimeInForce.POST_ONLY: "poc",
}


class GateSpotPrivateClient(BaseClient):
    """Gate spot trading account for TD.

    Symbols cross this boundary in canonical form (``BTCUSDT``) and are
    resolved to Gate's ``BTC_USDT`` on the wire; everything coming back is
    resolved home again. Venue-only order options ride in
    ``PlaceOrderRequest.params``.
    """

    name = "Gate"
    #: Gate's spot plane is a venue of its own — one credential, one market —
    #: so every instrument this connector touches is on this category, and it
    #: builds the ticker rather than being told it. A unified-account venue
    #: could not, which is why the order path carries the ticker there.
    category = Category.SPOT

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        ws_url: str = GATE_SPOT_WS_URL,
        rest_url: str = GATE_SPOT_REST_URL,
        symbols: SymbolResolver,
        ws: GateSpotWebSocket | None = None,
        rest: GateSpotRest | None = None,
        account: str = "spot",
    ) -> None:
        super().__init__()
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret are required")
        self.api_key = api_key
        self.api_secret = api_secret
        self.account = account
        self.ws = ws or GateSpotWebSocket(
            api_key=api_key, api_secret=api_secret, url=ws_url
        )
        self.rest = rest or GateSpotRest(
            api_key=api_key, api_secret=api_secret, base_url=rest_url
        )
        self.symbols = symbols
        # order_id / client_order_id → venue currency_pair. Gate needs the pair
        # to cancel, but the shared interface cancels by id alone.
        self._pairs: dict[str, str] = {}

    # --- lifecycle ---------------------------------------------------------

    async def connect(self) -> None:
        await self.ws.connect()
        await self.rest.connect()
        self._connected = True
        logger.info("Gate connected key=%s…", self.api_key[:6])

    async def close(self) -> None:
        self._connected = False
        self._pairs.clear()
        await self.ws.close()
        await self.rest.close()

    # --- order entry (WebSocket) -------------------------------------------

    async def place_order(self, request: PlaceOrderRequest) -> Order:
        self._ensure_connected()
        if request.type is OrderType.LIMIT and request.price is None:
            raise OrderError("limit order requires a price")
        if request.type is OrderType.MARKET and request.side is Side.BUY:
            # Gate reads `amount` as QUOTE currency for a spot market buy,
            # while PlaceOrderRequest.qty is base. Passing qty straight through
            # would size the order wrong by the price. Refuse rather than guess.
            raise OrderError(
                "Gate market buy sizes in quote currency, which "
                "PlaceOrderRequest.qty (base) cannot express; use a limit order"
            )
        extras = dict(request.params or {})
        # params comes from strategy code; a key that shadows a common field
        # would be a silent contradiction, so drop it rather than let it win.
        for reserved in _RESERVED_PARAMS:
            if extras.pop(reserved, None) is not None:
                logger.warning(
                    "Gate ignoring params[%r]; set it on the request",
                    reserved,
                )
        # Market orders must not rest; Gate only takes ioc/fok there.
        default_tif = "ioc" if request.type is OrderType.MARKET else None
        if request.tif is not None:
            default_tif = _TIF[request.tif]
        ticker = request.ticker
        check_venue(ticker, self.name, {self.category})
        pair = await self.symbols.exch_ticker(ticker)
        try:
            ack = await self.ws.place_order(
                currency_pair=pair,
                side=request.side,
                amount=request.qty,
                price=request.price,
                type=request.type,
                account=extras.pop("account", self.account),
                time_in_force=extras.pop("time_in_force", default_tif),
                text=request.client_order_id,
                **extras,
            )
        except GateApiError as exc:
            # Surface as OrderError so TD publishes an order reject instead of
            # treating a venue rejection as a transport failure.
            raise OrderError(str(exc)) from exc
        # No lookup on this path: the ticker is the one we sent the order
        # under, so a thin ``action_mode=ACK`` reply that omits the pair has
        # nothing to reconcile.
        order = ack.to_order(ticker)
        self._remember(order, pair)
        return order

    async def cancel_order(self, order_id: str) -> Order:
        self._ensure_connected()
        return await self._cancel(order_id)

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order:
        """Gate accepts a cancel by ``text`` when the id is in ``t-`` form."""
        self._ensure_connected()
        return await self._cancel(to_text(client_order_id))

    async def fetch_order_by_client_order_id(
        self, client_order_id: str, *, ticker: UniversalTicker | None = None
    ) -> Order | None:
        """Ask REST what happened to an order the WebSocket never reported.

        Gate takes the ``t-`` text form where an order id goes. The pair is
        required, and for an order we never got an ack for there is nothing
        cached to look it up by — hence the ``symbol`` hint.
        """
        self._ensure_connected()
        text = to_text(client_order_id)
        pair = self._pairs.get(text)
        if pair is None and ticker is not None:
            pair = await self.symbols.exch_ticker(ticker)
        if pair is None:
            raise OrderError(
                f"cannot resolve Gate order {client_order_id!r} "
                "without its symbol"
            )
        try:
            order = await self.rest.fetch_order(
                text, currency_pair=pair, ticker=await self._resolve(pair)
            )
        except GateApiError as exc:
            # ORDER_NOT_FOUND is an answer: the submit never landed.
            if "NOT_FOUND" in str(exc).upper():
                return None
            raise OrderError(str(exc)) from exc
        self._remember(order, pair)
        return order

    def on_reconnect(self, callback) -> None:
        self.ws.on_reconnect(callback)

    async def _cancel(self, order_id: str) -> Order:
        currency_pair = await self._pair_for(order_id)
        try:
            ack = await self.ws.cancel_order(
                order_id, currency_pair=currency_pair, account=self.account
            )
        except GateApiError as exc:
            raise OrderError(str(exc)) from exc
        return await self._inbound(ack, currency_pair)

    async def _pair_for(self, order_id: str) -> str:
        """Resolve an id to its pair, refreshing from REST only if unseen."""
        pair = self._pairs.get(order_id)
        if pair is not None:
            return pair
        await self.fetch_open_orders()
        pair = self._pairs.get(order_id)
        if pair is None:
            raise OrderError(f"no open Gate order for id {order_id!r}")
        return pair

    def _ticker(self, symbol: str) -> UniversalTicker:
        """The universal identity of a symbol on this connector's market."""
        return UniversalTicker.of(self.name, self.category, symbol)

    async def _venue_symbol(self, symbol: str) -> str:
        """Canonical → Gate's spelling, via the plane."""
        return await self.symbols.exch_ticker(self._ticker(symbol))

    async def _resolve(self, pair: str) -> UniversalTicker:
        """Gate's spelling → the universal ticker, on this connector's market."""
        return await self.symbols.symbol_for(
            self.name, pair, category=self.category
        )

    async def _inbound(self, ack: GateOrderAck, pair: str) -> Order:
        """One venue order reply, resolved home and indexed.

        The ticker is looked up *before* the conversion rather than patched on
        after it, so an :class:`~mft.exchange.models.Order` never exists in a
        state where its identity is Gate's spelling of a pair.
        """
        native = ack.currency_pair or pair
        order = ack.to_order(await self._resolve(native))
        self._remember(order, native)
        return order

    def _remember(self, order: Order, native_symbol: str) -> None:
        """Index an order's venue pair by every id it can be cancelled with."""
        keys = [order.order_id]
        if order.client_order_id:
            keys += [order.client_order_id, to_text(order.client_order_id)]
        if order.status in TERMINAL:
            for key in keys:
                self._pairs.pop(key, None)
            return
        for key in keys:
            if key:
                self._pairs[key] = native_symbol

    # --- recon reads (REST) ------------------------------------------------

    async def fetch_order(self, order_id: str) -> Order:
        self._ensure_connected()
        currency_pair = await self._pair_for(order_id)
        order = await self.rest.fetch_order(
            order_id,
            currency_pair=currency_pair,
            ticker=await self._resolve(currency_pair),
        )
        self._remember(order, currency_pair)
        return order

    async def fetch_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Open orders, for one pair or across the account.

        Gate's account-wide form returns every pair at once, and each row has
        to be resolved to its own ticker — so unlike the single-pair form there
        is no one ticker to hand down, and the rows come back grouped by the
        pair they were asked under.
        """
        self._ensure_connected()
        if symbol is not None:
            native = await self._venue_symbol(symbol)
            orders = await self.rest.fetch_open_orders(
                native, ticker=await self._resolve(native)
            )
            for order in orders:
                self._remember(order, native)
            return orders

        out: list[Order] = []
        for pair, rows in (await self.rest.fetch_open_order_rows()).items():
            ticker = await self._resolve(pair)
            for row in rows:
                order = row.to_order(ticker)
                self._remember(order, pair)
                out.append(order)
        return out

    async def fetch_balances(self) -> list[Balance]:
        self._ensure_connected()
        return await self.rest.fetch_balances()

    # --- account streams (WebSocket) ---------------------------------------

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
        stream = await self.ws.subscribe_orders()
        async for update in stream:
            # Also keeps the cancel path off REST for anything seen live.
            order = update.to_order(await self._resolve(update.currency_pair))
            self._remember(order, update.currency_pair)
            yield order

    async def _fills(self) -> AsyncIterator[Fill]:
        stream = await self.ws.subscribe_user_trades()
        async for trade in stream:
            yield trade.to_fill(await self._resolve(trade.currency_pair))

    async def _balances(self) -> AsyncIterator[Balance]:
        stream = await self.ws.subscribe_balances()
        async for balance in stream:
            yield balance.to_balance()


__all__ = ["GateSpotPrivateClient"]
