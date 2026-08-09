"""Binance spot WebSocket API — order entry, account reads, and snapshots.

Everything Binance answers on request rather than pushes, on one socket::

    async with BinanceSpotWsApi(api_key=k, api_secret=pem) as api:
        ack = await api.place_order(
            symbol="BTCUSDT", side="buy", type="limit",
            quantity="0.001", price="60000", client_order_id="my-id",
        )
        await api.cancel_order("BTCUSDT", order_id=ack.order_id)
        reports = await api.subscribe_execution_reports()
        async for report in reports:
            ...

Credentials are optional. Without them the market-data methods still work —
Binance serves ``depth``, ``klines``, ``ticker.24hr`` and ``exchangeInfo`` to
anyone — which is what lets the market-data connector use this class without a
trading account. With them, :meth:`connect` runs ``session.logon`` and the
trading half unlocks.

**The logon is the whole authentication story.** Binance signs it with the
Ed25519 key and then authenticates the *connection*, so every later call —
orders, cancels, account reads, the user data subscribe — carries no key, no
timestamp-and-signature, and costs no crypto. That also means a lost socket is
a lost authentication: :meth:`_on_open` re-runs the logon on every reconnect
before anything else is replayed, and a call that arrives in between is
refused here rather than by the venue.

**The user data stream is one subscription, not three.** Binance pushes order
events, balance snapshots and balance deltas down the same subscription, so
:meth:`subscribe_execution_reports` and its neighbours are filtered views of
one ``userDataStream.subscribe`` — taken out the first time any of them is
asked for, and replayed on reconnect only if something is still reading.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, TypeVar

from mft.exchange.binance.spot import methods as m
from mft.exchange.binance.spot.models import (
    BinanceAccountInfo,
    BinanceAccountPosition,
    BinanceBalanceUpdate,
    BinanceDepth,
    BinanceExecutionReport,
    BinanceOrderAck,
    BinanceTicker,
    kline_from_row,
)
from mft.exchange.binance.spot.protocol import (
    BINANCE_SPOT_WS_API_URL,
    BinanceResponse,
    load_private_key,
    logon_frame,
    now_ms,
    request_frame,
    signed_frame,
)
from mft.exchange.binance.spot.socket import BinanceSocket
from mft.exchange.errors import ExchangeError
from mft.exchange.models import Instrument, Kline, OrderBook, OrderType, Side, Ticker
from mft.exchange.stream import EventStream
from mft.exchange.tickers import UniversalTicker

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Binance's ``depth`` reply caps out here; asking for more is an error, not a
#: truncated book.
MAX_DEPTH = 5000
#: Most candles ``klines`` returns in one call.
MAX_KLINES = 1000


@dataclass
class _EventSub:
    """One filtered view of the single user data subscription."""

    event_type: str
    stream: EventStream[Any]
    parse: Callable[[dict[str, Any]], Any]


class BinanceSpotWsApi(BinanceSocket):
    """Binance spot WebSocket API connectivity."""

    name = "binance.spot"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        url: str = BINANCE_SPOT_WS_API_URL,
        recv_window: int | None = None,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        keepalive: float = 20.0,
    ) -> None:
        super().__init__(
            url,
            ack_timeout=ack_timeout,
            reconnect=reconnect,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            max_retry_backoff=max_retry_backoff,
            keepalive=keepalive,
        )
        self.api_key = api_key
        #: How far a request may lag the server clock, in ms. Left unset means
        #: Binance's own default (5000); raising it widens the window in which
        #: a replayed request is still accepted, so it is opt-in.
        self.recv_window = recv_window
        # Parsed once, at construction, so a malformed key fails where it was
        # configured rather than on the first order.
        self._key = load_private_key(api_secret) if api_secret else None
        self._logged_on = False
        self._user_streams: list[_EventSub] = []
        self._user_subscribed = False

    # --- lifecycle ---------------------------------------------------------

    @property
    def authenticated(self) -> bool:
        """Whether credentials are configured (trading + user data)."""
        return bool(self.api_key and self._key)

    @property
    def logged_on(self) -> bool:
        """Whether ``session.logon`` has succeeded on the current socket."""
        return self._logged_on

    async def _on_open(self) -> None:
        self._logged_on = False
        if not self.authenticated:
            return
        assert self.api_key and self._key
        frame, req_id = logon_frame(
            api_key=self.api_key,
            private_key=self._key,
            recv_window=self.recv_window,
        )
        await self.handshake(frame, req_id, method=m.SESSION_LOGON)
        self._logged_on = True
        logger.info("%s session logged on key=%s…", self.name, self.api_key[:6])

    async def _restore(self) -> None:
        # Only if something is still reading it. Re-subscribing for streams
        # every consumer has closed would leave the socket carrying a firehose
        # nobody drains.
        if self._user_streams:
            self._user_subscribed = False
            await self._ensure_user_data()

    def _teardown(self) -> None:
        self._logged_on = False
        self._user_subscribed = False
        for sub in list(self._user_streams):
            sub.stream.close()
        self._user_streams.clear()

    # --- calls -------------------------------------------------------------

    async def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Make one WebSocket API call and return its ``result``.

        The escape hatch for methods not wrapped below. Signing is decided from
        :data:`.methods.SIGNED` and the session's state, not by the caller: off
        a logged-on socket a signed method gets a per-call ``apiKey`` +
        ``signature``, and on one it gets neither.

        **It still gets a ``timestamp``.** ``session.logon`` authenticates the
        connection, which replaces the credential on each call — not the clock.
        Binance rejects a signed method with no ``timestamp`` as ``-1102``
        however the connection was authenticated, and it is a mandatory
        parameter of the endpoint rather than a part of the signature scheme.
        """
        self._ensure_connected()
        signed = method in m.SIGNED or method in m.SESSION_ONLY
        if signed and not self.authenticated:
            kind = "trading call" if method in m.TRADING else "authenticated call"
            raise ExchangeError(
                f"{method} is a {kind}; api_key and api_secret are required"
            )
        if method in m.SESSION_ONLY and not self._logged_on:
            raise ExchangeError(
                f"{method} requires an active session.logon; reconnect or "
                f"construct the client with api_key/api_secret"
            )

        if signed and not self._logged_on:
            assert self.api_key and self._key
            frame, req_id = signed_frame(
                method,
                params,
                api_key=self.api_key,
                private_key=self._key,
                recv_window=self.recv_window,
            )
        else:
            frame, req_id = request_frame(method, self._stamped(method, params))
        resp = await self.request(frame, req_id, method=method, timeout=timeout)
        return resp.result

    def _stamped(
        self, method: str, params: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        """Add the clock a signed method needs even on an authenticated session.

        Only :data:`.methods.SIGNED` methods take one. The session-only methods
        do not: ``userDataStream.subscribe`` carries no params at all, and
        sending it a ``timestamp`` would be a parameter the endpoint does not
        accept rather than one it requires.
        """
        if method not in m.SIGNED:
            return params
        body: dict[str, Any] = dict(params or {})
        body["timestamp"] = now_ms()
        if self.recv_window is not None:
            body["recvWindow"] = self.recv_window
        return body

    # --- market data (open) ------------------------------------------------

    async def fetch_instruments(self, *symbols: str) -> list[Instrument]:
        """``exchangeInfo`` — tradeable symbols and their filters.

        Left in Binance's own spelling: this is what the symbol plane ingests
        to *build* the canonical mapping, so it cannot depend on that mapping
        existing.
        """
        params: dict[str, Any] = {}
        if len(symbols) == 1:
            params["symbol"] = symbols[0]
        elif symbols:
            # ``symbols`` is a JSON array in a string-ish param; Binance's own
            # examples send it as a real array over the WebSocket API.
            params["symbols"] = list(symbols)
        result = await self.call(m.EXCHANGE_INFO, params or None)
        rows = (result or {}).get("symbols") or []
        return [
            _to_instrument(row)
            for row in rows
            # ``exchangeInfo`` also lists halted and pre-launch symbols;
            # Instrument has nowhere to say "not yet", so one that cannot be
            # traded is dropped rather than returned looking live.
            if row.get("status") == "TRADING"
        ]

    async def fetch_order_book(
        self, symbol: str, *, ticker: UniversalTicker, depth: int = 100
    ) -> OrderBook:
        """``depth`` — a whole book, capped at ``depth``.

        The reply carries no instrument and no timestamp, so both are the
        caller's. ``symbol`` is what goes on the wire and ``ticker`` is what
        the answer is labelled with: only the symbol plane maps between them,
        and it lives a layer up, so this one is told both.
        """
        result = await self.call(m.DEPTH, {"symbol": symbol, "limit": depth})
        return BinanceDepth.model_validate(result).to_order_book(ticker)

    async def fetch_ticker(
        self, symbol: str, *, ticker: UniversalTicker
    ) -> Ticker:
        """``ticker.24hr`` — the same row shape ``@ticker`` pushes."""
        result = await self.call(m.TICKER_24HR, {"symbol": symbol})
        return BinanceTicker.model_validate(result).to_ticker(ticker)

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        *,
        ticker: UniversalTicker,
        limit: int = 100,
    ) -> list[Kline]:
        """``klines`` — recent candles, oldest first.

        ``interval`` is Binance's own spelling; translating from the canonical
        one happens a layer up, in :class:`.public.BinanceSpotPublicClient`.
        """
        result = await self.call(
            m.KLINES, {"symbol": symbol, "interval": interval, "limit": limit}
        )
        return [kline_from_row(row, ticker, interval) for row in result or []]

    # --- trading -----------------------------------------------------------

    async def place_order(
        self,
        *,
        symbol: str,
        side: Side | str,
        type: OrderType | str = OrderType.LIMIT,
        quantity: Decimal | str | None = None,
        quote_order_qty: Decimal | str | None = None,
        price: Decimal | str | None = None,
        time_in_force: str | None = None,
        client_order_id: str | None = None,
        **extra: Any,
    ) -> BinanceOrderAck:
        """``order.place``.

        ``type`` and ``side`` are uppercased for Binance; pass either our enum
        or the venue's spelling. ``client_order_id`` becomes
        ``newClientOrderId`` unchanged — Binance imposes its own character set
        on it and refusing here would only duplicate a check the venue does
        better.

        ``quantity`` is in the base asset for every order type including a
        market buy, which is the shared model's unit — so unlike some venues
        there is nothing to convert and no size to get wrong by a price.
        ``quote_order_qty`` is Binance's alternative for spending a fixed
        amount of quote currency, offered here for callers that want it.
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": str(side).upper(),
            "type": str(type).upper(),
        }
        if quantity is not None:
            params["quantity"] = quantity
        if quote_order_qty is not None:
            params["quoteOrderQty"] = quote_order_qty
        if price is not None:
            params["price"] = price
        if time_in_force:
            params["timeInForce"] = time_in_force.upper()
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        params.update(extra)
        result = await self.call(m.ORDER_PLACE, params)
        return BinanceOrderAck.model_validate(result)

    async def cancel_order(
        self,
        symbol: str,
        *,
        order_id: str | int | None = None,
        client_order_id: str | None = None,
        **extra: Any,
    ) -> BinanceOrderAck:
        """``order.cancel`` — by venue id or by the id we gave it.

        Binance needs the symbol either way: an order id alone does not
        identify an order to it.
        """
        params = _by_id(symbol, order_id, client_order_id, what="cancel")
        params.update(extra)
        result = await self.call(m.ORDER_CANCEL, params)
        return BinanceOrderAck.model_validate(result)

    async def query_order(
        self,
        symbol: str,
        *,
        order_id: str | int | None = None,
        client_order_id: str | None = None,
    ) -> BinanceOrderAck:
        """``order.status`` — what became of one order."""
        result = await self.call(
            m.ORDER_STATUS, _by_id(symbol, order_id, client_order_id, what="query")
        )
        return BinanceOrderAck.model_validate(result)

    async def fetch_open_orders(
        self, symbol: str | None = None
    ) -> list[BinanceOrderAck]:
        """``openOrders.status`` — one symbol's resting orders, or all of them.

        Asking across the account is heavily weighted by Binance (it scans
        every symbol), so pass a symbol when one is known.
        """
        result = await self.call(
            m.OPEN_ORDERS_STATUS, {"symbol": symbol} if symbol else None
        )
        return [BinanceOrderAck.model_validate(row) for row in result or []]

    async def fetch_account(self, *, omit_zero: bool = True) -> BinanceAccountInfo:
        """``account.status`` — permissions plus the balance sheet.

        Zero balances are omitted by default: a spot account lists every asset
        Binance has ever listed, and the empty ones are several hundred rows
        that say nothing.
        """
        result = await self.call(m.ACCOUNT_STATUS, {"omitZeroBalances": omit_zero})
        return BinanceAccountInfo.model_validate(result)

    # --- user data stream --------------------------------------------------

    async def subscribe_execution_reports(
        self,
    ) -> EventStream[BinanceExecutionReport]:
        """``executionReport`` — order lifecycle, and fills along with it."""
        return await self._user_stream(
            m.EXECUTION_REPORT, BinanceExecutionReport.model_validate
        )

    async def subscribe_account_positions(
        self,
    ) -> EventStream[BinanceAccountPosition]:
        """``outboundAccountPosition`` — balances that moved, as snapshots."""
        return await self._user_stream(
            m.OUTBOUND_ACCOUNT_POSITION, BinanceAccountPosition.model_validate
        )

    async def subscribe_balance_updates(self) -> EventStream[BinanceBalanceUpdate]:
        """``balanceUpdate`` — deposits, withdrawals and transfers, as deltas."""
        return await self._user_stream(
            m.BALANCE_UPDATE, BinanceBalanceUpdate.model_validate
        )

    async def _user_stream(
        self, event_type: str, parse: Callable[[dict[str, Any]], T]
    ) -> EventStream[T]:
        self._ensure_connected()
        await self._ensure_user_data()
        stream: EventStream[T] = EventStream(on_close=self._drop_user_stream)
        self._user_streams.append(
            _EventSub(event_type=event_type, stream=stream, parse=parse)
        )
        return stream

    async def _ensure_user_data(self) -> None:
        """Take out the single subscription, once per socket."""
        if self._user_subscribed:
            return
        await self.call(m.USER_DATA_STREAM_SUBSCRIBE)
        self._user_subscribed = True
        logger.info("%s user data stream subscribed", self.name)

    def _drop_user_stream(self, stream: EventStream[Any]) -> None:
        self._user_streams = [
            s for s in self._user_streams if s.stream is not stream
        ]

    # --- push routing ------------------------------------------------------

    def _push(self, resp: BinanceResponse) -> None:
        if resp.event is None:
            logger.debug("%s ignoring push %r", self.name, resp)
            return
        for sub in [s for s in self._user_streams if s.event_type == resp.event_type]:
            try:
                sub.stream.push(sub.parse(resp.event))
            except Exception:
                logger.exception(
                    "%s failed to parse %s event: %r",
                    self.name,
                    resp.event_type,
                    resp.event,
                )


def _by_id(
    symbol: str,
    order_id: str | int | None,
    client_order_id: str | None,
    *,
    what: str,
) -> dict[str, Any]:
    """Build the ``symbol`` + one-id params both cancel and query take."""
    if order_id is None and not client_order_id:
        raise ExchangeError(
            f"Binance {what} needs orderId or origClientOrderId, got neither"
        )
    params: dict[str, Any] = {"symbol": symbol}
    if order_id is not None:
        params["orderId"] = int(order_id)
    if client_order_id:
        params["origClientOrderId"] = client_order_id
    return params


def _to_instrument(row: dict[str, Any]) -> Instrument:
    """One ``exchangeInfo`` symbol, with its steps pulled out of ``filters``.

    Binance keeps the steps in a list of typed filter objects rather than as
    fields, and publishes ``0`` for a step it does not enforce. A zero step is
    dropped rather than stored, because a zero here would divide.
    """
    filters = {
        str(f.get("filterType", "")): f for f in row.get("filters", []) or []
    }
    price = filters.get("PRICE_FILTER", {})
    lot = filters.get("LOT_SIZE", {})
    # ``NOTIONAL`` superseded ``MIN_NOTIONAL``; older symbols still carry the
    # latter and a few carry both.
    notional = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}

    fields: dict[str, Any] = {
        "symbol": str(row.get("symbol", "")),
        "base": str(row.get("baseAsset", "")),
        "quote": str(row.get("quoteAsset", "")),
        "min_qty": _opt_dec(lot.get("minQty")),
        "min_notional": _opt_dec(notional.get("minNotional")),
    }
    tick = _opt_dec(price.get("tickSize"))
    if tick is not None:
        fields["tick_size"] = tick
    step = _opt_dec(lot.get("stepSize"))
    if step is not None:
        fields["lot_size"] = step
    return Instrument(**fields)


def _opt_dec(value: Any) -> Decimal | None:
    """``None`` where Binance publishes no bound, so the filter reads as absent."""
    if value is None or value == "":
        return None
    parsed = Decimal(str(value))
    return parsed if parsed > 0 else None


__all__ = ["MAX_DEPTH", "MAX_KLINES", "BinanceSpotWsApi"]
