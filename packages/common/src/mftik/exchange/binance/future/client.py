"""Binance futures WebSocket API — order entry, account reads, listen keys.

Everything ``ws-fapi.binance.com`` answers on request::

    async with BinanceFutureWsApi(api_key=k, api_secret=pem) as api:
        ack = await api.place_order(
            symbol="BTCUSDT", side="buy", type="limit",
            quantity="0.01", price="60000", time_in_force="GTC",
            client_order_id="my-id",
        )
        await api.cancel_order("BTCUSDT", order_id=ack.order_id)
        positions = await api.fetch_positions()

Credentials are optional. Without them the three market-data methods still
work — Binance serves ``depth``, ``ticker.book`` and ``ticker.price`` to
anyone. With them, :meth:`connect` runs ``session.logon`` and the trading half
unlocks.

**The logon is the whole authentication story**, as on spot: Binance signs it
with the Ed25519 key and then authenticates the *connection*, so every later
call carries no key, no signature and costs no crypto. A lost socket is a lost
authentication, so :meth:`_on_open` re-runs the logon on every reconnect before
anything else.

**Nothing is pushed to this socket.** The futures WebSocket API has no
``userDataStream.subscribe``: the account feed is a listen key handed out here
(:meth:`start_user_stream`) and read on a different connection, which is
:class:`~mftik.exchange.binance.future.user.BinanceFutureUserStream`'s job. That
is the deepest structural difference from the spot adapter, and it is why this
class has no ``_push`` and no stream registry.

**Two reads that are on spot's WebSocket API are not on this one**: candles and
the instrument listing. They are REST-only on futures, and live in
:mod:`mftik.exchange.binance.future.rest`.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from mftik.exchange.binance.future import methods as m
from mftik.exchange.binance.future.models import (
    BinanceFutureBalance,
    BinanceFutureBookQuote,
    BinanceFutureDepth,
    BinanceFutureOrderAck,
    BinanceFuturePosition,
    BinanceFuturePrice,
)
from mftik.exchange.binance.future.protocol import (
    BINANCE_FUTURE_WS_API_URL,
    load_private_key,
    logon_frame,
    now_ms,
    request_frame,
    signed_frame,
)
from mftik.exchange.binance.models import secs
from mftik.exchange.binance.socket import BinanceSocket
from mftik.exchange.errors import ExchangeError
from mftik.exchange.models import OrderBook, OrderType, Side, Ticker
from mftik.exchange.tickers import UniversalTicker

logger = logging.getLogger(__name__)

#: Binance's futures ``depth`` reply caps out here; asking for more is an
#: error, not a truncated book.
MAX_DEPTH = 1000


class BinanceFutureWsApi(BinanceSocket):
    """Binance USDⓈ-M futures WebSocket API connectivity."""

    name = "binance.future"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        url: str = BINANCE_FUTURE_WS_API_URL,
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

    def _teardown(self) -> None:
        self._logged_on = False

    # --- calls -------------------------------------------------------------

    async def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Make one WebSocket API call and return its ``result``.

        The escape hatch for methods not wrapped below. How a call authenticates
        is decided from :mod:`.methods` and the session's state, not by the
        caller, and futures has three cases rather than spot's two:

        * **signed** (:data:`~.methods.SIGNED`) — off a logged-on socket, an
          ``apiKey`` and a ``signature``; on one, neither. Either way a
          ``timestamp``: ``session.logon`` replaces the credential on each
          call, not the clock, and Binance answers ``-1102`` without one.
        * **key-only** (:data:`~.methods.API_KEY_ONLY`) — the listen key
          methods. They name whose stream is meant and sign nothing, so off a
          logged-on socket they carry ``apiKey`` alone, and on one they carry
          no params at all. Sending them a timestamp is a parameter the
          endpoint does not take.
        * **open** — the market data, which needs nothing.
        """
        self._ensure_connected()
        signed = method in m.SIGNED
        key_only = method in m.API_KEY_ONLY
        if (signed or key_only) and not self.authenticated:
            kind = "trading call" if method in m.TRADING else "authenticated call"
            raise ExchangeError(
                f"{method} is a {kind}; api_key and api_secret are required"
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
        """Add whatever a call still needs once the session is authenticated."""
        if method in m.API_KEY_ONLY:
            # Only off a logged-on socket, where the connection does not say
            # whose stream is meant.
            if self._logged_on or not self.api_key:
                return params
            body = dict(params or {})
            body["apiKey"] = self.api_key
            return body
        if method not in m.SIGNED:
            return params
        body = dict(params or {})
        body["timestamp"] = now_ms()
        if self.recv_window is not None:
            body["recvWindow"] = self.recv_window
        return body

    # --- market data (open) ------------------------------------------------

    async def fetch_order_book(
        self, symbol: str, *, ticker: UniversalTicker, depth: int = 100
    ) -> OrderBook:
        """``depth`` — a whole book, capped at ``depth``.

        The reply carries no instrument, so ``symbol`` is what goes on the wire
        and ``ticker`` is what the answer is labelled with: only the symbol
        plane maps between them, and it lives a layer up.
        """
        result = await self.call(m.DEPTH, {"symbol": symbol, "limit": depth})
        return BinanceFutureDepth.model_validate(result).to_order_book(ticker)

    async def fetch_book_quote(self, symbol: str) -> BinanceFutureBookQuote:
        """``ticker.book`` — top of book with sizes, in Binance's own terms."""
        result = await self.call(m.TICKER_BOOK, {"symbol": symbol})
        return BinanceFutureBookQuote.model_validate(_one(result))

    async def fetch_price(self, symbol: str) -> BinanceFuturePrice:
        """``ticker.price`` — the last traded price."""
        result = await self.call(m.TICKER_PRICE, {"symbol": symbol})
        return BinanceFuturePrice.model_validate(_one(result))

    async def fetch_ticker(self, symbol: str, *, ticker: UniversalTicker) -> Ticker:
        """A :class:`~mftik.exchange.models.Ticker`, which costs two calls here.

        Futures has no ``ticker.24hr`` on the WebSocket API and its
        ``ticker.book`` carries no last price, so the quote and the last trade
        come from different methods. Both are asked for rather than one being
        derived: a mid price is not a last price, and a last price is not a
        quote — either substitution would be a number this platform made up.
        """
        quote = await self.fetch_book_quote(symbol)
        last = await self.fetch_price(symbol)
        return Ticker(
            universal_ticker=str(ticker),
            bid=quote.bid,
            ask=quote.ask,
            last=last.price,
            ts=last.ts or secs(quote.time),
        )

    # --- trading -----------------------------------------------------------

    async def place_order(
        self,
        *,
        symbol: str,
        side: Side | str,
        type: OrderType | str = OrderType.LIMIT,
        quantity: Decimal | str | None = None,
        price: Decimal | str | None = None,
        time_in_force: str | None = None,
        client_order_id: str | None = None,
        position_side: str | None = None,
        reduce_only: bool | None = None,
        **extra: Any,
    ) -> BinanceFutureOrderAck:
        """``order.place``.

        ``type`` and ``side`` are uppercased for Binance; pass either our enum
        or the venue's spelling. ``client_order_id`` becomes
        ``newClientOrderId`` unchanged — Binance imposes its own character set
        on it and refusing here would only duplicate a check the venue does
        better.

        ``quantity`` is in the base asset, which on a USDⓈ-M contract is what
        the contract is denominated in — so, unlike some venues, a market buy
        needs no conversion and there is no size to get wrong by a price.

        ``position_side`` is left unset on a one-way account, where Binance
        defaults it to ``BOTH``; a hedge-mode account must name ``LONG`` or
        ``SHORT`` on every order and Binance rejects one that does not.
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": str(side).upper(),
            "type": str(type).upper(),
        }
        if quantity is not None:
            params["quantity"] = quantity
        if price is not None:
            params["price"] = price
        if time_in_force:
            params["timeInForce"] = time_in_force.upper()
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        if position_side:
            params["positionSide"] = position_side.upper()
        if reduce_only is not None:
            params["reduceOnly"] = reduce_only
        params.update(extra)
        result = await self.call(m.ORDER_PLACE, params)
        return BinanceFutureOrderAck.model_validate(result)

    async def cancel_order(
        self,
        symbol: str,
        *,
        order_id: str | int | None = None,
        client_order_id: str | None = None,
        **extra: Any,
    ) -> BinanceFutureOrderAck:
        """``order.cancel`` — by venue id or by the id we gave it.

        Binance needs the symbol either way: an order id alone does not
        identify an order to it.
        """
        params = _by_id(symbol, order_id, client_order_id, what="cancel")
        params.update(extra)
        result = await self.call(m.ORDER_CANCEL, params)
        return BinanceFutureOrderAck.model_validate(result)

    async def query_order(
        self,
        symbol: str,
        *,
        order_id: str | int | None = None,
        client_order_id: str | None = None,
    ) -> BinanceFutureOrderAck:
        """``order.status`` — what became of one order."""
        result = await self.call(
            m.ORDER_STATUS, _by_id(symbol, order_id, client_order_id, what="query")
        )
        return BinanceFutureOrderAck.model_validate(result)

    # --- account -----------------------------------------------------------

    async def fetch_balances(self) -> list[BinanceFutureBalance]:
        """``v2/account.balance`` — every asset in the futures wallet."""
        result = await self.call(m.ACCOUNT_BALANCE)
        return [BinanceFutureBalance.model_validate(row) for row in result or []]

    async def fetch_positions(self) -> list[BinanceFuturePosition]:
        """``v2/account.position`` — the contracts this account holds.

        The v2 method answers only for symbols with a position or a resting
        order, where the v1 one answers for every listed contract. That is the
        difference between a handful of rows and several hundred on every
        recon, and none of the empty ones say anything.
        """
        result = await self.call(m.ACCOUNT_POSITION)
        return [BinanceFuturePosition.model_validate(row) for row in result or []]

    # --- user data stream --------------------------------------------------

    async def start_user_stream(self) -> str:
        """``userDataStream.start`` — a listen key for the account feed.

        The key is a credential and a session: it identifies the account *and*
        the stream, expires 60 minutes after the last :meth:`ping_user_stream`,
        and the events arrive on a socket of their own. Handing it back rather
        than opening that socket here keeps this class request/reply only.
        """
        result = await self.call(m.USER_DATA_STREAM_START)
        key = str((result or {}).get("listenKey") or "")
        if not key:
            raise ExchangeError(
                f"{m.USER_DATA_STREAM_START} answered without a listenKey: {result!r}"
            )
        return key

    async def ping_user_stream(self) -> None:
        """``userDataStream.ping`` — another 60 minutes for the current key.

        Keyed to the account, not to a key: Binance extends whichever listen
        key this credential has open, which is why the caller does not pass
        one back.
        """
        await self.call(m.USER_DATA_STREAM_PING)

    async def stop_user_stream(self) -> None:
        """``userDataStream.stop`` — close the account feed now."""
        await self.call(m.USER_DATA_STREAM_STOP)


def _one(result: Any) -> dict[str, Any]:
    """One row, whether Binance answered with an object or a one-item array.

    ``ticker.book`` and ``ticker.price`` answer an array when asked for several
    symbols and a bare object when asked for one — and answer the array form
    for a single symbol on some contract types. Reading both shapes here keeps
    that off every caller.
    """
    if isinstance(result, list):
        if not result:
            raise ExchangeError("Binance answered with an empty ticker array")
        return dict(result[0])
    return dict(result or {})


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
            f"Binance futures {what} needs orderId or origClientOrderId, "
            f"got neither"
        )
    params: dict[str, Any] = {"symbol": symbol}
    if order_id is not None:
        params["orderId"] = int(order_id)
    if client_order_id:
        params["origClientOrderId"] = client_order_id
    return params


__all__ = ["MAX_DEPTH", "BinanceFutureWsApi"]
