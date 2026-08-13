"""Bybit v5 REST — the reads the sockets cannot serve, and a fallback path.

Split by whether the call is signed, because the two have different reasons to
exist:

* :class:`BybitPublicRest` — MD's snapshot reads. Instruments, ticker, order
  book and candle history have no push equivalent a caller can wait for: the
  topics arrive on their own schedule, and ``kline`` pushes the window in
  progress and nothing before it.
* :class:`BybitRest` — TD's recon reads, plus order entry as a fallback. Bybit
  has no WebSocket call for "what is open right now" or "what is the balance";
  those exist only over REST, and recon needs both at attach time. Returning
  nothing there would leave the OMS believing the account is flat, which is
  worse than not reconciling at all.

Order entry appears in both transports on purpose. The trade socket
(:mod:`.trade`) is the fast path — one authenticated connection, no signature
per order — and this is the same call for a caller that would rather not hold
a second socket open. They take the same arguments and answer the same two ids.

**The reads that answer a shared model take a ``ticker``** as well as the
venue symbol, and the two are not redundant: the symbol is what goes on the
wire, and the ticker is what the answer is labelled with. Only the symbol
plane can map between them, and it lives a layer up — so this layer is told
both rather than deriving either.

**Every response is a 200.** Bybit reports refusals in the body, as a non-zero
``retCode``, and reserves HTTP status codes for transport-level trouble. So the
status line is not the check — :meth:`_parse` reads the envelope, and a caller
that only looked at ``response.status_code`` would treat every rejected order
as a success.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import httpx

from mft.exchange.bybit import channels as ch
from mft.exchange.bybit.models import (
    EXEC_TYPE_TRADE,
    BybitExecution,
    BybitOrderAck,
    BybitOrderUpdate,
    BybitPosition,
    BybitTicker,
    BybitWallet,
    instrument_from_row,
    kline_from_row,
    order_book_from_result,
)
from mft.exchange.bybit.protocol import (
    BYBIT_REST_URL,
    DEFAULT_RECV_WINDOW_MS,
    RET_OK,
    SPOT,
    BybitRestError,
    json_body,
    query_string,
    rest_headers,
)
from mft.exchange.errors import ExchangeError
from mft.exchange.models import (
    Balance,
    Instrument,
    Kline,
    OrderBook,
    Ticker,
)
from mft.exchange.tickers import UniversalTicker

logger = logging.getLogger(__name__)

#: Most candles ``/v5/market/kline`` returns in one call.
MAX_KLINES = 1000

#: Most rows ``instruments-info`` returns per page. More than that comes back
#: paginated, behind a cursor.
MAX_INSTRUMENT_PAGE = 1000

#: Most history rows ``execution/list`` / ``order/history`` return per page.
MAX_HISTORY = 100

#: What a unified trading account is called. A classic account's spot wallet is
#: ``SPOT``; Bybit has been migrating everyone to ``UNIFIED`` for years, so it
#: is the default and the other is a constructor argument.
UNIFIED = "UNIFIED"


class _BybitRestTransport:
    """httpx lifecycle and envelope decoding, shared by the signed/public pair."""

    def __init__(
        self,
        *,
        base_url: str = BYBIT_REST_URL,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client = client
        self._owns_client = client is None

    async def connect(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url, timeout=self.timeout
            )
            self._owns_client = True

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    def _headers(self, payload: str) -> dict[str, str]:
        """Per-request headers. Public calls send none beyond ``Accept``."""
        return {"Accept": "application/json"}

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        await self.connect()
        assert self._client is not None
        # Built once and used twice: the signature covers this exact string, so
        # letting httpx render its own query would sign something else.
        query = query_string(params)
        url = f"{path}?{query}" if query else path
        response = await self._client.get(url, headers=self._headers(query))
        return self._parse(response, path)

    async def _post(self, path: str, args: dict[str, Any] | None = None) -> Any:
        await self.connect()
        assert self._client is not None
        body = json_body(args or {})
        headers = self._headers(body)
        headers["Content-Type"] = "application/json"
        response = await self._client.post(path, content=body, headers=headers)
        return self._parse(response, path)

    def _parse(self, response: httpx.Response, path: str) -> Any:
        try:
            payload = response.json()
        except ValueError:
            raise BybitRestError(
                None,
                response.text[:200],
                status=response.status_code,
                op=path,
            ) from None
        if not isinstance(payload, dict):
            raise BybitRestError(
                None,
                f"unexpected body {payload!r}",
                status=response.status_code,
                op=path,
            )
        code = payload.get("retCode")
        if response.status_code >= 400 or (code is not None and code != RET_OK):
            raise BybitRestError(
                None if code is None else int(code),
                str(payload.get("retMsg") or response.text[:200]),
                status=response.status_code,
                op=path,
            )
        return payload.get("result") or {}


class BybitPublicRest(_BybitRestTransport):
    """Unsigned reads — the market-data snapshots MD asks for on demand.

    Takes no credentials: Bybit serves all of these to anyone, and requiring
    keys for public data would mean MD could not run a feed without a trading
    account.

    Every method names a ``product`` (``spot``, ``linear``, …). Unlike the
    public sockets, where the category is the URL, here it is a parameter — so
    one client covers every book.
    """

    async def fetch_instruments(
        self, product: str = SPOT, *, symbol: str | None = None
    ) -> list[Instrument]:
        """``instruments-info`` — tradeable symbols and their filters.

        Left in Bybit's own spelling: this is what the symbol plane ingests to
        *build* the canonical mapping, so it cannot depend on that mapping
        existing.

        Paginated, and followed to the end. Bybit returns a cursor rather than
        a total, and a caller that read only the first page would silently see
        a fraction of the venue — which for spot is several hundred symbols.
        """
        instruments: list[Instrument] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {
                "category": product,
                "limit": MAX_INSTRUMENT_PAGE,
            }
            if symbol:
                params["symbol"] = symbol
            if cursor:
                params["cursor"] = cursor
            result = await self._get(ch.MARKET_INSTRUMENTS, params)
            for row in result.get("list") or []:
                # The endpoint also lists pre-launch and delisted symbols;
                # Instrument has nowhere to say "not yet", so one that cannot
                # be traded is dropped rather than returned looking live.
                if str(row.get("status", "")) != "Trading":
                    continue
                instruments.append(instrument_from_row(row))
            cursor = str(result.get("nextPageCursor") or "")
            if not cursor:
                return instruments

    async def fetch_ticker_row(self, product: str, symbol: str) -> BybitTicker:
        """``tickers`` — the venue's row, sizes and all.

        Unlike the ``tickers`` push, the REST form carries ``bid1Price`` and
        ``bid1Size`` on spot as well, so a caller wanting the whole quote does
        not need the book. Venue-native because those sizes have no place on a
        shared :class:`~mft.exchange.models.Ticker`, which states a price.
        """
        result = await self._get(
            ch.MARKET_TICKERS, {"category": product, "symbol": symbol}
        )
        rows = result.get("list") or []
        if not rows:
            raise BybitRestError(
                None, f"no ticker for {symbol}", op=ch.MARKET_TICKERS
            )
        return BybitTicker.model_validate(rows[0])

    async def fetch_ticker(
        self, product: str, symbol: str, *, ticker: UniversalTicker
    ) -> Ticker:
        """``tickers`` — 24h stats, and top of book on every category."""
        row = await self.fetch_ticker_row(product, symbol)
        return row.to_ticker(ticker)

    async def fetch_order_book(
        self,
        product: str,
        symbol: str,
        *,
        ticker: UniversalTicker,
        depth: int = 50,
    ) -> OrderBook:
        """``orderbook`` — a whole book, capped at ``depth``, dated by Bybit."""
        result = await self._get(
            ch.MARKET_ORDER_BOOK,
            {"category": product, "symbol": symbol, "limit": depth},
        )
        return order_book_from_result(result, ticker)

    async def fetch_klines(
        self,
        product: str,
        symbol: str,
        interval: str,
        *,
        ticker: UniversalTicker,
        limit: int = 100,
    ) -> list[Kline]:
        """``kline`` — recent candles, **reversed to oldest first**.

        Bybit answers newest first, which is the opposite of every other venue
        here and of what a series wants. Reversing at the boundary means no
        caller has to know that, and none can forget.

        ``interval`` is Bybit's own spelling; translating from the canonical
        one happens a layer up, in :class:`.public.BybitPublicClient`.
        """
        result = await self._get(
            ch.MARKET_KLINE,
            {
                "category": product,
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
            },
        )
        rows = result.get("list") or []
        return [kline_from_row(row, ticker, interval) for row in reversed(rows)]

    async def server_time(self) -> float:
        """``time`` — Bybit's clock, in seconds.

        Worth having because every signature this adapter makes is a deadline
        compared against it: a socket auth that fails with ``10004`` on a
        machine whose clock has drifted looks exactly like a bad key.
        """
        result = await self._get(ch.MARKET_TIME)
        return float(result.get("timeNano", 0) or 0) / 1e9 or float(
            result.get("timeSecond", 0) or 0
        )


class BybitRest(_BybitRestTransport):
    """Signed calls — recon reads, positions, and order entry as a fallback.

    One credential covers every category, so these methods take the product as
    an argument rather than the client being built for one.
    """

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        base_url: str = BYBIT_REST_URL,
        recv_window: int = DEFAULT_RECV_WINDOW_MS,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(base_url=base_url, timeout=timeout, client=client)
        self.api_key = api_key
        self._api_secret = api_secret
        self.recv_window = recv_window

    def _headers(self, payload: str) -> dict[str, str]:
        headers = rest_headers(
            api_key=self.api_key,
            api_secret=self._api_secret,
            payload=payload,
            recv_window=self.recv_window,
        )
        headers["Accept"] = "application/json"
        return headers

    # --- history reads -----------------------------------------------------

    async def fetch_executions(
        self,
        product: str,
        symbol: str,
        *,
        cursor: str | None = None,
        start_time: int | None = None,
        limit: int = MAX_HISTORY,
    ) -> tuple[list[BybitExecution], str | None]:
        """``execution/list`` — this account's fills, and where the next page is.

        ``execType=Trade`` is sent rather than filtered here. The same endpoint
        carries funding, ADL, liquidation and delivery rows, and letting the
        venue drop them costs a query parameter where doing it locally costs
        bandwidth, pages and rate-limit budget shared with live trading.

        Returns Bybit's own ``nextPageCursor``, opaque and passed straight
        back. No arithmetic on it here: how a venue numbers its pages is the
        adapter's business, which is what lets the caller treat every venue's
        cursor as one string.
        """
        params: dict[str, Any] = {
            "category": product,
            "symbol": symbol,
            "execType": EXEC_TYPE_TRADE,
            "limit": min(limit, MAX_HISTORY),
        }
        if cursor:
            params["cursor"] = cursor
        elif start_time is not None:
            params["startTime"] = start_time
        result = await self._get(ch.EXECUTION_LIST_PATH, params)
        rows = [
            BybitExecution.model_validate(row) for row in result.get("list") or []
        ]
        return rows, (result.get("nextPageCursor") or None)

    async def fetch_order_history(
        self,
        product: str,
        symbol: str,
        *,
        cursor: str | None = None,
        start_time: int | None = None,
        limit: int = MAX_HISTORY,
    ) -> tuple[list[BybitOrderUpdate], str | None]:
        """``order/history`` — orders that have finished, newest first.

        Not for attribution: Bybit puts ``orderLinkId`` on the execution rows
        themselves, so a fill re-read from this venue already knows whose it
        was — unlike Binance, where a trade row carries no client order id at
        all. This is read to make orders placed *outside* the platform visible,
        which no execution of ours would ever mention.
        """
        params: dict[str, Any] = {
            "category": product,
            "symbol": symbol,
            "limit": min(limit, MAX_HISTORY),
        }
        if cursor:
            params["cursor"] = cursor
        elif start_time is not None:
            params["startTime"] = start_time
        result = await self._get(ch.ORDER_HISTORY_PATH, params)
        rows = [
            BybitOrderUpdate.model_validate(row) for row in result.get("list") or []
        ]
        return rows, (result.get("nextPageCursor") or None)

    # --- recon reads -------------------------------------------------------

    async def fetch_open_orders(
        self, product: str, symbol: str | None = None
    ) -> list[BybitOrderUpdate]:
        """``order/realtime`` — orders still working, for one symbol or all.

        ``settleCoin`` is what makes the account-wide form work on the contract
        books: Bybit refuses an unfiltered query there and wants either a
        symbol or a settle coin, while spot is happy with neither.
        """
        params: dict[str, Any] = {"category": product, "limit": 50}
        if symbol:
            params["symbol"] = symbol
        elif product != SPOT:
            params["settleCoin"] = "USDT"
        result = await self._get(ch.ORDER_REALTIME_PATH, params)
        return [
            BybitOrderUpdate.model_validate(row) for row in result.get("list") or []
        ]

    async def fetch_order(
        self,
        product: str,
        *,
        symbol: str | None = None,
        order_id: str | None = None,
        order_link_id: str | None = None,
    ) -> BybitOrderUpdate | None:
        """What became of one order, live or finished.

        Two endpoints, because Bybit splits them: ``order/realtime`` knows only
        open orders, and an order that filled or was cancelled moves to
        ``order/history`` — where it stays for a bounded window. ``None`` means
        neither has it, which is an answer: for an order we never saw an ack
        for, the submit never landed.
        """
        params: dict[str, Any] = {"category": product}
        if symbol:
            params["symbol"] = symbol
        if order_id:
            params["orderId"] = order_id
        if order_link_id:
            params["orderLinkId"] = order_link_id
        for path in (ch.ORDER_REALTIME_PATH, ch.ORDER_HISTORY_PATH):
            try:
                result = await self._get(path, params)
            except BybitRestError as exc:
                if exc.not_found:
                    continue
                raise
            rows = result.get("list") or []
            if rows:
                return BybitOrderUpdate.model_validate(rows[0])
        return None

    async def fetch_balances(
        self, *, account_type: str = UNIFIED, coin: str | None = None
    ) -> list[Balance]:
        """``account/wallet-balance`` — the balance sheet, per coin.

        A unified account answers one row holding every coin, so the outer list
        is flattened away here.
        """
        params: dict[str, Any] = {"accountType": account_type}
        if coin:
            params["coin"] = coin
        result = await self._get(ch.WALLET_BALANCE_PATH, params)
        balances: list[Balance] = []
        for row in result.get("list") or []:
            balances.extend(BybitWallet.model_validate(row).to_balances())
        return balances

    async def fetch_position_rows(
        self, product: str, symbol: str | None = None
    ) -> list[BybitPosition]:
        """``position/list`` — open contracts. Empty on spot, which has none.

        Venue-native: turning a row into a shared
        :class:`~mft.exchange.oms.Position` needs the instrument's ticker, and
        only the symbol plane can say what it is — so the rows come back as
        Bybit sent them and the connector resolves each one.

        A flat position (``size == 0``) is dropped here: Bybit keeps reporting
        a symbol after it is closed, and an OMS reading that as a position
        would hold a row that says nothing. Callers that need the configured
        leverage on a flat book use :meth:`fetch_leverage_row` instead.
        """
        if product == SPOT:
            return []
        params: dict[str, Any] = {"category": product}
        if symbol:
            params["symbol"] = symbol
        else:
            params["settleCoin"] = "USDT"
        result = await self._get(ch.POSITION_LIST_PATH, params)
        return [
            row
            for row in (
                BybitPosition.model_validate(raw)
                for raw in result.get("list") or []
            )
            if row.size > 0
        ]

    async def fetch_leverage_row(
        self, product: str, symbol: str
    ) -> BybitPosition:
        """``position/list`` for one symbol, including a flat book.

        Bybit returns the configured leverage even when ``size`` is zero, but
        only when ``symbol`` is passed — without it the list is open positions
        only. This is the read :meth:`fetch_leverage` needs.
        """
        if product == SPOT:
            raise ExchangeError("spot has no leverage")
        if not symbol:
            raise ValueError("symbol is required")
        result = await self._get(
            ch.POSITION_LIST_PATH, {"category": product, "symbol": symbol}
        )
        rows = [
            BybitPosition.model_validate(raw)
            for raw in result.get("list") or []
        ]
        if not rows:
            raise ExchangeError(
                f"Bybit position/list returned no row for {symbol}"
            )
        return rows[0]

    # --- order entry (fallback) --------------------------------------------

    async def place_order(
        self,
        *,
        category: str,
        symbol: str,
        side: str,
        order_type: str,
        qty: Decimal | str,
        price: Decimal | str | None = None,
        time_in_force: str | None = None,
        order_link_id: str | None = None,
        market_unit: str | None = None,
        **extra: Any,
    ) -> BybitOrderAck:
        """``POST /v5/order/create`` — the trade socket's call over HTTP.

        Same arguments and same answer as
        :meth:`~mft.exchange.bybit.trade.BybitTradeSocket.place_order`,
        including the ``market_unit`` trap on a spot market buy.
        """
        args: dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": str(side).strip().title(),
            "orderType": str(order_type).strip().title(),
            "qty": qty,
        }
        if price is not None:
            args["price"] = price
        if time_in_force:
            args["timeInForce"] = time_in_force
        if order_link_id:
            args["orderLinkId"] = order_link_id
        if market_unit:
            args["marketUnit"] = market_unit
        args.update(extra)
        return BybitOrderAck.model_validate(
            await self._post(ch.ORDER_CREATE_PATH, args) or {}
        )

    async def cancel_order(
        self,
        *,
        category: str,
        symbol: str,
        order_id: str | None = None,
        order_link_id: str | None = None,
        **extra: Any,
    ) -> BybitOrderAck:
        """``POST /v5/order/cancel`` — by Bybit's id or by the id we gave it."""
        if not order_id and not order_link_id:
            raise BybitRestError(
                None,
                "Bybit cancel needs orderId or orderLinkId, got neither",
                op=ch.ORDER_CANCEL_PATH,
            )
        args: dict[str, Any] = {"category": category, "symbol": symbol}
        if order_id:
            args["orderId"] = order_id
        if order_link_id:
            args["orderLinkId"] = order_link_id
        args.update(extra)
        return BybitOrderAck.model_validate(
            await self._post(ch.ORDER_CANCEL_PATH, args) or {}
        )


__all__ = [
    "MAX_INSTRUMENT_PAGE",
    "MAX_KLINES",
    "UNIFIED",
    "BybitPublicRest",
    "BybitRest",
]
