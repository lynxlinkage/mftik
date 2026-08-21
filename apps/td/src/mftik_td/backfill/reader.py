"""Venue history readers — the account reads, on the cheapest transport each has.

One reader per venue, built from a credential, used and closed. Deliberately
not the trading connector: that one opens sockets it needs for order entry and
for the user data stream, and a batch read on a schedule has no use for either.
What a reader holds is the least a venue will accept for "tell me what this
account did".

Every venue paginates differently and the executor knows about none of it. A
cursor is one opaque string: a trade id on Binance, Bybit's own page token, and
on Gate — which numbers pages rather than handing out either — the window and
the position in it together. Keeping that arithmetic inside each adapter is
what stops the walk from growing a branch per venue.

Support is by method, not by declaration: a venue this cannot read has no
reader, and the executor leaves its cursors where they are. Paper is the one
that stays that way — its book is invented in another process and there is no
venue to re-read it from, so its record is whatever TD caught and the board
goes on calling it provisional. That is the honest answer, not a gap.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol, TypeVar

from mftik.exchange import venues
from mftik.exchange.binance.future.rest import MAX_HISTORY as BINANCE_FUTURE_MAX
from mftik.exchange.binance.future.rest import BinanceFutureRest
from mftik.exchange.binance.spot.rest import MAX_ROWS as BINANCE_SPOT_MAX
from mftik.exchange.binance.spot.rest import BinanceSpotRest
from mftik.exchange.bybit.protocol import LINEAR, SPOT
from mftik.exchange.bybit.rest import MAX_HISTORY as BYBIT_MAX
from mftik.exchange.bybit.rest import BybitRest
from mftik.exchange.gate.future.rest import MAX_HISTORY as GATE_FUTURES_MAX
from mftik.exchange.gate.future.rest import GateFuturesRest
from mftik.exchange.gate.spot.rest import MAX_HISTORY as GATE_MAX
from mftik.exchange.gate.spot.rest import GateSpotRest
from mftik.exchange.models import Fill, Order
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.symbols import SymbolClient

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Rows asked for per page. The venues cap this; readers clamp to their own.
DEFAULT_PAGE = 1000


class NoHistoryReaderError(Exception):
    """This venue cannot be re-read, so its record stays provisional."""


@dataclass(frozen=True)
class HistoryPage[T]:
    """One page of history, and where the next one starts.

    ``next_cursor`` is opaque to everything but the reader that produced it —
    a trade id on one venue, an order id on another, a timestamp on a third.
    Keeping the arithmetic inside the adapter is what stops the executor from
    growing a branch per venue, and what lets a cursor be stored as a string.

    ``None`` means the walk is drained: there was nothing after this page.
    """

    rows: list[T] = field(default_factory=list)
    next_cursor: str | None = None


class TradeHistoryReader(Protocol):
    """What a venue must serve for its history to be re-readable.

    ``connect`` and ``close`` are required; the two reads are not, and a venue
    that serves only one is still worth having — orders alone make external
    activity visible even where executions cannot be recovered.

    ``pages_newest_first`` is not a formatting detail. A walk that pages
    *backwards* has verified ``[oldest row read, ceiling]`` after each page — an
    interval whose lower bound is still moving — and a single
    ``confirmed_through_ts`` cannot say that. The executor reads this to know
    that such a walk may not advance its line until it drains. Reversing rows
    inside a page would not help: page two still holds older rows than page one.
    """

    venue: str
    #: True when page one is the newest rows. Verified against each venue.
    pages_newest_first: bool
    #: Most rows this venue will return per page, whatever was asked for.
    #: Compared against to decide "drained", so it must be the venue's cap and
    #: not the caller's request — a full page read as a short one would confirm
    #: an entire unread window.
    max_page: int

    async def connect(self) -> None: ...

    async def close(self) -> None: ...


class BinanceSpotHistoryReader:
    """Binance spot's ``myTrades`` and ``allOrders``, in canonical form.

    Both endpoints are per-symbol — Binance requires it — and both paginate by
    id. Ids are monotonic per symbol and have no window-length cap, where a
    time range has both and cannot separate two trades in one millisecond, so
    time is used only to open a walk that has no id to resume from.
    """

    venue = venues.BINANCE.name
    pages_newest_first = False
    max_page = BINANCE_SPOT_MAX

    def __init__(
        self,
        *,
        symbols: SymbolClient,
        rest: BinanceSpotRest,
    ) -> None:
        self.symbols = symbols
        self.rest = rest

    async def connect(self) -> None:
        await self.rest.connect()

    async def close(self) -> None:
        await self.rest.close()

    async def _symbol(self, ticker: UniversalTicker) -> str:
        if ticker.venue != self.venue:
            raise ValueError(
                f"{self.venue} reader was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker)

    async def fetch_my_trades(
        self,
        ticker: UniversalTicker,
        *,
        cursor: str | None = None,
        since_ts: float | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> HistoryPage[Fill]:
        """Executions from ``cursor`` forward, oldest first.

        The fills carry no ``client_order_id``: Binance does not put one on a
        trade row, and inventing one from the order id would make an execution
        look attributable when it is not. Tying them back is the executor's
        job, through the orders read.
        """
        limit = min(limit, self.max_page)
        symbol = await self._symbol(ticker)
        rows = await self.rest.fetch_my_trades(
            symbol,
            from_id=int(cursor) if cursor is not None else None,
            start_time=_ms(since_ts) if cursor is None else None,
            limit=limit,
        )
        fills = [row.to_fill(ticker) for row in rows]
        return HistoryPage(
            rows=fills,
            # ``fromId`` is inclusive, so the next page starts one past the
            # highest id seen — re-reading it would be harmless and wasteful.
            next_cursor=(
                str(max(int(row.trade_id) for row in rows) + 1)
                if len(rows) >= limit
                else None
            ),
        )

    async def fetch_orders(
        self,
        ticker: UniversalTicker,
        *,
        cursor: str | None = None,
        since_ts: float | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> HistoryPage[Order]:
        """Orders from ``cursor`` forward, ours and everybody else's.

        The counterpart to :meth:`fetch_my_trades`, and not optional alongside
        it — this is the only read that carries a ``client_order_id``, so it is
        the only one an execution can be attributed through.
        """
        limit = min(limit, self.max_page)
        symbol = await self._symbol(ticker)
        rows = await self.rest.fetch_orders(
            symbol,
            from_order_id=int(cursor) if cursor is not None else None,
            start_time=_ms(since_ts) if cursor is None else None,
            limit=limit,
        )
        orders = [row.to_order(ticker) for row in rows]
        return HistoryPage(
            rows=orders,
            next_cursor=(
                str(max(int(row.order_id) for row in rows) + 1)
                if len(rows) >= limit
                else None
            ),
        )


def _ms(ts: float | None) -> int | None:
    """Epoch seconds → milliseconds, which is what Binance and Bybit want."""
    return None if ts is None else int(ts * 1000)


def _sec(ts: float | None) -> int | None:
    """Epoch seconds, truncated. Gate's ``from`` is in seconds, not millis.

    Its own payloads say so — a candlestick row opens with a ten-digit time,
    and every millisecond field it sends is spelled ``*_time_ms``. Worth its own
    helper next to :func:`_ms` because the two are one keystroke apart and the
    venue will not tell you which one it got: ``from`` a thousand times too
    large is a timestamp in the year 58,000, and Gate answers that with ``200``
    and an empty array rather than an error.
    """
    return None if ts is None else int(ts)


class BinanceFutureHistoryReader:
    """Binance USDⓈ-M's ``userTrades`` and ``allOrders``.

    The same shape as spot — per-symbol, paginated by id — on a different host
    and a separate credential: Binance's futures plane is its own account, so a
    spot key here would fail its signature rather than read the wrong book.

    The venue reports a ``realizedPnl`` on every trade row and it is dropped on
    the way through. Binance computes it against the *account's* position
    basis, so a fill closing what another session opened carries a figure
    derived from executions this one never made — stored on a row that names a
    ``session_id`` it would be summed per session and be wrong.
    """

    venue = venues.BINANCE_FUTURE.name
    pages_newest_first = False
    max_page = BINANCE_FUTURE_MAX

    def __init__(self, *, symbols: SymbolClient, rest: BinanceFutureRest) -> None:
        self.symbols = symbols
        self.rest = rest

    async def connect(self) -> None:
        await self.rest.connect()

    async def close(self) -> None:
        await self.rest.close()

    async def _symbol(self, ticker: UniversalTicker) -> str:
        if ticker.venue != self.venue:
            raise ValueError(
                f"{self.venue} reader was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker)

    async def fetch_my_trades(
        self,
        ticker: UniversalTicker,
        *,
        cursor: str | None = None,
        since_ts: float | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> HistoryPage[Fill]:
        limit = min(limit, self.max_page)
        symbol = await self._symbol(ticker)
        rows = await self.rest.fetch_my_trades(
            symbol,
            from_id=int(cursor) if cursor is not None else None,
            start_time=_ms(since_ts) if cursor is None else None,
            limit=limit,
        )
        return HistoryPage(
            rows=[row.to_fill(ticker) for row in rows],
            next_cursor=(
                str(max(int(row.trade_id) for row in rows) + 1)
                if len(rows) >= limit
                else None
            ),
        )

    async def fetch_orders(
        self,
        ticker: UniversalTicker,
        *,
        cursor: str | None = None,
        since_ts: float | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> HistoryPage[Order]:
        limit = min(limit, self.max_page)
        symbol = await self._symbol(ticker)
        rows = await self.rest.fetch_orders(
            symbol,
            from_order_id=int(cursor) if cursor is not None else None,
            start_time=_ms(since_ts) if cursor is None else None,
            limit=limit,
        )
        return HistoryPage(
            rows=[row.to_order(ticker) for row in rows],
            next_cursor=(
                str(max(int(row.order_id) for row in rows) + 1)
                if len(rows) >= limit
                else None
            ),
        )


class BybitHistoryReader:
    """Bybit's ``execution/list`` and ``order/history``.

    The one venue that paginates by an opaque cursor of its own, which is what
    ``HistoryPage.next_cursor`` was shaped for: it is passed straight back with
    no arithmetic, the same way it arrived.

    Non-trade rows are excluded by the venue rather than here —
    ``execType=Trade`` on the request. The same endpoint carries funding, ADL,
    liquidation and delivery, and those belong in ``cash_flows`` rather than
    among executions; letting Bybit drop them costs a parameter where filtering
    locally costs pages and rate-limit budget shared with live trading.

    Bybit's execution rows carry ``orderLinkId``, so a fill re-read here is
    already attributable and does not wait on the orders walk. The orders are
    read anyway, to surface what was placed outside this platform.
    """

    venue = venues.BYBIT.name
    #: Verified against the live venue: execution/list and order/history both
    #: answer newest first.
    pages_newest_first = True
    max_page = BYBIT_MAX

    def __init__(self, *, symbols: SymbolClient, rest: BybitRest) -> None:
        self.symbols = symbols
        self.rest = rest

    async def connect(self) -> None:
        await self.rest.connect()

    async def close(self) -> None:
        await self.rest.close()

    async def _pair(self, ticker: UniversalTicker) -> tuple[str, str]:
        """``(product, symbol)`` — Bybit takes the book as a parameter.

        One credential covers every category on this venue, so which book a
        read is about travels beside the symbol rather than in the client.
        """
        if ticker.venue != self.venue:
            raise ValueError(
                f"{self.venue} reader was handed a {ticker.venue} ticker: {ticker}"
            )
        product = LINEAR if ticker.category is Category.PERP else SPOT
        return product, await self.symbols.exch_ticker(ticker)

    async def fetch_my_trades(
        self,
        ticker: UniversalTicker,
        *,
        cursor: str | None = None,
        since_ts: float | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> HistoryPage[Fill]:
        limit = min(limit, self.max_page)
        product, symbol = await self._pair(ticker)
        rows, next_cursor = await self.rest.fetch_executions(
            product,
            symbol,
            cursor=cursor,
            start_time=_ms(since_ts) if cursor is None else None,
            limit=limit,
        )
        # ``is_fill`` is belt and braces: the request already asked the venue
        # for trades only, and a row that is not one would be a surprise worth
        # dropping rather than booking.
        return HistoryPage(
            rows=[row.to_fill(ticker) for row in rows if row.is_fill],
            next_cursor=next_cursor,
        )

    async def fetch_orders(
        self,
        ticker: UniversalTicker,
        *,
        cursor: str | None = None,
        since_ts: float | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> HistoryPage[Order]:
        limit = min(limit, self.max_page)
        product, symbol = await self._pair(ticker)
        rows, next_cursor = await self.rest.fetch_order_history(
            product,
            symbol,
            cursor=cursor,
            start_time=_ms(since_ts) if cursor is None else None,
            limit=limit,
        )
        return HistoryPage(
            rows=[row.to_order(ticker) for row in rows],
            next_cursor=next_cursor,
        )


class GateSpotHistoryReader:
    """Gate spot's ``my_trades`` and ``orders``.

    The weakest pagination of the three: Gate numbers pages rather than
    handing out an id or a cursor, and a page number only means anything
    against a fixed query. So the cursor carried here is ``"{since_s}:{page}"``
    — the window *and* the position in it — which keeps the walk stable for a
    run and lets the next one start again from the settlement line. Opaque to
    the executor either way, which is the point of the cursor being a string.

    The window is in **seconds**. Gate is the only venue here that wants them,
    and asking in milliseconds is not an error to it: ``from`` a thousand times
    too large lands beyond any trade that will ever exist, so it answers ``200``
    with ``[]``. A walk reading that sees a short page, calls itself drained,
    and moves the settlement line to the ceiling — the account marked settled
    on history nobody read. Nothing downstream can tell that apart from an
    account that genuinely traded nothing, which is why it is spelled out here
    and pinned by :func:`_sec` rather than left to whoever edits this next.

    Gate trade rows carry ``text``, our own client order id, so an execution
    re-read here arrives already attributable — as Bybit's do, and as neither
    Binance market's do.
    """

    venue = venues.GATE.name
    #: Verified against the live venue: my_trades and orders both answer
    #: newest first.
    pages_newest_first = True
    max_page = GATE_MAX

    def __init__(self, *, symbols: SymbolClient, rest: GateSpotRest) -> None:
        self.symbols = symbols
        self.rest = rest

    async def connect(self) -> None:
        await self.rest.connect()

    async def close(self) -> None:
        await self.rest.close()

    async def _pair(self, ticker: UniversalTicker) -> str:
        if ticker.venue != self.venue:
            raise ValueError(
                f"{self.venue} reader was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker)

    @staticmethod
    def _resume(cursor: str | None, since_ts: float | None) -> tuple[int, int | None]:
        """``(page, since_s)`` from either half of the pair."""
        if cursor:
            window, _, page = cursor.partition(":")
            return max(1, int(page or 1)), int(window) if window else None
        return 1, _sec(since_ts)

    @staticmethod
    def _next(page: int, since: int | None, served: int, limit: int) -> str | None:
        if served < limit:
            return None
        return f"{since if since is not None else ''}:{page + 1}"

    async def fetch_my_trades(
        self,
        ticker: UniversalTicker,
        *,
        cursor: str | None = None,
        since_ts: float | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> HistoryPage[Fill]:
        limit = min(limit, self.max_page)
        pair = await self._pair(ticker)
        page, since = self._resume(cursor, since_ts)
        rows = await self.rest.fetch_my_trades(
            pair, page=page, since=since, limit=limit
        )
        return HistoryPage(
            rows=[row.to_fill(ticker) for row in rows],
            next_cursor=self._next(page, since, len(rows), limit),
        )

    async def fetch_orders(
        self,
        ticker: UniversalTicker,
        *,
        cursor: str | None = None,
        since_ts: float | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> HistoryPage[Order]:
        limit = min(limit, self.max_page)
        pair = await self._pair(ticker)
        page, since = self._resume(cursor, since_ts)
        rows = await self.rest.fetch_orders(
            pair, page=page, since=since, limit=limit
        )
        return HistoryPage(
            rows=[row.to_order(ticker) for row in rows],
            next_cursor=self._next(page, since, len(rows), limit),
        )


class GateFuturesHistoryReader:
    """Gate USDT-perp ``my_trades`` and ``orders``.

    Offset pagination against a fixed ``from`` window, same seconds-not-millis
    trap as spot. Cursor is ``"{since_s}:{offset}"``. Trade rows carry
    ``text``, so fills arrive already attributable.
    """

    venue = venues.GATE_FUTURES.name
    pages_newest_first = True
    max_page = GATE_FUTURES_MAX

    def __init__(self, *, symbols: SymbolClient, rest: GateFuturesRest) -> None:
        self.symbols = symbols
        self.rest = rest

    async def connect(self) -> None:
        await self.rest.connect()

    async def close(self) -> None:
        await self.rest.close()

    async def _pair(self, ticker: UniversalTicker) -> str:
        if ticker.venue != self.venue:
            raise ValueError(
                f"{self.venue} reader was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker)

    async def _multiplier(self, ticker: UniversalTicker):
        size = await self.symbols.contract_size(ticker)
        if size is None or size <= 0:
            raise ValueError(f"no contract_size for {ticker}")
        return size

    @staticmethod
    def _resume(cursor: str | None, since_ts: float | None) -> tuple[int, int | None]:
        if cursor:
            window, _, offset = cursor.partition(":")
            return max(0, int(offset or 0)), int(window) if window else None
        return 0, _sec(since_ts)

    @staticmethod
    def _next(offset: int, since: int | None, served: int, limit: int) -> str | None:
        if served < limit:
            return None
        return f"{since if since is not None else ''}:{offset + served}"

    async def fetch_my_trades(
        self,
        ticker: UniversalTicker,
        *,
        cursor: str | None = None,
        since_ts: float | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> HistoryPage[Fill]:
        limit = min(limit, self.max_page)
        pair = await self._pair(ticker)
        size = await self._multiplier(ticker)
        offset, since = self._resume(cursor, since_ts)
        rows = await self.rest.fetch_my_trades(
            pair, offset=offset, since=since, limit=limit
        )
        return HistoryPage(
            rows=[row.to_fill(ticker, size) for row in rows],
            next_cursor=self._next(offset, since, len(rows), limit),
        )

    async def fetch_orders(
        self,
        ticker: UniversalTicker,
        *,
        cursor: str | None = None,
        since_ts: float | None = None,
        limit: int = DEFAULT_PAGE,
    ) -> HistoryPage[Order]:
        limit = min(limit, self.max_page)
        pair = await self._pair(ticker)
        size = await self._multiplier(ticker)
        offset, since = self._resume(cursor, since_ts)
        rows = await self.rest.fetch_orders(
            pair, offset=offset, since=since, limit=limit
        )
        return HistoryPage(
            rows=[row.to_order(ticker, size) for row in rows],
            next_cursor=self._next(offset, since, len(rows), limit),
        )


class HistoryReaderFactory:
    """Venue name → reader. The only place the backfill names a venue.

    Built per run and closed after it. Unlike the fetch plane's readers, these
    are not kept: those are keyed by venue and answer interactive queries,
    where these are keyed by *account* and answer a batch job. Holding one open
    per credential would be a connection each for work that runs on a timer.
    """

    def __init__(self, symbols: SymbolClient) -> None:
        self._symbols = symbols

    async def create(self, venue: str, row: Any) -> TradeHistoryReader:
        """Build a reader for ``venue`` from an ``apis`` row's credential."""
        if venue == venues.BINANCE.name:
            return BinanceSpotHistoryReader(
                symbols=self._symbols,
                rest=BinanceSpotRest(
                    api_key=row.api_key, api_secret=row.api_secret
                ),
            )
        if venue == venues.BINANCE_FUTURE.name:
            return BinanceFutureHistoryReader(
                symbols=self._symbols,
                rest=BinanceFutureRest(
                    api_key=row.api_key, api_secret=row.api_secret
                ),
            )
        if venue == venues.BYBIT.name:
            return BybitHistoryReader(
                symbols=self._symbols,
                rest=BybitRest(
                    api_key=row.api_key, api_secret=row.api_secret
                ),
            )
        if venue == venues.GATE.name:
            return GateSpotHistoryReader(
                symbols=self._symbols,
                rest=GateSpotRest(
                    api_key=row.api_key, api_secret=row.api_secret
                ),
            )
        if venue == venues.GATE_FUTURES.name:
            return GateFuturesHistoryReader(
                symbols=self._symbols,
                rest=GateFuturesRest(
                    api_key=row.api_key, api_secret=row.api_secret
                ),
            )
        # Paper lands here, and should: its book is invented tick by tick in
        # another process and there is no venue to re-read it from. A paper
        # account's record is whatever TD caught, and calling it provisional
        # forever is the honest answer rather than a gap to close.
        raise NoHistoryReaderError(
            f"no history reader for venue {venue!r}; its record stays provisional"
        )


__all__ = [
    "DEFAULT_PAGE",
    "BinanceFutureHistoryReader",
    "BinanceSpotHistoryReader",
    "BybitHistoryReader",
    "GateFuturesHistoryReader",
    "GateSpotHistoryReader",
    "HistoryPage",
    "HistoryReaderFactory",
    "NoHistoryReaderError",
    "TradeHistoryReader",
]
