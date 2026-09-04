"""Per-venue readers, composed here rather than behind a shared interface.

Each venue gets its own reader, assembled from the pieces its exchange module
offers. Gate needs REST and only REST: ``spot.candlesticks`` pushes the window
in progress and never what came before it, and ``spot.order_book`` pushes on a
timer rather than on demand — so both reads have to be asked for, and asking
has nothing to do with the socket.

Binance is the venue that shows why that is a per-venue decision rather than a
rule. It answers the same reads on its WebSocket API, so its reader composes a
socket where Gate's composes an HTTP client — and holds it open, because a
request/reply socket is a connection to the venue's read plane, not a
subscription to a feed. Nothing else has to know which of the two a venue is.

Binance's futures plane then shows that the decision is not even per *brand*:
the same company serves no candles at all over that market's WebSocket API, so
``BinanceFuture``'s reader is an HTTP one beside spot's socket.

That is why the composition lives on this side. ``mftik.exchange.<venue>``
publishes connectors, not a contract — see :mod:`mftik.exchange.base` — and the
shape below is what the fetch plane needs of them, stated by the fetch plane.

Nothing here opens a feed. A reader is built the first time its venue is asked
for and kept for the life of the process, so a query never waits on a
subscription and a venue nothing streams is queryable all the same.
"""

from __future__ import annotations

import logging
from typing import Protocol

from mftik.exchange import venues
from mftik.exchange.binance.delivery.protocol import BINANCE_DELIVERY_REST_URL
from mftik.exchange.binance.delivery.public import (
    venue_interval as binance_delivery_interval,
)
from mftik.exchange.binance.delivery.rest import BinanceDeliveryPublicRest
from mftik.exchange.binance.future.protocol import BINANCE_FUTURE_REST_URL
from mftik.exchange.binance.future.public import (
    FUNDING_CATEGORIES as BINANCE_FUTURE_FUNDING_CATEGORIES,
)
from mftik.exchange.binance.future.public import (
    venue_interval as binance_future_interval,
)
from mftik.exchange.binance.future.rest import BinanceFuturePublicRest
from mftik.exchange.binance.spot.client import BinanceSpotWsApi
from mftik.exchange.binance.spot.protocol import BINANCE_SPOT_WS_API_URL
from mftik.exchange.binance.spot.public import venue_interval as binance_interval
from mftik.exchange.bybit.protocol import (
    BYBIT_REST_URL,
    INVERSE,
    LINEAR,
    product_of,
)
from mftik.exchange.bybit.public import (
    OPEN_INTEREST_CATEGORIES as BYBIT_OPEN_INTEREST_CATEGORIES,
)
from mftik.exchange.bybit.public import venue_interval as bybit_interval
from mftik.exchange.bybit.rest import BybitPublicRest
from mftik.exchange.gate.future.public import GATE_FUTURES_INTERVALS
from mftik.exchange.gate.future.rest import (
    GATE_FUTURES_REST_URL,
    GateFuturesPublicRest,
)
from mftik.exchange.gate.spot.public import GATE_INTERVALS
from mftik.exchange.gate.spot.rest import GATE_SPOT_REST_URL, GateSpotPublicRest
from mftik.exchange.intervals import InvalidIntervalError, normalize_interval
from mftik.exchange.models import (
    BestQuote,
    FundingRate,
    Kline,
    OpenInterest,
    OrderBook,
)
from mftik.exchange.okx.protocol import OKX_REST_URL
from mftik.exchange.okx.protocol import product_of as okx_product_of
from mftik.exchange.okx.public import venue_interval as okx_interval
from mftik.exchange.okx.rest import OkxPublicRest
from mftik.exchange.symbols import SymbolResolver
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.symbols import SymbolClient

logger = logging.getLogger(__name__)


class VenueReader(Protocol):
    """What the fetch plane needs of a venue to answer a query.

    Every read is optional but ``connect``/``close``. A venue that cannot serve
    one has no method for it, and the plane refuses the query naming the venue
    rather than calling something that raises — same rule as the feeds.
    """

    venue: str

    async def connect(self) -> None: ...

    async def close(self) -> None: ...


class GateSpotReader:
    """Gate spot reads over REST, in canonical symbol and interval.

    Composes only :class:`GateSpotPublicRest`. The Gate connector that pairs it
    with a WebSocket exists for feeds and is not used here — connecting a
    socket to ask one REST question is the coupling this plane was built to
    avoid.
    """

    venue = "Gate"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        rest: GateSpotPublicRest | None = None,
        rest_url: str = GATE_SPOT_REST_URL,
    ) -> None:
        self.symbols = symbols
        self.rest = rest or GateSpotPublicRest(base_url=rest_url)

    async def connect(self) -> None:
        await self.rest.connect()

    async def close(self) -> None:
        await self.rest.close()

    async def _pair(self, ticker: UniversalTicker) -> str:
        """Gate's pair for one instrument — resolved through the plane.

        The ticker needs no resolving: it is what every payload out of here is
        stamped with, and the only thing that has to be looked up is the
        spelling that goes on the wire.
        """
        if ticker.venue != self.venue:
            raise ValueError(
                f"{self.venue} reader was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker)

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int
    ) -> list[Kline]:
        """Recent candles, oldest first, answering in the caller's spelling.

        The interval is translated on the way down and stamped back on the
        way up, so Gate's ``30d`` vocabulary does not escape this method; the
        symbol needs no stamping, since the payload carries the ticker it was
        asked under.
        """
        canonical_interval = normalize_interval(interval)
        gate_interval = GATE_INTERVALS.get(canonical_interval)
        if gate_interval is None:
            raise InvalidIntervalError(
                f"{self.venue} serves no {canonical_interval} candles; "
                f"supported: {sorted(GATE_INTERVALS)}"
            )
        pair = await self._pair(ticker)
        klines = await self.rest.fetch_klines(
            pair, gate_interval, ticker=ticker, limit=limit
        )
        return [
            kline.model_copy(update={"interval": canonical_interval})
            for kline in klines
        ]

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int
    ) -> OrderBook:
        """``GET /spot/order_book`` — a whole book, capped at ``depth``.

        Gate's reply carries no pair, so the caller's ticker is stamped on.
        """
        pair = await self._pair(ticker)
        return await self.rest.fetch_order_book(
            pair, ticker=ticker, depth=depth
        )

    async def fetch_best_quote(
        self, ticker: UniversalTicker
    ) -> BestQuote | None:
        """Top of book with sizes, or None when a side is empty.

        The same REST read as :meth:`fetch_order_book` at depth 1 — Gate serves
        no endpoint for the touch alone, and a second call to get two numbers
        out of a book we already have would be a round trip for nothing.

        None rather than zeros when a side has nothing resting. A caller asking
        for the touch is almost always checking whether its own price can rest
        against it, and a zero bid would answer that question wrongly rather
        than declining to answer it.
        """
        book = await self.fetch_order_book(ticker, depth=1)
        if not book.bids or not book.asks:
            return None
        bid, ask = book.bids[0], book.asks[0]
        return BestQuote(
            universal_ticker=book.universal_ticker,
            bid=bid.price,
            bid_qty=bid.qty,
            ask=ask.price,
            ask_qty=ask.qty,
            ts=book.ts,
        )


class GateFuturesReader:
    """Gate USDT-perp reads over REST, with sizes converted to base."""

    venue = "GateFutures"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        rest: GateFuturesPublicRest | None = None,
        rest_url: str = GATE_FUTURES_REST_URL,
    ) -> None:
        self.symbols = symbols
        self.rest = rest or GateFuturesPublicRest(base_url=rest_url)

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

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int
    ) -> list[Kline]:
        canonical_interval = normalize_interval(interval)
        gate_interval = GATE_FUTURES_INTERVALS.get(canonical_interval)
        if gate_interval is None:
            raise InvalidIntervalError(
                f"{self.venue} serves no {canonical_interval} candles; "
                f"supported: {sorted(GATE_FUTURES_INTERVALS)}"
            )
        pair = await self._pair(ticker)
        klines = await self.rest.fetch_klines(
            pair,
            gate_interval,
            ticker=ticker,
            contract_size=await self._multiplier(ticker),
            limit=limit,
        )
        return [
            kline.model_copy(update={"interval": canonical_interval})
            for kline in klines
        ]

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int
    ) -> OrderBook:
        pair = await self._pair(ticker)
        return await self.rest.fetch_order_book(
            pair,
            ticker=ticker,
            contract_size=await self._multiplier(ticker),
            depth=depth,
        )

    async def fetch_best_quote(
        self, ticker: UniversalTicker
    ) -> BestQuote | None:
        book = await self.fetch_order_book(ticker, depth=1)
        if not book.bids or not book.asks:
            return None
        bid, ask = book.bids[0], book.asks[0]
        return BestQuote(
            universal_ticker=book.universal_ticker,
            bid=bid.price,
            bid_qty=bid.qty,
            ask=ask.price,
            ask_qty=ask.qty,
            ts=book.ts,
        )

    async def fetch_funding_history(
        self, ticker: UniversalTicker, *, limit: int
    ) -> list[FundingRate]:
        """Settled rates, oldest first. Gate answers newest first; REST reverses."""
        pair = await self._pair(ticker)
        return await self.rest.fetch_funding_history(
            pair, ticker=ticker, limit=limit
        )

    async def fetch_open_interest(self, ticker: UniversalTicker) -> OpenInterest:
        """Current size, in base. Off the ticker row, not ``contract_stats``."""
        pair = await self._pair(ticker)
        return await self.rest.fetch_open_interest(
            pair,
            ticker=ticker,
            contract_size=await self._multiplier(ticker),
        )


class BinanceSpotReader:
    """Binance spot reads over the WebSocket API, in canonical symbol and interval.

    Composes an unauthenticated :class:`BinanceSpotWsApi` — the same socket the
    feed connector uses for its snapshot reads, built separately here so a
    query never depends on a feed session existing. Binance serves ``depth``,
    ``klines`` and the rest to anyone, so this holds no credentials.
    """

    venue = "Binance"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        api: BinanceSpotWsApi | None = None,
        ws_url: str = BINANCE_SPOT_WS_API_URL,
    ) -> None:
        self.symbols = symbols
        self.api = api or BinanceSpotWsApi(url=ws_url)

    async def connect(self) -> None:
        await self.api.connect()

    async def close(self) -> None:
        await self.api.close()

    async def _symbol(self, ticker: UniversalTicker) -> str:
        """Binance's symbol for one instrument — resolved through the plane.

        The ticker needs no resolving: it is what every payload out of here is
        stamped with, and the only thing that has to be looked up is the
        spelling that goes on the wire.
        """
        if ticker.venue != self.venue:
            raise ValueError(
                f"{self.venue} reader was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker)

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int
    ) -> list[Kline]:
        """Recent candles, oldest first, answering in the caller's spelling.

        The interval is translated on the way down and stamped back on the way
        up, so Binance's ``1M`` for a month does not escape this method.
        """
        canonical = normalize_interval(interval)
        native_interval = binance_interval(canonical)
        native = await self._symbol(ticker)
        klines = await self.api.fetch_klines(
            native, native_interval, ticker=ticker, limit=limit
        )
        return [
            kline.model_copy(update={"interval": canonical}) for kline in klines
        ]

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int
    ) -> OrderBook:
        """``depth`` — a whole book, capped at ``depth``.

        Binance's reply names neither the instrument nor a time, so the
        caller's ticker is stamped on and the book is dated on arrival.
        """
        native = await self._symbol(ticker)
        return await self.api.fetch_order_book(
            native, ticker=ticker, depth=depth
        )

    async def fetch_best_quote(self, ticker: UniversalTicker) -> BestQuote | None:
        """Top of book with sizes, or None when a side is empty.

        Read out of a depth-1 book rather than through ``ticker.book``: that
        method exists, but this is the same round trip and returns the sizes in
        the same shape as Gate's reader, so the two venues answer alike.

        None rather than zeros when a side has nothing resting. A caller asking
        for the touch is almost always checking whether its own price can rest
        against it, and a zero bid would answer that question wrongly rather
        than declining to answer it.
        """
        book = await self.fetch_order_book(ticker, depth=1)
        if not book.bids or not book.asks:
            return None
        bid, ask = book.bids[0], book.asks[0]
        return BestQuote(
            universal_ticker=book.universal_ticker,
            bid=bid.price,
            bid_qty=bid.qty,
            ask=ask.price,
            ask_qty=ask.qty,
            ts=book.ts,
        )


class BinanceFutureReader:
    """Binance USDⓈ-M futures reads over REST, in canonical symbol and interval.

    REST, where the spot reader holds a socket — and that is not a preference:
    the futures WebSocket API serves no ``klines`` at all, so a socket here
    could answer one of this reader's three questions. Composing
    :class:`BinanceFuturePublicRest` instead means one transport for all of
    them, and no connection held open between queries.
    """

    venue = "BinanceFuture"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        rest: BinanceFuturePublicRest | None = None,
        base_url: str = BINANCE_FUTURE_REST_URL,
    ) -> None:
        self.symbols = symbols
        self.rest = rest or BinanceFuturePublicRest(base_url=base_url)

    async def connect(self) -> None:
        await self.rest.connect()

    async def close(self) -> None:
        await self.rest.close()

    async def _symbol(self, ticker: UniversalTicker) -> str:
        """Binance's symbol for one instrument — resolved through the plane."""
        if ticker.venue != self.venue:
            raise ValueError(
                f"{self.venue} reader was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker)

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int
    ) -> list[Kline]:
        """Recent candles, oldest first, answering in the caller's spelling.

        The interval is translated on the way down and stamped back on the way
        up. Futures serves one window fewer than spot — there are no
        one-second candles here — and ``binance_future_interval`` refuses that
        one before the round trip.
        """
        canonical = normalize_interval(interval)
        native_interval = binance_future_interval(canonical)
        native = await self._symbol(ticker)
        klines = await self.rest.fetch_klines(
            native, native_interval, ticker=ticker, limit=limit
        )
        return [
            kline.model_copy(update={"interval": canonical}) for kline in klines
        ]

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int
    ) -> OrderBook:
        """``GET /fapi/v1/depth`` — a whole book, capped at ``depth``.

        Dated by Binance, unlike the spot reply, so nothing is stamped on here.
        """
        native = await self._symbol(ticker)
        return await self.rest.fetch_order_book(native, ticker=ticker, depth=depth)

    async def fetch_best_quote(self, ticker: UniversalTicker) -> BestQuote | None:
        """Top of book with sizes, or None when a side is empty.

        Read out of a depth-1 book, so the answer has the same shape as every
        other reader's. None rather than zeros when a side has nothing resting:
        a caller asking for the touch is almost always checking whether its own
        price can rest against it, and a zero bid would answer that question
        wrongly rather than declining to answer it.
        """
        book = await self.fetch_order_book(ticker, depth=1)
        if not book.bids or not book.asks:
            return None
        bid, ask = book.bids[0], book.asks[0]
        return BestQuote(
            universal_ticker=book.universal_ticker,
            bid=bid.price,
            bid_qty=bid.qty,
            ask=ask.price,
            ask_qty=ask.qty,
            ts=book.ts,
        )

    async def fetch_funding_history(
        self, ticker: UniversalTicker, *, limit: int
    ) -> list[FundingRate]:
        """Settled rates, oldest first. Dated futures have none.

        Guarded on the same set ``stream_funding_rate`` guards on, so a
        query and a subscribe refuse the same books.
        """
        if ticker.category not in BINANCE_FUTURE_FUNDING_CATEGORIES:
            raise NoReaderError(
                f"{self.venue} {ticker.category} serves no funding history"
            )
        native = await self._symbol(ticker)
        return await self.rest.fetch_funding_history(
            native, ticker=ticker, limit=limit
        )

    async def fetch_open_interest(self, ticker: UniversalTicker) -> OpenInterest:
        """Current size, already in base."""
        native = await self._symbol(ticker)
        return await self.rest.fetch_open_interest(native, ticker=ticker)


class BinanceDeliveryReader:
    """Binance COIN-M reads over REST, in canonical symbol and interval.

    REST, same reason as USD-M: dapi's WebSocket API serves no ``klines``.
    Book and quote sizes stay in contracts. Candles need
    ``quote_per_contract`` — dapi's volume columns are swapped relative to
    a linear bar, and ``contractSize`` is USD per contract, not base.
    """

    venue = "BinanceDelivery"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        rest: BinanceDeliveryPublicRest | None = None,
        base_url: str = BINANCE_DELIVERY_REST_URL,
    ) -> None:
        self.symbols = symbols
        self.rest = rest or BinanceDeliveryPublicRest(base_url=base_url)

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

    async def _quote_per_contract(self, ticker: UniversalTicker):
        size = await self.symbols.contract_size(ticker)
        if size is None or size <= 0:
            raise ValueError(f"no contract_size for {ticker}")
        return size

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int
    ) -> list[Kline]:
        """Recent candles, oldest first, answering in the caller's spelling.

        Refuses when the plane has no ``contract_size``: a linear read of a
        dapi bar is silently wrong rather than short.
        """
        canonical = normalize_interval(interval)
        native_interval = binance_delivery_interval(canonical)
        native = await self._symbol(ticker)
        klines = await self.rest.fetch_klines(
            native,
            native_interval,
            ticker=ticker,
            quote_per_contract=await self._quote_per_contract(ticker),
            limit=limit,
        )
        return [kline.model_copy(update={"interval": canonical}) for kline in klines]

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int
    ) -> OrderBook:
        """``GET /dapi/v1/depth`` — a whole book, sizes in contracts."""
        native = await self._symbol(ticker)
        return await self.rest.fetch_order_book(native, ticker=ticker, depth=depth)

    async def fetch_best_quote(self, ticker: UniversalTicker) -> BestQuote | None:
        """Top of book with sizes, or None when a side is empty.

        Sizes stay in contracts. None rather than zeros when a side has
        nothing resting.
        """
        book = await self.fetch_order_book(ticker, depth=1)
        if not book.bids or not book.asks:
            return None
        bid, ask = book.bids[0], book.asks[0]
        return BestQuote(
            universal_ticker=book.universal_ticker,
            bid=bid.price,
            bid_qty=bid.qty,
            ask=ask.price,
            ask_qty=ask.qty,
            ts=book.ts,
        )

    async def fetch_funding_history(
        self, ticker: UniversalTicker, *, limit: int
    ) -> list[FundingRate]:
        """Settled rates, oldest first. Binance already answers that way."""
        native = await self._symbol(ticker)
        return await self.rest.fetch_funding_history(
            native, ticker=ticker, limit=limit
        )

    async def fetch_open_interest(self, ticker: UniversalTicker) -> OpenInterest:
        """Current size, in contracts. Distinct from every linear book."""
        native = await self._symbol(ticker)
        return await self.rest.fetch_open_interest(native, ticker=ticker)


class BybitReader:
    """Bybit reads over REST, across every category the venue trades.

    Composes an unauthenticated :class:`BybitPublicRest` — the same client the
    feed connector uses for its snapshot reads, built separately here so a
    query never depends on a feed session existing. Bybit serves all of these
    to anyone, so this holds no credentials.

    Unlike the other readers there is no per-venue market: the ticker names the
    book, and ``product_of`` turns it into the ``category`` every Bybit call
    carries. One reader answers for spot and perps alike.
    """

    venue = "Bybit"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        rest: BybitPublicRest | None = None,
        base_url: str = BYBIT_REST_URL,
    ) -> None:
        self.symbols = symbols
        self.rest = rest or BybitPublicRest(base_url=base_url)

    async def connect(self) -> None:
        await self.rest.connect()

    async def close(self) -> None:
        await self.rest.close()

    async def _resolve(self, ticker: UniversalTicker) -> tuple[str, str]:
        """``(venue symbol, product)`` — the two things every call needs."""
        if ticker.venue != self.venue:
            raise ValueError(
                f"{self.venue} reader was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker), product_of(ticker.category)

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int
    ) -> list[Kline]:
        """Recent candles, oldest first, answering in the caller's spelling.

        Bybit answers newest first and names its windows by the number of
        minutes; neither escapes this method.
        """
        canonical = normalize_interval(interval)
        native_interval = bybit_interval(canonical)
        native, product = await self._resolve(ticker)
        klines = await self.rest.fetch_klines(
            product, native, native_interval, ticker=ticker, limit=limit
        )
        return [
            kline.model_copy(update={"interval": canonical}) for kline in klines
        ]

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int
    ) -> OrderBook:
        """``orderbook`` — a whole book, capped at ``depth``, dated by Bybit."""
        native, product = await self._resolve(ticker)
        return await self.rest.fetch_order_book(
            product, native, ticker=ticker, depth=depth
        )

    async def fetch_best_quote(self, ticker: UniversalTicker) -> BestQuote | None:
        """Top of book with sizes, or None when a side is empty.

        Off ``tickers`` rather than a depth-1 book: the REST form carries
        ``bid1Price`` and ``ask1Price`` on every category — unlike the push,
        which omits them on spot — so this is one call instead of one plus a
        book, and it answers in the same shape as the other readers.

        None rather than zeros when a side has nothing resting. A caller asking
        for the touch is almost always checking whether its own price can rest
        against it, and a zero bid would answer that question wrongly rather
        than declining to answer it.
        """
        native, product = await self._resolve(ticker)
        row = await self.rest.fetch_ticker_row(product, native)
        if not row.bid or not row.ask or not row.bid_qty or not row.ask_qty:
            return None
        return BestQuote(
            universal_ticker=str(ticker),
            bid=row.bid,
            bid_qty=row.bid_qty,
            ask=row.ask,
            ask_qty=row.ask_qty,
        )

    async def fetch_funding_history(
        self, ticker: UniversalTicker, *, limit: int
    ) -> list[FundingRate]:
        """Settled rates, oldest first. Spot has none — refused before HTTP."""
        native, product = await self._resolve(ticker)
        if product not in {LINEAR, INVERSE}:
            raise NoReaderError(
                f"{self.venue} {product} serves no funding history"
            )
        return await self.rest.fetch_funding_history(
            product, native, ticker=ticker, limit=limit
        )

    async def fetch_open_interest(self, ticker: UniversalTicker) -> OpenInterest:
        """Current size, in base. Spot and options are refused before HTTP.

        A dated future is answered: that book has open interest, unlike
        funding, which only a perpetual settles.

        Guarded on the set ``stream_open_interest`` guards on, and not on
        a spot test of its own. I4 is that the query and the subscribe
        refuse the same books; two guards spelled differently drift, and
        the one that drifts wide sends an option ticker to the venue to
        come back as ``MD_VENUE_CALL_FAILED`` — which reads as "the venue
        broke" rather than "we do not serve this".
        """
        if ticker.category not in BYBIT_OPEN_INTEREST_CATEGORIES:
            raise NoReaderError(
                f"{self.venue} {ticker.category} serves no open interest"
            )
        native, product = await self._resolve(ticker)
        return await self.rest.fetch_open_interest(
            product, native, ticker=ticker
        )


class OkxReader:
    """OKX reads over REST, across every category the venue trades.

    Same unified-account shape as :class:`BybitReader`: the ticker names the
    book, and one reader answers for spot and perps alike.
    """

    venue = "Okx"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        rest: OkxPublicRest | None = None,
        base_url: str = OKX_REST_URL,
    ) -> None:
        self.symbols = symbols
        self.rest = rest or OkxPublicRest(base_url=base_url)

    async def connect(self) -> None:
        await self.rest.connect()

    async def close(self) -> None:
        await self.rest.close()

    async def _resolve(self, ticker: UniversalTicker) -> str:
        if ticker.venue != self.venue:
            raise ValueError(
                f"{self.venue} reader was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker)

    async def _multiplier(self, ticker: UniversalTicker):
        if ticker.category is not Category.PERP:
            return None
        size = await self.symbols.contract_size(ticker)
        if size is None or size <= 0:
            raise ValueError(f"no contract_size for {ticker}")
        return size

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int
    ) -> list[Kline]:
        canonical = normalize_interval(interval)
        bar = okx_interval(canonical)
        native = await self._resolve(ticker)
        klines = await self.rest.fetch_klines(
            native,
            bar,
            ticker=ticker,
            limit=limit,
            contract_size=await self._multiplier(ticker),
        )
        return [
            kline.model_copy(update={"interval": canonical}) for kline in klines
        ]

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int
    ) -> OrderBook:
        native = await self._resolve(ticker)
        return await self.rest.fetch_order_book(
            native,
            ticker=ticker,
            depth=depth,
            contract_size=await self._multiplier(ticker),
        )

    async def fetch_best_quote(self, ticker: UniversalTicker) -> BestQuote | None:
        native = await self._resolve(ticker)
        row = await self.rest.fetch_ticker_row(native)
        return row.to_best_quote(
            ticker, contract_size=await self._multiplier(ticker)
        )

    async def fetch_funding_history(
        self, ticker: UniversalTicker, *, limit: int
    ) -> list[FundingRate]:
        """Settled rates, oldest first. Non-SWAP is refused before HTTP."""
        if ticker.category is not Category.PERP:
            raise NoReaderError(
                f"{self.venue} {ticker.category} serves no funding history"
            )
        native = await self._resolve(ticker)
        return await self.rest.fetch_funding_history(
            native, ticker=ticker, limit=limit
        )

    async def fetch_open_interest(self, ticker: UniversalTicker) -> OpenInterest:
        """Current size, in base. Non-SWAP is refused before HTTP.

        The same books ``stream_open_interest`` serves
        (:data:`~mftik.exchange.okx.public.OPEN_INTEREST_PRODUCTS`), which
        on this venue is spot being refused. Tested on the category rather
        than the product because ``product_of`` raises on one this venue
        has no mapping for — ``Inverse`` — and I4 wants that refused by
        name, not as a venue call that failed.

        ``oiCcy`` is used as sent; the side-count calibration lives in
        ``docs/MdOpenInterest.md``.
        """
        if ticker.category is not Category.PERP:
            raise NoReaderError(
                f"{self.venue} {ticker.category} serves no open interest"
            )
        native = await self._resolve(ticker)
        size = await self.symbols.contract_size(ticker)
        if size is not None and size <= 0:
            size = None
        return await self.rest.fetch_open_interest(
            native,
            ticker=ticker,
            inst_type=okx_product_of(ticker.category),
            contract_size=size,
        )


class ReaderFactory(Protocol):
    """Builds the reader for a venue name."""

    async def create(self, venue: str) -> VenueReader:
        """Build (but do not connect) a reader, or raise if the venue has none."""


class NoReaderError(Exception):
    """Nothing here can answer reads for this venue.

    Distinct from an empty answer, and raised before any call: a caller has to
    be able to tell "cannot ask" from "asked, and there is none".
    """


class VenueReaderFactory:
    """Venue name → reader. The only place the fetch plane names a venue.

    Every difference between venues is settled by which reader gets built, so
    nothing downstream branches on the venue again.
    """

    def __init__(self, symbols: SymbolClient) -> None:
        self._symbols = symbols

    async def create(self, venue: str) -> VenueReader:
        if venue == venues.GATE.name:
            return GateSpotReader(symbols=self._symbols)
        if venue == venues.GATE_FUTURES.name:
            return GateFuturesReader(symbols=self._symbols)
        if venue == venues.BINANCE.name:
            return BinanceSpotReader(symbols=self._symbols)
        if venue == venues.BINANCE_FUTURE.name:
            return BinanceFutureReader(symbols=self._symbols)
        if venue == venues.BINANCE_DELIVERY.name:
            return BinanceDeliveryReader(symbols=self._symbols)
        if venue == venues.BYBIT.name:
            return BybitReader(symbols=self._symbols)
        if venue == venues.OKX.name:
            return OkxReader(symbols=self._symbols)
        if venue == venues.PAPER.name:
            # The paper engine's book lives in another process and its prices
            # are invented tick by tick; nothing here can be read out of band.
            raise NoReaderError("the paper venue serves no on-demand reads")
        raise NoReaderError(f"no reader for venue {venue!r}")
