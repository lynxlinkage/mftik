"""Per-venue connector + feed pumps."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from mftik.exchange.models import OrderBook, Ticker, Trade
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    MD_AGG_TRADE,
    MD_BEST_QUOTE,
    MD_FUNDING_RATE,
    MD_KLINE,
    MD_LIQUIDATION,
    MD_OPEN_INTEREST,
    MD_ORDERBOOK,
    MD_TICKER,
    MD_TRADE,
    UntypedEnvelope,
)

logger = logging.getLogger(__name__)


class MarketDataConnector(Protocol):
    """What MD needs of a venue, stated by MD rather than by the venue.

    ``mftik.exchange`` has no shared public interface on purpose — venues differ
    too much for one to be honest (see :mod:`mftik.exchange.base`). So the shape
    lives here, with the consumer, and holds only what every venue really does
    provide: a lifecycle and the three feeds nobody lacks.

    ``stream_kline``, ``stream_best_quote``, ``stream_agg_trades``,
    ``stream_liquidation``, ``stream_funding_rate`` and
    ``stream_open_interest`` are deliberately absent. Gate serves kline
    and best-quote and paper does not; only Binance has the aggregated
    tape; Bybit, OKX, GateFutures and ``BinanceFuture`` have
    liquidations; perpetual venues have funding. Open interest is the
    same optional: a venue that cannot should have no such method rather
    than one that raises — :meth:`VenueSession._open` looks for them and
    refuses the subscribe when they are missing, which is the same
    answer one venue short of the full set was always going to give.

    Streams are opened on a :class:`~mftik.exchange.tickers.UniversalTicker`, not
    a symbol. A unified-account venue is one connector serving several markets,
    and ``BTCUSDT`` alone does not say whether the spot book or the perp was
    meant.
    """

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    def stream_ticker(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]: ...

    def stream_trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]: ...

    def stream_order_book(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[OrderBook]: ...


TOPIC_ORDERBOOK = "orderbook"
TOPIC_TICKER = "ticker"
TOPIC_TRADE = "trade"
#: The same tape with a venue's own coalescing applied — one print per
#: aggressing order per price rather than one per match. Not every venue has
#: the concept, so a venue without it publishes no such stream and the
#: subscribe is refused by name, same as ``kline`` and ``bestquote``.
TOPIC_AGG_TRADE = "aggtrade"
TOPIC_BEST_QUOTE = "bestquote"
#: Public forced-liquidation prints. Bybit, OKX (SWAP only), GateFutures and
#: BinanceFuture publish them; a venue without the method refuses by name.
TOPIC_LIQUIDATION = "liquidation"
#: Predicted funding rate for the upcoming settlement. Perpetual venues
#: publish it; spot and paper have no method and the subscribe is refused
#: by name. A late joiner on a ticker-shared wire (Bybit, Gate) is silent
#: until the next rate-bearing delta — the pump is not REST-filled.
TOPIC_FUNDING_RATE = "funding_rate"
#: Current open interest. Contract venues that push it publish a stream;
#: Binance futures and every spot / paper book have no method and the
#: subscribe is refused by name. A late joiner on a ticker-shared wire
#: (Bybit, Gate) is silent until the next size-bearing delta — the pump
#: is not REST-filled.
TOPIC_OPEN_INTEREST = "open_interest"
#: Klines need an interval, and a feed key is only ``topic.ticker`` — so the
#: interval rides in the topic: ``kline_1m.Paper_Spot_BTCUSDT``. The split is
#: on ``.``, so the underscore here is not ambiguous with the ticker's.
KLINE_PREFIX = "kline_"

OnUpdate = Callable[[str, UniversalTicker, UntypedEnvelope], Awaitable[None]]


@dataclass
class Feed:
    """One active (topic, ticker) stream on a venue."""

    topic: str
    ticker: UniversalTicker
    task: asyncio.Task[None] | None = None
    stop: asyncio.Event = field(default_factory=asyncio.Event)


class VenueSession:
    """Owns one venue connector and its running feed pumps."""

    def __init__(
        self,
        venue: str,
        public: MarketDataConnector,
        *,
        on_update: OnUpdate,
    ) -> None:
        self.venue = venue
        self.public = public
        self._on_update = on_update
        self._feeds: dict[tuple[str, UniversalTicker], Feed] = {}
        self._started = False

    @property
    def feed_count(self) -> int:
        return len(self._feeds)

    async def start(self) -> None:
        if self._started:
            return
        await self.public.connect()
        self._started = True
        logger.info("MD venue started venue=%s", self.venue)

    async def stop(self) -> None:
        for key in list(self._feeds):
            await self.stop_feed(*key)
        if self._started:
            await self.public.close()
            self._started = False
        logger.info("MD venue stopped venue=%s", self.venue)

    async def ensure_feed(self, topic: str, ticker: UniversalTicker) -> None:
        key = (topic, ticker)
        if key in self._feeds:
            return
        # Opened here rather than inside the task so an unsupported topic or a
        # venue that does not publish this feed fails the subscribe call
        # instead of dying silently in a background pump.
        source, msg_type = self._open(topic, ticker)
        feed = Feed(topic=topic, ticker=ticker)
        feed.task = asyncio.create_task(
            self._pump(feed, source, msg_type),
            name=f"md-{topic}-{ticker}",
        )
        self._feeds[key] = feed
        logger.info("MD feed started topic=%s ticker=%s", topic, ticker)

    async def stop_feed(self, topic: str, ticker: UniversalTicker) -> None:
        feed = self._feeds.pop((topic, ticker), None)
        if feed is None:
            return
        feed.stop.set()
        if feed.task is not None:
            feed.task.cancel()
            await asyncio.gather(feed.task, return_exceptions=True)
        logger.info("MD feed stopped topic=%s ticker=%s", topic, ticker)

    def _stream(self, name: str) -> Any:
        """The connector's ``name`` stream, or a refusal naming the venue.

        Only the three universal feeds are on
        :class:`MarketDataConnector`; the rest a venue either has or has not.
        Absent reads as "this venue does not publish it", which is what the
        subscribe has to be told either way.
        """
        stream = getattr(self.public, name, None)
        if stream is None:
            raise ValueError(f"venue {self.venue!r} does not publish {name}")
        return stream

    def _open(
        self, topic: str, ticker: UniversalTicker
    ) -> tuple[AsyncIterator[Any], str]:
        """Resolve a feed topic to its venue stream and wire message type."""
        if ticker.venue != self.venue:
            raise ValueError(
                f"venue session {self.venue!r} was handed a "
                f"{ticker.venue!r} ticker: {ticker}"
            )
        if topic == TOPIC_ORDERBOOK:
            return self.public.stream_order_book(ticker), MD_ORDERBOOK
        if topic == TOPIC_TICKER:
            return self.public.stream_ticker(ticker), MD_TICKER
        if topic == TOPIC_TRADE:
            return self.public.stream_trades(ticker), MD_TRADE
        if topic == TOPIC_AGG_TRADE:
            return self._stream("stream_agg_trades")(ticker), MD_AGG_TRADE
        if topic == TOPIC_BEST_QUOTE:
            return self._stream("stream_best_quote")(ticker), MD_BEST_QUOTE
        if topic == TOPIC_LIQUIDATION:
            return self._stream("stream_liquidation")(ticker), MD_LIQUIDATION
        if topic == TOPIC_FUNDING_RATE:
            return self._stream("stream_funding_rate")(ticker), MD_FUNDING_RATE
        if topic == TOPIC_OPEN_INTEREST:
            return self._stream("stream_open_interest")(ticker), MD_OPEN_INTEREST
        if topic.startswith(KLINE_PREFIX):
            interval = topic[len(KLINE_PREFIX) :]
            if not interval:
                raise ValueError(
                    f"md kline topic needs an interval, got {topic!r} "
                    f"(expected e.g. {KLINE_PREFIX}1m)"
                )
            return self._stream("stream_kline")(ticker, interval), MD_KLINE
        raise ValueError(f"unsupported md topic: {topic!r}")

    async def _pump(
        self,
        feed: Feed,
        source: AsyncIterator[Any],
        msg_type: str,
    ) -> None:
        try:
            async for item in source:
                if feed.stop.is_set():
                    return
                env = UntypedEnvelope.wrap(
                    item.model_dump(mode="json"),
                    type=msg_type,
                    source="md",
                )
                await self._on_update(feed.topic, feed.ticker, env)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "MD %s pump failed ticker=%s", feed.topic, feed.ticker
            )
