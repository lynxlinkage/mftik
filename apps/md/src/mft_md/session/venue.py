"""Per-venue connector + feed pumps."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from mft.exchange.models import OrderBook, Ticker, Trade
from mft.protocol import (
    MD_BEST_QUOTE,
    MD_KLINE,
    MD_ORDERBOOK,
    MD_TICKER,
    MD_TRADE,
    UntypedEnvelope,
)

logger = logging.getLogger(__name__)


class MarketDataConnector(Protocol):
    """What MD needs of a venue, stated by MD rather than by the venue.

    ``mft.exchange`` has no shared public interface on purpose — venues differ
    too much for one to be honest (see :mod:`mft.exchange.base`). So the shape
    lives here, with the consumer, and holds only what every venue really does
    provide: a lifecycle and the three feeds nobody lacks.

    ``stream_kline`` and ``stream_best_quote`` are deliberately absent. Gate
    serves them and paper does not, and a venue that cannot should have no such
    method rather than one that raises — :meth:`VenueSession._open` looks for
    them and refuses the subscribe when they are missing, which is the same
    answer one venue short of the full set was always going to give.
    """

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    def stream_ticker(self, symbol: str) -> AsyncIterator[Ticker]: ...

    def stream_trades(self, symbol: str) -> AsyncIterator[Trade]: ...

    def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]: ...


TOPIC_ORDERBOOK = "orderbook"
TOPIC_TICKER = "ticker"
TOPIC_TRADE = "trade"
TOPIC_BEST_QUOTE = "bestquote"
#: Klines need an interval, and a feed key is only ``venue.topic.symbol`` — so
#: the interval rides in the topic: ``paper.kline_1m.BTCUSDT``.
KLINE_PREFIX = "kline_"

OnUpdate = Callable[[str, str, str, UntypedEnvelope], Awaitable[None]]


@dataclass
class Feed:
    """One active (topic, symbol) stream on a venue."""

    topic: str
    symbol: str
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
        self._feeds: dict[tuple[str, str], Feed] = {}
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

    async def ensure_feed(self, topic: str, symbol: str) -> None:
        key = (topic, symbol)
        if key in self._feeds:
            return
        # Opened here rather than inside the task so an unsupported topic or a
        # venue that does not publish this feed fails the subscribe call
        # instead of dying silently in a background pump.
        source, msg_type = self._open(topic, symbol)
        feed = Feed(topic=topic, symbol=symbol)
        feed.task = asyncio.create_task(
            self._pump(feed, source, msg_type),
            name=f"md-{self.venue}-{topic}-{symbol}",
        )
        self._feeds[key] = feed
        logger.info(
            "MD feed started venue=%s topic=%s symbol=%s",
            self.venue,
            topic,
            symbol,
        )

    async def stop_feed(self, topic: str, symbol: str) -> None:
        feed = self._feeds.pop((topic, symbol), None)
        if feed is None:
            return
        feed.stop.set()
        if feed.task is not None:
            feed.task.cancel()
            await asyncio.gather(feed.task, return_exceptions=True)
        logger.info(
            "MD feed stopped venue=%s topic=%s symbol=%s",
            self.venue,
            topic,
            symbol,
        )

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

    def _open(self, topic: str, symbol: str) -> tuple[AsyncIterator[Any], str]:
        """Resolve a feed topic to its venue stream and wire message type."""
        if topic == TOPIC_ORDERBOOK:
            return self.public.stream_order_book(symbol), MD_ORDERBOOK
        if topic == TOPIC_TICKER:
            return self.public.stream_ticker(symbol), MD_TICKER
        if topic == TOPIC_TRADE:
            return self.public.stream_trades(symbol), MD_TRADE
        if topic == TOPIC_BEST_QUOTE:
            return self._stream("stream_best_quote")(symbol), MD_BEST_QUOTE
        if topic.startswith(KLINE_PREFIX):
            interval = topic[len(KLINE_PREFIX) :]
            if not interval:
                raise ValueError(
                    f"md kline topic needs an interval, got {topic!r} "
                    f"(expected e.g. {KLINE_PREFIX}1m)"
                )
            return self._stream("stream_kline")(symbol, interval), MD_KLINE
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
                await self._on_update(self.venue, feed.topic, feed.symbol, env)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "MD %s pump failed venue=%s symbol=%s",
                feed.topic,
                self.venue,
                feed.symbol,
            )
