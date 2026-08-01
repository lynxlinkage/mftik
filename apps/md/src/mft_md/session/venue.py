"""Per-venue public client + feed pumps."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from mft.exchange.base import PublicClient
from mft.exchange.models import OrderBook
from mft.protocol import MD_ORDERBOOK, UntypedEnvelope

logger = logging.getLogger(__name__)

TOPIC_ORDERBOOK = "orderbook"

OnUpdate = Callable[[str, str, str, UntypedEnvelope], Awaitable[None]]


@dataclass
class Feed:
    """One active (topic, symbol) stream on a venue."""

    topic: str
    symbol: str
    task: asyncio.Task[None] | None = None
    stop: asyncio.Event = field(default_factory=asyncio.Event)


class VenueSession:
    """Owns one :class:`PublicClient` and running feed pumps."""

    def __init__(
        self,
        venue: str,
        public: PublicClient,
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
        if topic != TOPIC_ORDERBOOK:
            raise ValueError(f"unsupported md topic: {topic!r}")
        feed = Feed(topic=topic, symbol=symbol)
        feed.task = asyncio.create_task(
            self._pump_order_book(feed),
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

    async def _pump_order_book(self, feed: Feed) -> None:
        try:
            async for book in self.public.stream_order_book(feed.symbol):
                if feed.stop.is_set():
                    return
                assert isinstance(book, OrderBook)
                env = UntypedEnvelope.wrap(
                    book.model_dump(mode="json"),
                    type=MD_ORDERBOOK,
                    source="md",
                )
                await self._on_update(
                    self.venue, feed.topic, feed.symbol, env
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "MD orderbook pump failed venue=%s symbol=%s",
                self.venue,
                feed.symbol,
            )
