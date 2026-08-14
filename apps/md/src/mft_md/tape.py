"""Tape recording — keep a feed's recent history so a late strategy can warm up.

MD is the only process that sees every print on a feed, so it is the only one
that can record one. A strategy could keep its own buffer, but only for as long
as it has been running, which is precisely the window a warm-up needs and does
not have.

Only the trade feeds are recorded. ``aggtrade`` and ``trade`` are the ones whose
history cannot be recovered — no venue here serves a tape lookup (see
``mft_md.fetch.readers``, which offers klines, book and quote and nothing else)
— and they are cheap: an ``AggTrade`` payload is under 200 bytes against more
than 1500 for a twenty-level book. Recording the book feeds would cost seven
times as much per message to answer a question nobody asks, because a strategy
that subscribes to ``orderbook`` has a full snapshot on its next message anyway.

Recording follows the feed, not the strategy that asked for it. A feed pumps
while its refcount is above zero (``Dispatcher``), so the recording starts with
the first subscriber and stops with the last — and both edges are stamped into
the coverage hash, which is what lets a reader tell a continuous two hours from
two hours with a hole in the middle.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Sequence

from mft.broker import Broker
from mft.exchange.tickers import UniversalTicker
from mft.protocol import Topics

logger = logging.getLogger(__name__)

#: Feed topics worth recording. See the module docstring for why the book and
#: quote feeds are not here.
DEFAULT_TOPICS: tuple[str, ...] = ("aggtrade", "trade")

#: How far back a reader may warm up from.
DEFAULT_RETENTION_S = 7200.0

#: Hard cap per feed, in records — the memory fuse, not the policy. Sized so a
#: single feed cannot cost more than roughly 100 MB even if it prints far
#: faster than the retention window assumed: at ~200 bytes a record, 500k is a
#: little over two hours of a busy perp and a fortnight of a quiet one.
#: Whichever bound bites first, the coverage hash reports what is really there.
DEFAULT_MAXLEN = 500_000

#: Fields lifted off a trade payload, in the order they are written. Every
#: record in one stream carries the same set, which is what lets Redis keep the
#: names once per node instead of once per entry.
_TRADE_FIELDS = ("trade_id", "price", "qty", "side", "ts")
_AGG_EXTRA_FIELDS = ("first_trade_id", "last_trade_id")


def _now_ms() -> int:
    return int(time.time() * 1000)


class TapeRecorder:
    """Appends trade prints to a per-feed stream and tracks what it covers."""

    def __init__(
        self,
        broker: Broker,
        *,
        topics: Sequence[str] = DEFAULT_TOPICS,
        maxlen: int = DEFAULT_MAXLEN,
        retention_s: float = DEFAULT_RETENTION_S,
    ) -> None:
        self._broker = broker
        self._topics = frozenset(topics)
        self._maxlen = max(1, maxlen)
        self._retention_s = max(1.0, retention_s)
        #: Generous against the retention window rather than equal to it. The
        #: TTL is the backstop that reclaims a feed nobody subscribes to any
        #: more; the MINID trim is the policy. Equal values would race, and the
        #: one that lost would delete a tape that is still inside its window.
        self._ttl_seconds = int(self._retention_s * 2)

    @property
    def topics(self) -> frozenset[str]:
        return self._topics

    def records(self, topic: str) -> bool:
        """Whether ``topic`` is one of the recorded feeds."""
        return topic in self._topics

    async def append(
        self, topic: str, ticker: UniversalTicker, payload: dict[str, object]
    ) -> None:
        """Record one print. Never raises — a lost record is not a lost trade.

        Recording is a nicety for a warm-up that may never happen, while the
        fan-out this runs behind is feeding strategies that are trading now.
        """
        if topic not in self._topics:
            return
        feed = Topics.md_feed(topic, ticker)
        try:
            fields = _fields_from(topic, payload)
        except (KeyError, TypeError, ValueError):
            logger.exception("MD tape record unreadable feed=%s", feed)
            return
        try:
            await self._broker.tape_append(
                feed,
                fields,
                maxlen=self._maxlen,
                ttl_seconds=self._ttl_seconds,
            )
        except Exception:
            logger.exception("MD tape append failed feed=%s", feed)

    async def started(self, feed: str) -> None:
        """Stamp the moment ``feed`` began recording.

        Also the moment continuity broke: anything already in the stream is on
        the far side of a gap whose length nobody measured.
        """
        try:
            await self._broker.tape_mark_recording(
                feed, since_ms=_now_ms(), ttl_seconds=self._ttl_seconds
            )
        except Exception:
            logger.exception("MD tape start stamp failed feed=%s", feed)

    async def stopped(self, feed: str) -> None:
        """Stamp the moment ``feed`` stopped recording."""
        try:
            await self._broker.tape_mark_stopped(feed, at_ms=_now_ms())
        except Exception:
            logger.exception("MD tape stop stamp failed feed=%s", feed)

    async def trim(self, feeds: Iterable[str]) -> None:
        """Drop records past the retention window on every recorded feed."""
        horizon = _now_ms() - int(self._retention_s * 1000)
        for feed in feeds:
            topic, _, _rest = feed.partition(".")
            if topic not in self._topics:
                continue
            try:
                dropped = await self._broker.tape_trim_before(
                    feed, min_id_ms=horizon
                )
            except Exception:
                logger.exception("MD tape trim failed feed=%s", feed)
                continue
            if dropped:
                logger.debug(
                    "MD tape trimmed %d record(s) feed=%s", dropped, feed
                )


def _fields_from(topic: str, payload: dict[str, object]) -> dict[str, str]:
    """Flatten a trade payload into stream fields.

    Flat fields rather than one JSON blob: entries sharing a field set store
    only their values after the first, which is most of the saving a stream has
    over a list of serialized envelopes.
    """
    names = _TRADE_FIELDS
    if topic == "aggtrade":
        names = _TRADE_FIELDS + _AGG_EXTRA_FIELDS
    fields: dict[str, str] = {}
    for name in names:
        value = payload.get(name)
        fields[name] = "" if value is None else str(value)
    if not fields["price"] or not fields["qty"]:
        raise ValueError(f"trade payload has no price/qty: {payload!r}")
    return fields
