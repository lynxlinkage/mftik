"""How events wait for the strategy thread (§5.3, F8, F23, F25).

One row of the table, as data. The mode names are IF-07's.

* ticker, bestquote, greeks, funding_rate, open_interest, orderbook —
  ``latest``. Conflate to the newest per feed, before decode.
* kline — ``kline``. Conflate to the newest per ``(feed, bar_open)``.
  A closed bar is its own key, so the next bar does not drop it.
* trade, aggtrade, liquidation — ``all``. One bounded queue per feed.
  Drop the oldest, warn, and count it.
* TD, ``feed_end``, RPC reply, and the availability notices
  (``on_md_update``, ``on_td_update``, ``on_resync``) — ``all``, and
  not overridable. Do not drop. Fail the session.

``latest`` and ``kline`` replace an event in its slot. The replaced
event is marked :attr:`~mftik_sts.session_worker.events.LogMark.SUPERSEDED`
and is never returned by :meth:`Delivery.take`. A gap in ``seq`` on a
``latest`` feed is that replacement. It is not a loss, and it is not
recorded: there is no gap hook and no gap list (F23). A gap on an
``all`` feed is a loss. The strategy sees it as a hole in ``seq``
(F25). This layer does not write it down either.

A feed's ``delivery:`` override (``strategy.yml``, already parsed by
IF-07) replaces the default for that feed. It does not replace the
must-deliver row. TD, ``feed_end``, RPC replies and availability
notices stay ``all`` and fail on overflow no matter what string is
passed.

``all_capacity`` is the bound of one ``all`` feed queue
(:data:`~mftik_sts.session_worker.limits.ALL_QUEUE_CAPACITY`).
``must_capacity`` is the bound of the shared must-deliver queue
(:data:`~mftik_sts.session_worker.limits.MUST_DELIVER_CAPACITY`, #296).
They are not the same number: an ``all`` feed drops its oldest, and a
must-deliver overflow fails the session. ``latest`` and ``kline`` do
not use either: they conflate. Neither argument has a default.

Must-deliver kinds share one FIFO, so a fill and the RPC reply about
the same order cannot swap, and a ``RESYNC`` stays ahead of the
deferred ``ready`` offered after it. :meth:`Delivery.take` drains that
FIFO before any market data. A full book does not stand between a
strategy and a fill: market data is what drops when the strategy is
behind. Once the FIFO is empty, market-data feeds are round-robin in
the order they were first seen. An empty feed is skipped. Inside one
kline feed the smallest ``bar_open`` comes out first, so a closed bar
is delivered before a later bar whose bytes arrived earlier. Inside
one ``all`` feed the order is arrival order.

Disposition marks and drop-warning lines are capped
(:data:`~mftik_sts.session_worker.limits.MARK_RETENTION`,
:data:`~mftik_sts.session_worker.limits.WARNING_RETENTION`). The drop
count is not. B5-02 persists the event log; these caps bound the
in-memory notes until that writer can discard a line it has stored.

The lookup functions below are the table. They are pure. The queues
apply the table.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Mapping
from enum import StrEnum

from mftik.clock import Clock
from mftik.exchange.atoms import KLINE_PREFIX
from mftik.protocol import (
    DELIVERY_ALL,
    DELIVERY_KLINE,
    DELIVERY_LATEST,
    DELIVERY_MODES,
)

from mftik_sts.session_worker.errors import SessionFailed
from mftik_sts.session_worker.events import Inbound, LogMark, LogRecord, StreamKind
from mftik_sts.session_worker.limits import (
    DROP_WARN_INTERVAL_S,
    MARK_RETENTION,
    WARNING_RETENTION,
)

logger = logging.getLogger(__name__)

#: Kinds that are ``all``, that ignore a delivery override, and that
#: fail the session instead of dropping. Not feeds, so ``strategy.yml``
#: has nowhere to write an override for them. Availability notices are
#: in this set so a market-data flood cannot drop one (B5-05).
MUST_DELIVER = frozenset(
    {
        StreamKind.TD,
        StreamKind.FEED_END,
        StreamKind.RPC_REPLY,
        StreamKind.MD_NOTICE,
        StreamKind.TD_NOTICE,
        StreamKind.RESYNC,
    }
)

#: The §5.3 default per kind, before a feed override.
DEFAULT_DELIVERY: dict[StreamKind, str] = {
    StreamKind.TICKER: DELIVERY_LATEST,
    StreamKind.BESTQUOTE: DELIVERY_LATEST,
    StreamKind.GREEKS: DELIVERY_LATEST,
    StreamKind.FUNDING: DELIVERY_LATEST,
    StreamKind.OPEN_INTEREST: DELIVERY_LATEST,
    StreamKind.ORDERBOOK: DELIVERY_LATEST,
    StreamKind.KLINE: DELIVERY_KLINE,
    StreamKind.TRADE: DELIVERY_ALL,
    StreamKind.AGGTRADE: DELIVERY_ALL,
    StreamKind.LIQUIDATION: DELIVERY_ALL,
    StreamKind.TD: DELIVERY_ALL,
    StreamKind.FEED_END: DELIVERY_ALL,
    StreamKind.RPC_REPLY: DELIVERY_ALL,
    StreamKind.MD_NOTICE: DELIVERY_ALL,
    StreamKind.TD_NOTICE: DELIVERY_ALL,
    StreamKind.RESYNC: DELIVERY_ALL,
}

_TOPIC_KIND: dict[str, StreamKind] = {
    kind.value: kind
    for kind in StreamKind
    if kind not in MUST_DELIVER and kind is not StreamKind.KLINE
}


class Overflow(StrEnum):
    """What a full queue does. Conflate is not a drop (F25, F23)."""

    CONFLATE = "conflate"
    DROP_OLDEST = "drop_oldest"
    FAIL = "fail"


def _positive_capacity(name: str, value: object) -> int:
    """An explicit queue bound. No default, and ``bool`` is not a count."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be an integer >= 1, got {value!r}")
    return value


def topic_of(feed: str) -> str:
    """``ticker.Paper_Spot_BTCUSDT`` → ``ticker``.

    A feed key is the topic, a dot, and the universal ticker. The
    ticker may not contain a dot; the topic may (it doesn't, today).
    Only the first dot splits.
    """
    topic, sep, _rest = feed.partition(".")
    if not sep or not topic or not _rest:
        raise ValueError(f"feed must be 'topic.ticker', got {feed!r}")
    return topic


def kind_of_topic(topic: str) -> StreamKind:
    """The delivery kind of a platform topic. ``kline_1m`` is kline."""
    if topic == StreamKind.KLINE.value or topic.startswith(KLINE_PREFIX):
        return StreamKind.KLINE
    try:
        return _TOPIC_KIND[topic]
    except KeyError:
        raise ValueError(f"no delivery kind for topic {topic!r}") from None


def kind_of_feed(feed: str) -> StreamKind:
    """:func:`kind_of_topic` of :func:`topic_of`."""
    return kind_of_topic(topic_of(feed))


def delivery_mode(kind: StreamKind, override: str | None = None) -> str:
    """The mode ``kind`` is delivered in.

    ``override`` is the feed's ``delivery:`` value, or ``None`` when
    the feed didn't set one. A must-deliver kind ignores it and stays
    ``all``: those events are not feeds, and they are not droppable.
    """
    if kind in MUST_DELIVER:
        return DELIVERY_ALL
    if override is None:
        return DEFAULT_DELIVERY[kind]
    if override not in DELIVERY_MODES:
        raise ValueError(
            f"delivery override must be one of {sorted(DELIVERY_MODES)}, "
            f"got {override!r}"
        )
    return override


def overflow_policy(kind: StreamKind, override: str | None = None) -> Overflow:
    """What happens when ``kind``'s queue is past its bound.

    Must-deliver fails even if ``override`` says ``latest``. ``kline``
    and ``latest`` conflate. ``all`` drops the oldest. An override can
    move a feed between those last two; it cannot move TD.
    """
    if kind in MUST_DELIVER:
        return Overflow.FAIL
    mode = delivery_mode(kind, override)
    if mode == DELIVERY_ALL:
        return Overflow.DROP_OLDEST
    return Overflow.CONFLATE


class Delivery:
    """The queues the ingress holds events in until the strategy thread takes them.

    ``overrides`` is feed key → mode, the ``md_delivery`` map IF-07
    parses. A feed absent from it uses :data:`DEFAULT_DELIVERY`.

    ``all`` queues are per feed, so one busy trade feed does not punch
    holes in another feed's ``seq``. ``latest`` is one slot per feed.
    ``kline`` is one slot per ``(feed, bar_open)``. Must-deliver kinds
    share one FIFO of ``must_capacity``. One ``all`` feed holds
    ``all_capacity``.

    :meth:`log_records` stays empty. B5-02 persists the lines. The mark
    :meth:`mark` returns is the in-memory disposition only.

    **State this object holds (§3.3):** the not-yet-delivered events,
    the per-feed drop counts the ingress publishes on
    ``sts.status.{session_id}``, and the most recent disposition marks.
    The file those marks are written to is the event log, also this
    layer's, written by the writer thread rather than by :meth:`accept`.
    Marks older than :data:`MARK_RETENTION` are dropped here. B5-02 is
    what stores them first.

    Not locked against itself beyond the lock below. :class:`Ingress`
    calls :meth:`accept` and :meth:`take` from two threads and holds
    its own lock across each call. Direct use from one thread, which
    is what the contract tests do, does not need that.
    """

    def __init__(
        self,
        *,
        all_capacity: int,
        must_capacity: int,
        overrides: Mapping[str, str] | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.all_capacity = _positive_capacity("all_capacity", all_capacity)
        self.must_capacity = _positive_capacity("must_capacity", must_capacity)
        overrides = dict(overrides or {})
        bad = {
            feed: mode for feed, mode in overrides.items() if mode not in DELIVERY_MODES
        }
        if bad:
            raise ValueError(
                "delivery override must be one of "
                f"{sorted(DELIVERY_MODES)}, got {bad!r}"
            )
        self.overrides = overrides
        self._clock = clock
        self._lock = threading.Lock()
        self._must: deque[Inbound] = deque()
        self._all: dict[str, deque[Inbound]] = {}
        self._latest: dict[str, Inbound] = {}
        self._kline: dict[tuple[str, float], Inbound] = {}
        self._feeds: list[str] = []
        self._modes: dict[str, str] = {}
        self._rr = 0
        self._marks: OrderedDict[str, LogMark] = OrderedDict()
        self._dropped = 0
        self._dropped_by_feed: dict[str, int] = {}
        self._warnings: deque[str] = deque(maxlen=WARNING_RETENTION)
        self._warned_at: dict[str, float] = {}
        self._failed = False
        self._fail_reason: str | None = None

    def mode_of(self, event: Inbound) -> str:
        """The mode ``event`` will be queued under, override included."""
        if event.kind in MUST_DELIVER:
            return DELIVERY_ALL
        return delivery_mode(event.kind, self.overrides.get(event.feed))

    def accept(self, event: Inbound) -> None:
        """Queue ``event``, conflate it, drop the oldest, or fail the session.

        A kline without :attr:`~Inbound.bar_open` is refused here, before
        any queue runs: the key is the header, and a missing header is
        not an event this table can place.

        Raises :class:`SessionFailed` when the must-deliver queue is past
        ``must_capacity``. The event that did not fit is not marked
        ``dropped``. Events already accepted stay accepted. A later
        :meth:`accept` raises the same reason.
        """
        if not event.event_id:
            raise ValueError("event_id is required")
        if event.kind is StreamKind.KLINE and event.bar_open is None:
            raise ValueError(
                "a kline event needs bar_open; it is the conflation key "
                "and the ingress does not decode the body to find it"
            )
        with self._lock:
            self._accept(event)

    def take(self) -> Inbound | None:
        """The next event for the strategy thread to decode, or ``None``.

        ``None`` is the empty queue. Taking an event marks it
        ``delivered``. The must-deliver FIFO is drained first. Only
        when it is empty does a market-data feed come out, round-robin
        in first-seen order.
        """
        with self._lock:
            if self._must:
                event: Inbound | None = self._must.popleft()
            else:
                event = self._next_market()
            if event is None:
                return None
            self._remember_mark(event.event_id, LogMark.DELIVERED)
            return event

    @property
    def dropped(self) -> int:
        """How many ``all``-feed events were dropped as the oldest.

        The number ``sts.status`` progress publishes. A ``latest`` or
        kline replacement is not a drop. A must-deliver overflow is not
        a drop either — the session has failed, and the count stays.
        """
        with self._lock:
            return self._dropped

    @property
    def dropped_by_feed(self) -> dict[str, int]:
        """Drop counts for ``all`` feeds that have dropped at least one.

        A feed that has not dropped is absent. Replacement is not an
        entry. The total of the values is :attr:`dropped`.
        """
        with self._lock:
            return dict(self._dropped_by_feed)

    @property
    def failed(self) -> bool:
        """True once a must-deliver queue has overflowed."""
        with self._lock:
            return self._failed

    @property
    def fail_reason(self) -> str | None:
        """The :class:`SessionFailed` reason, or ``None`` if it hasn't."""
        with self._lock:
            return self._fail_reason

    def mark(self, event_id: str) -> LogMark | None:
        """The disposition of ``event_id``, or ``None`` if it has none yet.

        ``None`` means not accepted, accepted and still waiting, or
        forgotten because a newer mark pushed it past
        :data:`MARK_RETENTION`.
        """
        with self._lock:
            return self._marks.get(event_id)

    def warnings(self) -> tuple[str, ...]:
        """Warning lines written when an ``all`` feed dropped an event.

        Empty until something is dropped. One line per drop, not one
        line per event that survived, and only the most recent
        :data:`WARNING_RETENTION` lines. The logger is rate-limited;
        this tuple is not. The drop count is not trimmed with it.
        """
        with self._lock:
            return tuple(self._warnings)

    def log_records(self) -> tuple[LogRecord, ...]:
        """Inbound lines queued for the writer, oldest first.

        Empty until B5-02. :meth:`accept` is where the ingress logs at
        receive; the mark on a line changes when the outcome is known.
        Nothing here writes a file.
        """
        return ()

    def _accept(self, event: Inbound) -> None:
        if self._failed:
            raise SessionFailed(self._fail_reason or "delivery_overflow")
        if event.kind in MUST_DELIVER:
            self._accept_must(event)
            return
        mode = self.mode_of(event)
        if mode == DELIVERY_KLINE and event.bar_open is None:
            raise ValueError(
                "a kline event needs bar_open; it is the conflation key "
                "and the ingress does not decode the body to find it"
            )
        self._remember_feed(event.feed, mode)
        if mode == DELIVERY_ALL:
            self._accept_all(event)
        elif mode == DELIVERY_KLINE:
            self._accept_kline(event)
        else:
            self._accept_latest(event)

    def _remember_feed(self, feed: str, mode: str) -> None:
        current = self._modes.get(feed)
        if current is None:
            self._feeds.append(feed)
            self._modes[feed] = mode
            return
        if current != mode:
            raise ValueError(
                f"feed {feed} is already queued as {current}, not {mode}"
            )

    def _accept_must(self, event: Inbound) -> None:
        if len(self._must) >= self.must_capacity:
            reason = f"{event.kind.value}_overflow"
            self._failed = True
            self._fail_reason = reason
            logger.error("must-deliver overflow kind=%s", event.kind.value)
            raise SessionFailed(reason)
        self._must.append(event)

    def _accept_all(self, event: Inbound) -> None:
        queue = self._all.setdefault(event.feed, deque())
        if len(queue) >= self.all_capacity:
            oldest = queue.popleft()
            self._remember_mark(oldest.event_id, LogMark.DROPPED)
            self._dropped += 1
            count = self._dropped_by_feed.get(event.feed, 0) + 1
            self._dropped_by_feed[event.feed] = count
            line = (
                f"dropped oldest feed={event.feed} kind={event.kind.value} "
                f"seq={oldest.seq} count={count}"
            )
            self._warnings.append(line)
            self._warn_drop(event.feed, line)
        queue.append(event)

    def _accept_latest(self, event: Inbound) -> None:
        previous = self._latest.get(event.feed)
        if previous is not None:
            self._remember_mark(previous.event_id, LogMark.SUPERSEDED)
        self._latest[event.feed] = event

    def _accept_kline(self, event: Inbound) -> None:
        assert event.bar_open is not None
        key = (event.feed, event.bar_open)
        previous = self._kline.get(key)
        if previous is not None:
            self._remember_mark(previous.event_id, LogMark.SUPERSEDED)
        self._kline[key] = event

    def _next_market(self) -> Inbound | None:
        count = len(self._feeds)
        if count == 0:
            return None
        for _ in range(count):
            index = self._rr % count
            self._rr = index + 1
            event = self._pop_feed(index)
            if event is not None:
                return event
        return None

    def _pop_feed(self, index: int) -> Inbound | None:
        feed = self._feeds[index]
        mode = self._modes[feed]
        if mode == DELIVERY_ALL:
            queue = self._all.get(feed)
            if not queue:
                return None
            return queue.popleft()
        if mode == DELIVERY_KLINE:
            return self._pop_kline(feed)
        return self._latest.pop(feed, None)

    def _pop_kline(self, feed: str) -> Inbound | None:
        chosen: tuple[str, float] | None = None
        for key in self._kline:
            if key[0] != feed:
                continue
            if chosen is None or key[1] < chosen[1]:
                chosen = key
        if chosen is None:
            return None
        return self._kline.pop(chosen)

    def _remember_mark(self, event_id: str, mark: LogMark) -> None:
        self._marks[event_id] = mark
        self._marks.move_to_end(event_id)
        while len(self._marks) > MARK_RETENTION:
            self._marks.popitem(last=False)

    def _warn_drop(self, feed: str, line: str) -> None:
        now = self._monotonic()
        last = self._warned_at.get(feed)
        if last is not None and now - last < DROP_WARN_INTERVAL_S:
            return
        self._warned_at[feed] = now
        logger.warning("%s", line)

    def _monotonic(self) -> float:
        clock = self._clock
        if clock is None:
            return time.monotonic()
        return clock.monotonic()


__all__ = [
    "DEFAULT_DELIVERY",
    "MUST_DELIVER",
    "Delivery",
    "Overflow",
    "SessionFailed",
    "delivery_mode",
    "kind_of_feed",
    "kind_of_topic",
    "overflow_policy",
    "topic_of",
]
