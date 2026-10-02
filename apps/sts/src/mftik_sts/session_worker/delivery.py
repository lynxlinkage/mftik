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

``capacity`` is the bound of one queue. The plan does not give a
number, so the caller passes it and this module does not pick one.
Whether the must-deliver kinds share one queue or each have their own
is also not decided: every contract test overflows a single kind,
which is the same either way.

Klines are handed to :meth:`Delivery.take` in increasing ``bar_open``.
The plan doesn't say. A closed bar has to come out before the bar that
opened after it, including when the later bar's bytes arrived first.

The lookup functions below are the table. They are pure and they are
live. The queues that apply the table are not: :meth:`Delivery.accept`
raises ``NotImplementedError("IF-05")`` until B5-01.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum

from mftik.exchange.atoms import KLINE_PREFIX
from mftik.protocol import (
    DELIVERY_ALL,
    DELIVERY_KLINE,
    DELIVERY_LATEST,
    DELIVERY_MODES,
)

from mftik_sts.session_worker.errors import SessionFailed
from mftik_sts.session_worker.events import Inbound, LogMark, LogRecord, StreamKind

#: Kinds that are ``all``, that ignore a delivery override, and that
#: fail the session instead of dropping. Not feeds, so ``strategy.yml``
#: has nowhere to write an override for them. Availability notices are
#: in this set so the temporary buffer cannot drop one (B5-05).
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
    """What happens when ``kind``'s queue is past ``capacity``.

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
    ``kline`` is one slot per ``(feed, bar_open)``.

    :meth:`accept` and the marks it would stamp raise
    ``NotImplementedError("IF-05")``. :meth:`take` returns ``None``,
    :meth:`mark` returns ``None``, :attr:`dropped` is ``0``. B5-01
    fills the queues in. B5-02 persists :meth:`log_records`.

    **State this object holds, once it does anything (§3.3):** the
    not-yet-delivered events, the drop count the ingress publishes on
    ``sts.status.{session_id}``, and the in-memory disposition mark of
    each event. The file those marks are written to is the event log,
    also this layer's, written by the writer thread rather than by
    :meth:`accept`.
    """

    def __init__(
        self,
        *,
        capacity: int,
        overrides: Mapping[str, str] | None = None,
    ) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError(f"capacity must be an integer >= 1, got {capacity!r}")
        overrides = dict(overrides or {})
        bad = {
            feed: mode for feed, mode in overrides.items() if mode not in DELIVERY_MODES
        }
        if bad:
            raise ValueError(
                "delivery override must be one of "
                f"{sorted(DELIVERY_MODES)}, got {bad!r}"
            )
        self.capacity = capacity
        self.overrides = overrides

    def mode_of(self, event: Inbound) -> str:
        """The mode ``event`` will be queued under, override included."""
        if event.kind in MUST_DELIVER:
            return DELIVERY_ALL
        return delivery_mode(event.kind, self.overrides.get(event.feed))

    def accept(self, event: Inbound) -> None:
        """Queue ``event``, conflate it, drop the oldest, or fail the session.

        A kline without :attr:`~Inbound.bar_open` is refused here, before
        any queue runs: the key is the header, and a missing header is
        not an event this table can place. That check is live. The
        queue is not.

        Raises :class:`SessionFailed` when a must-deliver queue is past
        ``capacity``. The event that did not fit is not marked
        ``dropped``. Events already accepted stay accepted.

        Raises :class:`NotImplementedError` until B5-01.
        """
        if not event.event_id:
            raise ValueError("event_id is required")
        if event.kind is StreamKind.KLINE and event.bar_open is None:
            raise ValueError(
                "a kline event needs bar_open; it is the conflation key "
                "and the ingress does not decode the body to find it"
            )
        raise NotImplementedError("IF-05")

    def take(self) -> Inbound | None:
        """The next event for the strategy thread to decode, or ``None``.

        ``None`` is the empty queue. It is also what this stub always
        returns. Taking an event marks it ``delivered``.
        """
        return None

    @property
    def dropped(self) -> int:
        """How many ``all``-feed events were dropped as the oldest.

        The number ``sts.status`` progress publishes. A ``latest``
        replacement is not a drop. A must-deliver overflow is not a
        drop either — the session has failed, and the count stays.
        """
        return 0

    @property
    def failed(self) -> bool:
        """True once a must-deliver queue has overflowed."""
        return False

    @property
    def fail_reason(self) -> str | None:
        """The :class:`SessionFailed` reason, or ``None`` if it hasn't."""
        return None

    def mark(self, event_id: str) -> LogMark | None:
        """The disposition of ``event_id``, or ``None`` if it has none yet.

        ``None`` means not accepted, or accepted and still waiting.
        The stub has accepted nothing, so it is always ``None``.
        """
        return None

    def warnings(self) -> tuple[str, ...]:
        """Warning lines written when an ``all`` feed dropped an event.

        Empty until something is dropped. One line per drop, not one
        line per event that survived.
        """
        return ()

    def log_records(self) -> tuple[LogRecord, ...]:
        """Inbound lines queued for the writer, oldest first.

        Empty until B5-02. :meth:`accept` logs at receive; the mark on
        a line changes when the outcome is known. Nothing here writes
        a file.
        """
        return ()


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
