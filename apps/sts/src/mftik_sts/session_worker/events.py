"""What the ingress holds for one inbound event (§5.3, F25).

The strategy thread decodes ``body`` into the platform model the hook
receives. The ingress does not (I4), and the bytes are the NATS
payload as received — a platform model, not a venue frame (F21).

``recv_ts`` is when the ingress received it. ``seq`` is the MD
per-atom sequence: continuous inside one connection-worker
incarnation, restarted after ``on_md_update(..., "live")`` (F25). TD
events, ``feed_end`` and RPC replies do not carry one. ``age`` is how
long ago ``recv_ts`` was, so a strategy can tell a stale print from a
quiet feed (a quiet feed is still ``live``; staleness is this
number's question).

**Who fills ``bar_open``.** Kline conflation keys on
``(feed, bar_open)``, and that key has to be known before the
strategy thread decodes the body — otherwise a stuck hook buffers
every update of the bar. The ingress still does not decode. The
receiver that builds an :class:`Inbound` puts ``bar_open`` (and
``closed``) on it from the small header the event log also stores
(§5.3, "原始 bytes 加一個小 header"). Which side stamps that header
is not decided here; see the package docstring.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from mftik.exchange.atoms import (
    TOPIC_AGG_TRADE,
    TOPIC_BEST_QUOTE,
    TOPIC_FUNDING_RATE,
    TOPIC_GREEKS,
    TOPIC_LIQUIDATION,
    TOPIC_OPEN_INTEREST,
    TOPIC_ORDERBOOK,
    TOPIC_TICKER,
    TOPIC_TRADE,
)


class StreamKind(StrEnum):
    """What an inbound event is, which is what picks its delivery row (§5.3).

    The market values are the platform topic strings
    (:mod:`mftik.exchange.atoms`). ``funding`` and ``OI`` in the plan's
    table are ``funding_rate`` and ``open_interest`` here, because that
    is what a feed key says. ``kline_1m`` and the other intervals are
    :attr:`KLINE`, not one member each. Members after the market topics
    are not feeds: nothing in ``strategy.yml`` overrides them.
    """

    TICKER = TOPIC_TICKER
    BESTQUOTE = TOPIC_BEST_QUOTE
    GREEKS = TOPIC_GREEKS
    FUNDING = TOPIC_FUNDING_RATE
    OPEN_INTEREST = TOPIC_OPEN_INTEREST
    ORDERBOOK = TOPIC_ORDERBOOK
    KLINE = "kline"
    TRADE = TOPIC_TRADE
    AGGTRADE = TOPIC_AGG_TRADE
    LIQUIDATION = TOPIC_LIQUIDATION
    TD = "td"
    FEED_END = "feed_end"
    RPC_REPLY = "rpc_reply"
    MD_NOTICE = "md_notice"
    TD_NOTICE = "td_notice"
    RESYNC = "resync"


class LogMark(StrEnum):
    """What happened to an inbound event after the ingress logged it.

    Stamped when the outcome is known, not when the bytes arrive.
    ``delivered`` — the strategy thread took it.
    ``superseded`` — a ``latest`` or kline conflation replaced it.
    ``dropped`` — an ``all`` feed's queue overflowed and this was the
    oldest. A must-deliver overflow is not this: that fails the session
    and the event is not accepted.
    """

    DELIVERED = "delivered"
    SUPERSEDED = "superseded"
    DROPPED = "dropped"


@dataclass(frozen=True)
class Inbound:
    """One event the ingress has received and not yet decoded.

    ``clock`` is read by :attr:`age`. It is the ingress
    :class:`~mftik.clock.Clock`'s ``now``, bound when the event is
    built. ``age`` subtracts when it is read, which is on the strategy
    thread. Until a clock is bound, :attr:`age` is ``None`` — unknown,
    not zero. Zero would look like a print that just arrived.
    """

    kind: StreamKind
    feed: str
    recv_ts: float
    body: bytes
    event_id: str
    #: MD per-atom sequence (F25). ``None`` when the event is not MD.
    seq: int | None = None
    #: Kline conflation key, with :attr:`feed`. ``None`` on anything else.
    bar_open: float | None = None
    #: The header's closed-bar bit, when ``kind`` is kline.
    closed: bool | None = None
    clock: Callable[[], float] | None = None

    @property
    def age(self) -> float | None:
        """Seconds since the ingress received this, or ``None`` if no clock is bound."""
        if self.clock is None:
            return None
        return self.clock() - self.recv_ts


@dataclass(frozen=True)
class LogRecord:
    """One inbound line queued for the event-log writer (§5.3).

    ``log_seq`` is the file's own sequence, stamped at receive, and it
    is not :attr:`event_seq`. The writer thread appends the line.
    ``mark`` is filled in when the outcome is known; until then it is
    ``None``. The queue, the writer and the "no ``STS_EVENTLOG_DIR``
    means don't write" switch are B5-02. The existing
    :class:`mftik.strategy.eventlog.EventLog` rules for a full writer
    queue — drop, count, leave a hole in ``log_seq`` — stay those rules.
    """

    event_id: str
    log_seq: int
    recv_ts: float
    body: bytes
    kind: StreamKind
    feed: str
    event_seq: int | None = None
    mark: LogMark | None = None
    bar_open: float | None = None
