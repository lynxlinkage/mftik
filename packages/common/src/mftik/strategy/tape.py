"""Strategy-side tape reads — the prints a session was not running for.

MD records the trade feeds it pumps (see ``mftik_md.tape``). This reads one back,
so a strategy that has to see a few hundred prints before it can act does not
have to spend the first hour of its life watching them arrive.

A direct Redis read rather than a query to MD, for the reason
:class:`~mftik.strategy.ledger.StrategyLedger` reads ``td.ledger.{api_id}`` directly:
the data is already sitting in a key that MD owns and keeps current, and asking
its owner to hand over a copy would add a round trip and a second answer that
can disagree with the first. The ``mds.fetch_*`` plane is for the other case —
something only the venue knows, which somebody has to go and ask for.

What comes back is the same :class:`~mftik.exchange.models.Trade` /
:class:`~mftik.exchange.models.AggTrade` the live hooks are handed, so a strategy
can feed history and live prints through one code path instead of writing its
aggregation twice and hoping the two agree.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

from mftik.broker.client import decode_tape_gaps
from mftik.exchange.models import AggTrade, Side, Trade
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import Topics
from mftik.strategy.eventlog import session_log

if TYPE_CHECKING:
    from mftik.strategy.base import Strategy

logger = logging.getLogger(__name__)

#: The feed topics MD records. Asking for anything else is not an error worth
#: raising over — it simply has no tape, and the slice comes back empty.
RECORDED_TOPICS = ("aggtrade", "trade")

#: How many records one read will pull. A ceiling on memory and parse time, not
#: a recommendation: a caller wanting fewer should ask for fewer.
DEFAULT_LIMIT = 200_000

#: How many prints of one read reach the event log, and how many ride on a
#: line. The cap is a disk budget, not a judgement about what matters: at
#: roughly 200 bytes a print this is about 10 MB per read, against the ~40 MB
#: a full 200k-record warm-up would cost. A read past it is written short and
#: says so on the ``tape.read`` line, the way MD's own tape reports the fuse
#: rather than pretending it did not bite.
LOG_MAX_RECORDS = 50_000
LOG_CHUNK = 1_000

#: How long an interruption may be before a read refuses to span it.
#:
#: Sized for the thing that actually interrupts a feed on a healthy system: a
#: deploy, where MD is stopped and started and the venue feed re-established.
#: That is seconds. A venue outage or a crash loop is not, and reading across
#: one would hand a strategy a series that is not a series.
#:
#: This is a default, not a rule — a strategy whose bars are shorter than this
#: should pass its own. ``0`` restores the older, absolute behaviour: no gap is
#: tolerable, and anything before one is dropped.
DEFAULT_MAX_GAP_MS = 30_000


@dataclass(frozen=True)
class TapeGap:
    """An interruption somebody measured, between two stretches of recording.

    Only a recorder that shut down cleanly leaves one of these: it stamped when
    it stopped, and its successor stamped when it started. An interruption
    nobody was around to write down does not appear here — it ends the
    recording instead, and the tape before it never reaches the reader.
    """

    #: When recording stopped, in Redis' clock — the same clock stream ids and
    #: :attr:`TapeSlice.continuous_since_ms` are stamped against, not the
    #: venue's event time that rides on each record.
    start_ms: int
    #: When recording resumed.
    end_ms: int

    @property
    def duration_ms(self) -> int:
        return max(0, self.end_ms - self.start_ms)


@dataclass(frozen=True)
class TapeSlice:
    """Recorded prints, and an honest account of what they cover.

    The records alone cannot answer "is this enough?". Five thousand prints are
    two hours of a quiet instrument or four minutes of a busy one, and a series
    that spans a gap is not a series. Everything needed to tell those apart is
    here, so a strategy can decide rather than assume.
    """

    #: Oldest → newest. Continuous up to :attr:`gaps`, which are short enough
    #: that the caller said it would rather have the history than the purity;
    #: anything on the far side of a break too long for that is already gone.
    records: list[Trade] = field(default_factory=list)
    #: When the current recording began, or None if never recorded. It may
    #: predate one or more :attr:`gaps` — a measured interruption does not end
    #: the series, it puts a hole in it.
    continuous_since_ms: int | None = None
    #: Whether the feed is still being recorded. False means these prints end
    #: somewhere in the past — the feed lost its last subscriber.
    recording: bool = False
    #: How many records were dropped for falling before the start of the
    #: series: either the continuity mark, or the end of a gap too long to
    #: read across.
    dropped_before_gap: int = 0
    #: Measured holes inside :attr:`records`, oldest first. Only those the
    #: returned records actually span — a gap whose far side has already been
    #: trimmed out of the tape is not a hole in what came back.
    gaps: list[TapeGap] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.records)

    @property
    def span_ms(self) -> int:
        """Wall-clock milliseconds the records cover, 0 if fewer than two."""
        if len(self.records) < 2:
            return 0
        return int((self.records[-1].ts - self.records[0].ts) * 1000)

    @property
    def missing_ms(self) -> int:
        """How much of :attr:`span_ms` is hole rather than history."""
        return sum(gap.duration_ms for gap in self.gaps)


class StrategyTape:
    """Reads MD's recorded trade tape for warm-up."""

    def __init__(self) -> None:
        self._strategy: Strategy | None = None

    def bind(self, strategy: Strategy) -> None:
        self._strategy = strategy

    async def read(
        self,
        ticker: UniversalTicker | str,
        *,
        topic: str = "aggtrade",
        limit: int = DEFAULT_LIMIT,
        max_gap_ms: int = DEFAULT_MAX_GAP_MS,
    ) -> TapeSlice:
        """Read up to ``limit`` of the most recent prints on one feed.

        The most recent rather than the oldest: warming up is catching up to
        now, and the tape holds an unknown number of records — two independent
        bounds decide how many survive, so a count from the far end names no
        window anyone chose.

        ``max_gap_ms`` is how long a measured interruption may be before this
        stops reading across it and treats it as the start of the series. The
        prints before such a gap are dropped, exactly as they are for a
        recording that restarted without saying when. What comes back always
        reports the gaps it does span, on :attr:`TapeSlice.gaps`, so a strategy
        that wants to be stricter than the number it passed still can be.

        An empty slice is a normal answer, not a failure. Nothing has ever
        subscribed to this feed, MD is running with recording off, or the tape
        expired while nobody held the feed.
        """
        if self._strategy is None or self._strategy.session is None:
            raise RuntimeError("strategy tape is not bound to a session")
        broker = self._strategy.session.broker
        resolved = UniversalTicker.resolve(str(ticker))
        feed = Topics.md_feed(topic, resolved)
        # Resolved once, not once per record: a warm-up read is hundreds of
        # thousands of rows, and every one of them carries the same ticker.
        universal_ticker = str(resolved)

        coverage = await broker.tape_coverage(feed)
        since_ms = _int_or_none(coverage.get("continuous_since_ms"))
        recording = coverage.get("recording") == "1"
        gaps = [
            TapeGap(start_ms=start, end_ms=end)
            for start, end in decode_tape_gaps(coverage.get("gaps"))
        ]

        # Where the series actually begins. The continuity mark is the floor;
        # a gap longer than the caller will tolerate raises it, because reading
        # across one of those is the failure this whole mechanism exists to
        # prevent. Only the newest such gap matters — everything before it is
        # on the far side of it anyway.
        start_ms = since_ms
        too_long = [gap for gap in gaps if gap.duration_ms > max_gap_ms]
        if too_long:
            resumed = max(gap.end_ms for gap in too_long)
            start_ms = resumed if start_ms is None else max(start_ms, resumed)

        rows = await broker.tape_tail(feed, count=max(0, limit))
        records: list[Trade] = []
        dropped = 0
        oldest_kept_ms: int | None = None
        for record_id, fields in rows:
            # The stream id is Redis' clock at append time, which is what the
            # continuity mark is stamped against. The venue's own ts rides on
            # the record and is what the strategy reads — the two answer
            # different questions and are not interchangeable here.
            record_ms = _id_ms(record_id)
            if start_ms is not None and record_ms < start_ms:
                dropped += 1
                continue
            parsed = _parse(topic, universal_ticker, fields)
            if parsed is not None:
                records.append(parsed)
                if oldest_kept_ms is None:
                    oldest_kept_ms = record_ms

        # A gap the returned records do not reach back to is not a hole in
        # them. This is also what keeps the list bounded over time: gaps age
        # out of the answer along with the records around them.
        gaps = (
            [gap for gap in gaps if gap.start_ms >= oldest_kept_ms]
            if oldest_kept_ms is not None
            else []
        )

        if dropped:
            logger.info(
                "STS tape dropped %d record(s) from before the gap feed=%s",
                dropped,
                feed,
            )
        if gaps:
            logger.info(
                "STS tape read across %d measured gap(s) totalling %dms feed=%s",
                len(gaps),
                sum(gap.duration_ms for gap in gaps),
                feed,
            )
        log = session_log(self._strategy)
        log.record(
            "read",
            "tape.read",
            dir="out",
            feed=feed,
            limit=limit,
            records=len(records),
            continuous_since_ms=since_ms,
            recording=recording,
            dropped_before_gap=dropped,
            max_gap_ms=max_gap_ms,
            # The holes are part of what the prints below mean. A replay that
            # sees the records and not these would rebuild a series the
            # strategy never actually had.
            gaps=[[gap.start_ms, gap.end_ms] for gap in gaps],
            logged=min(len(records), LOG_MAX_RECORDS),
            truncated=len(records) > LOG_MAX_RECORDS or None,
        )
        _log_records(log, feed, records)
        return TapeSlice(
            records=records,
            continuous_since_ms=since_ms,
            recording=recording,
            dropped_before_gap=dropped,
            gaps=gaps,
        )


def _log_records(log, feed: str, records: list[Trade]) -> None:  # noqa: ANN001
    """Write the prints themselves, in chunks, up to the cap.

    The one read in this class whose answer cannot be inferred from anything
    else on disk. MD's tape is the only copy, it expires within hours, and a
    warm-up is the whole basis of what the strategy does for its first
    minutes — a coverage summary says how much history there was, not what was
    in it.

    Chunked because one line per print would multiply the per-record overhead
    by a hundred thousand, and one line for all of them would be a forty-megabyte
    string that no jsonl reader will take in a single bite.
    """
    for start in range(0, min(len(records), LOG_MAX_RECORDS), LOG_CHUNK):
        chunk = records[start : start + LOG_CHUNK]
        # The models, unserialized: they are frozen, and dumping them here
        # would put the cost of a whole warm-up on the event loop.
        log.record(
            "read",
            "tape.records",
            dir="out",
            feed=feed,
            offset=start,
            count=len(chunk),
            payload=chunk,
        )


def _id_ms(record_id: str) -> int:
    """Milliseconds out of a ``<ms>-<seq>`` stream id."""
    head, _, _tail = record_id.partition("-")
    try:
        return int(head)
    except ValueError:
        return 0


def _int_or_none(raw: str | None) -> int | None:
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _parse(
    topic: str, universal_ticker: str, fields: dict[str, str]
) -> Trade | None:
    """Rebuild one recorded print, or None if it cannot be read.

    A record that will not parse is dropped rather than raised on. It is one
    print out of a warm-up window of thousands, and the strategy waiting on
    this has a live feed to fall back to; failing the whole read over it would
    turn a rounding error in one venue message into a session that cannot
    start.
    """
    try:
        price = Decimal(fields["price"])
        qty = Decimal(fields["qty"])
        side = Side(fields["side"])
        ts = float(fields["ts"])
    except (KeyError, ValueError, ArithmeticError, InvalidOperation):
        logger.debug(
            "STS tape record unreadable ticker=%s row=%r",
            universal_ticker,
            fields,
        )
        return None

    common = {
        "universal_ticker": universal_ticker,
        "trade_id": fields.get("trade_id", ""),
        "price": price,
        "qty": qty,
        "side": side,
        "ts": ts,
    }
    if topic == "aggtrade":
        return AggTrade(
            **common,
            first_trade_id=fields.get("first_trade_id", ""),
            last_trade_id=fields.get("last_trade_id", ""),
        )
    return Trade(**common)
