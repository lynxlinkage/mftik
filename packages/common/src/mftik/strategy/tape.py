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


@dataclass(frozen=True)
class TapeSlice:
    """Recorded prints, and an honest account of what they cover.

    The records alone cannot answer "is this enough?". Five thousand prints are
    two hours of a quiet instrument or four minutes of a busy one, and a series
    that spans a gap is not a series. Everything needed to tell those apart is
    here, so a strategy can decide rather than assume.
    """

    #: Oldest → newest, and continuous: anything from before the recording
    #: restarted has already been dropped.
    records: list[Trade] = field(default_factory=list)
    #: When the current unbroken recording began, or None if never recorded.
    continuous_since_ms: int | None = None
    #: Whether the feed is still being recorded. False means these prints end
    #: somewhere in the past — the feed lost its last subscriber.
    recording: bool = False
    #: How many records were dropped for predating the continuity mark.
    dropped_before_gap: int = 0

    def __len__(self) -> int:
        return len(self.records)

    @property
    def span_ms(self) -> int:
        """Wall-clock milliseconds the records cover, 0 if fewer than two."""
        if len(self.records) < 2:
            return 0
        return int((self.records[-1].ts - self.records[0].ts) * 1000)


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
    ) -> TapeSlice:
        """Read up to ``limit`` of the most recent prints on one feed.

        The most recent rather than the oldest: warming up is catching up to
        now, and the tape holds an unknown number of records — two independent
        bounds decide how many survive, so a count from the far end names no
        window anyone chose.

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

        rows = await broker.tape_tail(feed, count=max(0, limit))
        records: list[Trade] = []
        dropped = 0
        for record_id, fields in rows:
            # The stream id is Redis' clock at append time, which is what the
            # continuity mark is stamped against. The venue's own ts rides on
            # the record and is what the strategy reads — the two answer
            # different questions and are not interchangeable here.
            if since_ms is not None and _id_ms(record_id) < since_ms:
                dropped += 1
                continue
            parsed = _parse(topic, universal_ticker, fields)
            if parsed is not None:
                records.append(parsed)

        if dropped:
            logger.info(
                "STS tape dropped %d record(s) from before the gap feed=%s",
                dropped,
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
            logged=min(len(records), LOG_MAX_RECORDS),
            truncated=len(records) > LOG_MAX_RECORDS or None,
        )
        _log_records(log, feed, records)
        return TapeSlice(
            records=records,
            continuous_since_ms=since_ms,
            recording=recording,
            dropped_before_gap=dropped,
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
