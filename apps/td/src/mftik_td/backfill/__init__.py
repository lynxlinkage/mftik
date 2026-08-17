"""Re-reading an account's history from the venue it was traded on.

The live writer records what TD saw. This records what the venue says, which
is not always the same thing: TD can be down, a socket can drop, a fee can
settle after the fill that earned it. Everything here exists to turn a window
of the record from *what we caught* into *what happened*.
"""

from mftik_td.backfill.executor import (
    MAX_PAGES_PER_WALK,
    SAFETY_LAG_S,
    BackfillExecutor,
    BackfillOutcome,
)
from mftik_td.backfill.reader import (
    HistoryPage,
    HistoryReaderFactory,
    NoHistoryReaderError,
    TradeHistoryReader,
)
from mftik_td.backfill.session import BackfillSession
from mftik_td.backfill.trigger import POST_TIMEOUT_S, request_backfill

__all__ = [
    "MAX_PAGES_PER_WALK",
    "SAFETY_LAG_S",
    "BackfillExecutor",
    "BackfillOutcome",
    "BackfillSession",
    "HistoryPage",
    "HistoryReaderFactory",
    "NoHistoryReaderError",
    "POST_TIMEOUT_S",
    "TradeHistoryReader",
    "request_backfill",
]
