"""Holds MD feeds open so their tape keeps recording. Places no orders.

MD pumps a feed while its refcount is above zero and stops the moment the last
subscriber goes (``mftik_md.session.manager._stop_feed_if_unused``). Recording
follows the pump, so a tape only exists while somebody is subscribed — and the
strategy that will want the history is, by definition, not running yet. This is
the somebody.

It subscribes and does nothing else. That is the whole design: every line it
does not have is a line that cannot stop the recording.

**Why not have the reader keep its own buffer.** It can, for as long as it has
been running. The window a warm-up needs is exactly the one before that.

**Why this places no orders.** ``td`` is left empty in its deploy document, so
the session attaches to no account and ``submit_order`` has nowhere to go. Not
a rule anybody has to remember — there is no account, so there is no order.

**Rebuild.** Safe, and uniquely so. ``rebuildable`` is off by default because a
restored strategy that does not know it was away will trade alongside orders it
left resting (see :class:`~mftik.strategy.Strategy`). This one holds no
position, no resting order and no venue state of any kind: coming back is
indistinguishable from starting, which is why it can say yes.

Its own restart *is* visible where it matters — MD stamps both edges of the
break into the tape's coverage, so a strategy warming up afterwards is told how
long the feed was unheld rather than reading across the hole as if it were not
there. A restart that got to run its shutdown leaves a hole with a measured
length, which a reader may decide to span; one that did not ends the series.
"""

from __future__ import annotations

from typing import Any

from mftik.exchange.models import AggTrade, Trade
from mftik.strategy import Strategy

#: How often to report that the feeds are still being held. Long: this exists
#: so an operator scanning logs can tell "holding, quiet" from "died an hour
#: ago", and anything faster is noise in a session that by design does nothing.
DEFAULT_REPORT_INTERVAL_MS = 300_000


class TapeKeeper(Strategy):
    """Subscribes to feeds and holds them, so MD keeps recording their tape."""

    name = "tape_keeper"
    id = 6
    rebuildable = True

    def __init__(self) -> None:
        super().__init__()
        self._prints = 0
        self._report_token = None

    @classmethod
    def on_initialized(cls, params: Any) -> dict[str, Any]:
        paras = super().on_initialized(params)
        raw = paras.get("report_interval_ms", DEFAULT_REPORT_INTERVAL_MS)
        try:
            interval = int(raw)
        except (TypeError, ValueError):
            raise ValueError(
                f"report_interval_ms must be a whole number of ms, got {raw!r}"
            ) from None
        if interval <= 0:
            raise ValueError(
                f"report_interval_ms must be positive, got {interval}"
            )
        paras["report_interval_ms"] = interval
        return paras

    async def on_start(self) -> None:
        feeds = list(self.session.md_ids) if self.session is not None else []
        if not feeds:
            # Nothing subscribed means nothing held, and nothing held means no
            # tape — the one way this strategy can fail is by looking fine
            # while doing nothing at all.
            self.fail("tape_keeper has no md feeds to hold")
            return
        await self.log(f"holding {len(feeds)} feed(s) for recording: {feeds}")

    async def on_ready(self) -> None:
        interval = int(
            self.paras.get("report_interval_ms", DEFAULT_REPORT_INTERVAL_MS)
        )
        self._report_token = self.timer.token()
        self._report_token.register(
            self.timer.now_ms() + interval, interval, self._report
        )

    async def on_rebuild(self, remembered: dict[str, str]) -> None:
        """Nothing to restore — see the module docstring.

        Deliberately empty rather than absent: an empty override here is a
        statement that the question was asked and the answer is genuinely
        nothing, which is not what an inherited no-op would mean.
        """

    async def on_stop(self) -> None:
        if self._report_token is not None:
            self._report_token.cancel()
            self._report_token = None
        await self.log(
            f"releasing feeds after {self._prints} print(s) — "
            "recording stops when the last subscriber goes"
        )

    # Counting is all these do. The prints themselves are already recorded by
    # MD before they reach here; this session's interest in them ends at being
    # able to say the feed is alive.

    async def on_agg_trade(self, trade: AggTrade) -> None:
        self._prints += 1

    async def on_trade(self, trade: Trade) -> None:
        self._prints += 1

    async def _report(self) -> None:
        await self.log(f"holding feeds — {self._prints} print(s) seen")
