"""MACD over quote-volume bars — long only, in on a bullish cross, out on a bearish one.

**Bars.** Not time bars. One bar closes when the trades folded into it have
carried ``bar_quote_volume`` of the quote currency, so a bar is a fixed amount
of *business* rather than a fixed amount of clock. A quiet hour produces one bar
and a violent minute produces thirty, which is the property the indicator wants:
a time bar spends most of the day describing an idle book.

The trade that crosses the threshold is folded in whole and the bar closes after
it; the next bar starts empty. The alternative — splitting a print across two
bars — invents a trade that never happened, and the overshoot is bounded by one
print anyway.

**Feed.** ``aggtrade`` or ``trade``, exactly one. They describe the same matches
at different granularity, so a session subscribed to both would count every fill
twice and every bar would close at half the volume it claims. Refused at start
rather than left to be discovered in the shape of the bars.

**Indicator.** Standard MACD on bar closes: ``EMA(fast) - EMA(slow)``, against a
signal line of ``EMA(macd, signal)``. Each EMA is seeded with its first sample
rather than a mean, so early values carry a bias that decays — which is what the
warm-up requirement below is for, not a detail of it.

**Warm-up.** The strategy needs ``slow + signal`` closed bars before it will
trade: ``slow`` for the slow EMA to describe the series rather than its own
seed, and ``signal`` more for the signal line to describe the MACD. It gets what
it can from MD's recorded tape at start, and if that is not enough it stays in
warm-up and keeps aggregating live prints until it is. It does not trade early
on a short history, and it does not fail — the bars are coming, and how long
they take is a property of the market, not an error.

How many bars the tape yields is not knowable in advance: it is recorded volume
divided by ``bar_quote_volume``. A threshold too large for the retention window
simply means a longer wait on live prints. If that wait is routinely long, the
threshold is too big for the instrument.

**Orders.** ``MARKET``, one at a time, sized ``qty_quote`` of the quote
currency. A cross is a statement about wanting exposure now; a resting limit
would turn it into a bet on being filled before the next cross. Nothing rests,
so there is nothing to cancel on the way out.

**Long only.** A bullish cross with no position opens one. A bearish cross with
a position closes it. A bearish cross while flat does nothing — it is not an
instruction to go short.

**Rebuild.** Off. The position is real and recon would report it, but reasoning
about a restored position against an indicator rebuilt from a different stretch
of tape is a decision this strategy has not been given. See
:meth:`~mft_sts.strategy.Strategy.on_rebuild`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from mft.exchange.models import (
    AggTrade,
    Fill,
    Order,
    OrderType,
    Side,
    Trade,
    is_terminal,
)
from mft.exchange.tickers import UniversalTicker
from mft.protocol import CancelReject, OrderReject, SymbolInfo

from mft_sts.strategy import Strategy

logger = logging.getLogger(__name__)

ZERO = Decimal(0)

DEFAULT_FEED = "aggtrade"
DEFAULT_FAST = 12
DEFAULT_SLOW = 26
DEFAULT_SIGNAL = 9

#: Recorded prints one warm-up read will pull. Two hours of a busy perp is
#: comfortably inside this; a smaller number would silently shorten the warm-up
#: on exactly the instruments that print fastest.
DEFAULT_WARMUP_LIMIT = 200_000

#: How many recent tape ids to keep for de-duplicating the live prints that
#: arrived while the tape was being read. The overlap is one read long — well
#: under a second — so this is generous by orders of magnitude.
_OVERLAP_GUARD = 2_000


@dataclass(frozen=True)
class _Bar:
    """One closed quote-volume bar."""

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    quote_volume: Decimal
    prints: int
    ts: float


class _Ema:
    """Exponential moving average seeded with its first sample."""

    def __init__(self, span: int) -> None:
        if span < 1:
            raise ValueError(f"EMA span must be >= 1, got {span}")
        self._alpha = Decimal(2) / Decimal(span + 1)
        self.value: Decimal | None = None

    def update(self, sample: Decimal) -> Decimal:
        if self.value is None:
            self.value = sample
        else:
            self.value += self._alpha * (sample - self.value)
        return self.value


class _BarBuilder:
    """Folds trade prints into quote-volume bars."""

    def __init__(self, threshold: Decimal) -> None:
        if threshold <= ZERO:
            raise ValueError(
                f"bar_quote_volume must be positive, got {threshold}"
            )
        self._threshold = threshold
        self._open: Decimal | None = None
        self._high = ZERO
        self._low = ZERO
        self._close = ZERO
        self._quote_volume = ZERO
        self._prints = 0
        self._ts = 0.0

    @property
    def quote_volume(self) -> Decimal:
        """Quote currency accumulated into the bar still being built."""
        return self._quote_volume

    def push(self, trade: Trade) -> _Bar | None:
        """Fold one print in. Returns the bar if this print closed it."""
        price = trade.price
        if price <= ZERO or trade.qty <= ZERO:
            # A venue does print these — a zero-quantity heartbeat, a wiped
            # field. They carry no volume, so they cannot move a bar's close,
            # and letting one set the open would put a phantom price on it.
            return None
        if self._open is None:
            self._open = price
            self._high = price
            self._low = price
        else:
            self._high = max(self._high, price)
            self._low = min(self._low, price)
        self._close = price
        self._quote_volume += price * trade.qty
        self._prints += 1
        self._ts = trade.ts
        if self._quote_volume < self._threshold:
            return None
        bar = _Bar(
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            quote_volume=self._quote_volume,
            prints=self._prints,
            ts=self._ts,
        )
        self._reset()
        return bar

    def _reset(self) -> None:
        self._open = None
        self._high = ZERO
        self._low = ZERO
        self._close = ZERO
        self._quote_volume = ZERO
        self._prints = 0


class MacdVolumeBars(Strategy):
    """MACD on quote-volume bars. Long only, market orders, one at a time."""

    name = "macd_volume"
    id = 7
    rebuildable = False

    def __init__(self) -> None:
        super().__init__()
        self._ticker: UniversalTicker | None = None
        self._info: SymbolInfo | None = None
        self._builder: _BarBuilder | None = None
        self._fast: _Ema | None = None
        self._slow: _Ema | None = None
        self._signal: _Ema | None = None
        self._bars_seen = 0
        self._required_bars = 0
        #: ``macd - signal`` on the last two closed bars. A cross is a change
        #: of sign between them, so both are kept rather than the sign alone.
        #: Warm-up bars advance this chain too: the tape and the live feed are
        #: one series, and a cross straddling the join is a real one.
        self._delta: Decimal | None = None
        self._prev_delta: Decimal | None = None
        self._warming = True
        self._pending_prints: list[Trade] = []
        self._seen_ids: set[str] = set()
        #: Newest event time seen on the tape, or None if the tape held
        #: nothing. None rather than zero because they are different claims:
        #: "no record is older than this" would be true of every print at a
        #: floor of zero, and the de-duplication below would discard the lot.
        self._last_tape_ts: float | None = None
        self._position = ZERO
        self._pending_cid: str | None = None

    # --- configuration -----------------------------------------------------

    @classmethod
    def on_initialized(cls, params: Any) -> dict[str, Any]:
        paras = super().on_initialized(params)

        feed = str(paras.get("feed", DEFAULT_FEED)).strip().lower()
        if feed not in {"aggtrade", "trade"}:
            raise ValueError(
                f"feed must be 'aggtrade' or 'trade', got {feed!r}. Those are "
                "the only recorded topics, and a bar needs one of them."
            )
        paras["feed"] = feed

        paras["bar_quote_volume"] = _positive_decimal(
            paras.get("bar_quote_volume"), "bar_quote_volume"
        )
        paras["qty_quote"] = _positive_decimal(
            paras.get("qty_quote"), "qty_quote"
        )

        fast = _positive_int(paras.get("fast", DEFAULT_FAST), "fast")
        slow = _positive_int(paras.get("slow", DEFAULT_SLOW), "slow")
        signal = _positive_int(paras.get("signal", DEFAULT_SIGNAL), "signal")
        if fast >= slow:
            raise ValueError(
                f"fast ({fast}) must be shorter than slow ({slow}); MACD is "
                "the fast EMA minus the slow one, and reversing them inverts "
                "every signal rather than failing"
            )
        paras["fast"] = fast
        paras["slow"] = slow
        paras["signal"] = signal
        paras["warmup_limit"] = _positive_int(
            paras.get("warmup_limit", DEFAULT_WARMUP_LIMIT), "warmup_limit"
        )
        return paras

    # --- lifecycle ---------------------------------------------------------

    async def on_start(self) -> None:
        if self.session is None:
            return
        try:
            self._resolve_feed()
        except ValueError as exc:
            self.fail(str(exc))
            return
        if len(self.session.td_api_ids) != 1:
            self.fail(
                "macd_volume trades one account; attach exactly one td, got "
                f"{self.session.td_api_ids}"
            )
            return

        self._builder = _BarBuilder(self.paras["bar_quote_volume"])
        self._fast = _Ema(self.paras["fast"])
        self._slow = _Ema(self.paras["slow"])
        self._signal = _Ema(self.paras["signal"])
        self._required_bars = self.paras["slow"] + self.paras["signal"]

        await self._warm_up()

    async def on_stop(self) -> None:
        # Nothing rests — every order this strategy sends is a MARKET order —
        # so there is nothing to cancel. The position, if any, is left open on
        # purpose: closing it here would turn "stop this session" into "sell my
        # inventory at whatever the book has", which is an operator's decision.
        if self._position > ZERO:
            await self.log(
                f"stopping with {self._position} still held — "
                "market orders leave nothing resting, but the position stays",
                level="warn",
            )

    # --- warm-up -----------------------------------------------------------

    async def _warm_up(self) -> None:
        """Build bars from MD's recorded tape, then from live prints."""
        topic = self.paras["feed"]
        try:
            tape = await self.tape.read(
                self._ticker, topic=topic, limit=self.paras["warmup_limit"]
            )
        except Exception as exc:
            # Not fatal. A warm-up that cannot read history is a warm-up that
            # takes longer, and the live feed is already arriving.
            await self.log(
                f"tape read failed ({exc}); warming up on live prints only",
                level="warn",
            )
            tape = None

        if tape is not None and tape.records:
            for record in tape.records:
                self._ingest(record)
                if record.trade_id:
                    self._seen_ids.add(record.trade_id)
                self._last_tape_ts = (
                    record.ts
                    if self._last_tape_ts is None
                    else max(self._last_tape_ts, record.ts)
                )
            if len(self._seen_ids) > _OVERLAP_GUARD:
                # Only the tail can overlap with what arrived during the read.
                self._seen_ids = set(
                    r.trade_id
                    for r in tape.records[-_OVERLAP_GUARD:]
                    if r.trade_id
                )
            await self.log(
                f"warm-up read {len(tape.records)} print(s) covering "
                f"{tape.span_ms / 1000:.0f}s → {self._bars_seen} bar(s); "
                f"tape recording={tape.recording}"
                + (
                    f", dropped {tape.dropped_before_gap} from before a gap"
                    if tape.dropped_before_gap
                    else ""
                )
            )
        else:
            await self.log(
                "warm-up found no recorded tape — is a tape_keeper holding "
                f"{self.paras['feed']}.{self._ticker}?",
                level="warn",
            )

        # Prints that arrived while the read was in flight. The md pump starts
        # before on_start runs, so without this they would be dropped, and with
        # a naive replay the ones already on the tape would be counted twice.
        replayed = 0
        for trade in self._pending_prints:
            if trade.universal_ticker != str(self._ticker):
                continue
            if self._is_duplicate(trade):
                continue
            self._ingest(trade)
            replayed += 1
        self._pending_prints.clear()
        self._warming = False
        self._seen_ids.clear()

        if self._bars_seen >= self._required_bars:
            await self.log(
                f"warm-up complete on {self._bars_seen} bar(s) "
                f"(+{replayed} live print(s)) — trading"
            )
        else:
            await self.log(
                f"warming up: {self._bars_seen}/{self._required_bars} bar(s) "
                f"(slow {self.paras['slow']} + signal {self.paras['signal']}). "
                "Aggregating live prints; no orders until then."
            )

    def _is_duplicate(self, trade: Trade) -> bool:
        """Whether this live print was already folded in from the tape."""
        if trade.trade_id:
            return trade.trade_id in self._seen_ids
        if self._last_tape_ts is None:
            # Nothing was read, so nothing can be a repeat of it.
            return False
        # No id to match on — fall back to time, and prefer dropping a print to
        # counting one twice. A bar is a sum, so a duplicate is a lasting error
        # while a miss is a rounding one.
        return trade.ts <= self._last_tape_ts

    # --- market data -------------------------------------------------------

    async def on_agg_trade(self, trade: AggTrade) -> None:
        await self._on_print(trade)

    async def on_trade(self, trade: Trade) -> None:
        await self._on_print(trade)

    async def _on_print(self, trade: Trade) -> None:
        # The ticker check tolerates not knowing yet. A print can reach a hook
        # before ``on_start`` has resolved the feed, and dropping it there
        # would lose exactly the prints the buffer below exists to keep. What
        # is buffered is filtered again on replay, once the answer is known.
        if self._ticker is not None and trade.universal_ticker != str(
            self._ticker
        ):
            return
        if self._warming:
            self._pending_prints.append(trade)
            return
        if self._ticker is None:
            return
        bar = self._ingest(trade)
        if bar is not None:
            await self._on_bar_closed(bar)

    def _ingest(self, trade: Trade) -> _Bar | None:
        """Fold one print into the bars and, if one closed, the indicator."""
        if (
            self._builder is None
            or self._fast is None
            or self._slow is None
            or self._signal is None
        ):
            return None
        bar = self._builder.push(trade)
        if bar is None:
            return None
        self._bars_seen += 1
        macd = self._fast.update(bar.close) - self._slow.update(bar.close)
        signal = self._signal.update(macd)
        self._prev_delta = self._delta
        self._delta = macd - signal
        return bar

    async def _on_bar_closed(self, bar: _Bar) -> None:
        delta = self._delta
        previous = self._prev_delta
        if delta is None:
            return

        if self._bars_seen < self._required_bars:
            if self._bars_seen % 10 == 0:
                await self.log(
                    f"warming up: {self._bars_seen}/{self._required_bars} bar(s)"
                )
            return
        if self._bars_seen == self._required_bars:
            await self.log(
                f"warm-up complete on {self._bars_seen} bar(s) — trading"
            )
        if previous is None:
            # First comparable bar: there is no previous side to have crossed
            # from, and treating "started above" as a cross would open a
            # position on the accident of when the session began.
            return

        if previous <= ZERO < delta:
            await self._enter(bar)
        elif previous >= ZERO > delta:
            await self._exit(bar)

    # --- orders ------------------------------------------------------------

    async def _enter(self, bar: _Bar) -> None:
        if self._position > ZERO:
            return
        if self._pending_cid is not None:
            await self.log(
                "bullish cross ignored — an order is still in flight",
                level="warn",
            )
            return
        info = await self._instrument()
        if info is None:
            return
        qty = info.qty_for_notional(self.paras["qty_quote"], bar.close)
        if qty <= ZERO or not info.meets_minimums(qty, bar.close):
            await self.log(
                f"bullish cross skipped — {self.paras['qty_quote']} at "
                f"{bar.close} rounds to {qty}, under the venue's minimums",
                level="warn",
            )
            return
        await self._submit(Side.BUY, qty, bar, "bullish cross")

    async def _exit(self, bar: _Bar) -> None:
        if self._position <= ZERO:
            # Long only: a bearish cross while flat is not an instruction to
            # go short.
            return
        if self._pending_cid is not None:
            await self.log(
                "bearish cross ignored — an order is still in flight",
                level="warn",
            )
            return
        info = await self._instrument()
        if info is None:
            return
        qty = info.round_qty(self._position)
        if qty <= ZERO:
            await self.log(
                f"bearish cross skipped — position {self._position} rounds to "
                "nothing at this venue's lot step",
                level="warn",
            )
            return
        await self._submit(Side.SELL, qty, bar, "bearish cross")

    async def _submit(
        self, side: Side, qty: Decimal, bar: _Bar, why: str
    ) -> None:
        api_id = self.session.td_api_ids[0]
        accepted = await self.oms.submit_order(
            api_id,
            ticker=self._ticker,
            side=side,
            qty=qty,
            type=OrderType.MARKET,
        )
        cid = self.oms.last_client_order_id
        if not accepted:
            await self.log(
                f"{why}: TD refused the {side} of {qty} — "
                f"{self.oms.last_reject_reason}",
                level="error",
            )
            return
        self._pending_cid = cid
        await self.log(
            f"{why} on bar close {bar.close} "
            f"({self._bars_seen} bars) → MARKET {side} {qty} cid={cid}"
        )

    async def _instrument(self) -> SymbolInfo | None:
        """Instrument metadata, fetched once and cached for the session."""
        if self._info is not None:
            return self._info
        try:
            self._info = await self.symbols.get(self._ticker)
        except Exception as exc:
            await self.log(
                f"cannot resolve {self._ticker}: {exc}", level="error"
            )
            return None
        return self._info

    # --- private events ----------------------------------------------------

    async def on_fill(self, api_id: int, fill: Fill) -> None:
        if not self.owns(fill.client_order_id):
            return
        if fill.side is Side.BUY:
            self._position += fill.qty
        else:
            self._position -= fill.qty
        if self._position < ZERO:
            # A sell that filled more than this strategy thought it held. The
            # venue is right and the arithmetic here is not, so say so and take
            # the venue's answer rather than carrying a negative into the next
            # entry check.
            await self.log(
                f"position went negative ({self._position}) after a fill — "
                "treating as flat",
                level="warn",
            )
            self._position = ZERO
        await self.log(
            f"fill {fill.side} {fill.qty} @ {fill.price} → position "
            f"{self._position}"
        )

    async def on_order_update(self, api_id: int, order: Order) -> None:
        if not self.owns(order.client_order_id):
            return
        if (
            self._pending_cid is not None
            and str(order.client_order_id) == str(self._pending_cid)
            and is_terminal(order.status)
        ):
            self._pending_cid = None

    async def on_order_reject(self, api_id: int, reject: OrderReject) -> None:
        if not self.owns(reject.client_order_id):
            return
        self._pending_cid = None
        await self.log(
            f"venue rejected the order: {reject.reason}", level="error"
        )

    async def on_cancel_reject(self, api_id: int, reject: CancelReject) -> None:
        # Nothing here ever cancels — market orders do not rest — so one of
        # these is somebody else's, or a bug worth seeing.
        if self.owns(reject.client_order_id):
            await self.log(
                f"unexpected cancel reject for our cid: {reject.reason}",
                level="warn",
            )

    # --- helpers -----------------------------------------------------------

    def _resolve_feed(self) -> None:
        """Pin the instrument, and refuse a double-counted subscription."""
        md_ids = list(self.session.md_ids) if self.session is not None else []
        wanted = self.paras["feed"]
        other = "trade" if wanted == "aggtrade" else "aggtrade"

        mine: list[UniversalTicker] = []
        clashing: list[str] = []
        for feed in md_ids:
            topic, _, rest = feed.partition(".")
            if topic == wanted:
                mine.append(UniversalTicker.resolve(rest))
            elif topic == other:
                clashing.append(feed)

        if clashing:
            raise ValueError(
                f"subscribed to both {wanted} and {clashing} — they report the "
                "same matches, so every bar would count its volume twice. "
                f"Keep {wanted} and drop the other."
            )
        if not mine:
            raise ValueError(
                f"no {wanted} feed in md {md_ids}; macd_volume builds its bars "
                f"from {wanted} prints"
            )
        if len(mine) > 1:
            raise ValueError(
                f"macd_volume trades one instrument, got {wanted} feeds for "
                f"{[str(t) for t in mine]}"
            )
        self._ticker = mine[0]


def _positive_decimal(raw: Any, name: str) -> Decimal:
    if raw is None:
        raise ValueError(f"{name} is required")
    try:
        value = Decimal(str(raw))
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        raise ValueError(f"{name} must be a number, got {raw!r}") from None
    if value <= ZERO:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _positive_int(raw: Any, name: str) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a whole number, got {raw!r}") from None
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value
