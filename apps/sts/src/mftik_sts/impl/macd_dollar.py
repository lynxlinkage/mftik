"""MACD over dollar bars — long only, in on a bullish cross, out on a bearish one.

**Bars.** Not time bars, and specifically *dollar* bars rather than volume bars.
One closes when the trades folded into it have carried ``bar_quote_volume`` of
the **quote** currency — ``price * qty`` summed, so USDT on a ``*USDT``
instrument, not units of the base asset. The distinction is the whole reason for
the name: a volume bar counts BTC, a dollar bar counts what the BTC was worth,
and on this instrument a threshold meant for one is out by five orders of
magnitude if read as the other.

Dollar rather than volume because the alternative drifts. The same 100 BTC is
four times the market activity at $120k that it was at $30k, so a volume bar
quietly changes what it measures as the price moves, while a fixed amount of
value does not. ("Dollar" is the standard name for the sampling scheme; the
currency is whatever the instrument quotes in.)

Either way a bar is a fixed amount of *business* rather than a fixed amount of
clock. A quiet hour produces one bar and a violent minute produces thirty, which
is the property the indicator wants: a time bar spends most of the day
describing an idle book.

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

**Orders.** ``LIMIT`` + ``IOC``, priced through the touch by ``cross_bps``, one
at a time, sized ``qty_quote`` of the quote currency.

Not ``MARKET``, because a market order's quantity does not mean the same thing
everywhere — some venues read it as base units, some as an amount of the quote
currency — and a strategy that sizes in quote currency and converts to base has
already decided which it means. A limit priced through the touch takes the same
liquidity a market order would, but the quantity is the one that was asked for
and the price carries a bound: ``cross_bps`` is the most slippage this will
accept, stated rather than discovered.

Nothing rests. An IOC either takes what is there now or is cancelled, so there
is no resting order to cancel on the way out and no repricing to do.

Priced off the live ``bestquote``, not off the bar that triggered it. A bar
closes on volume, and by the time it does its close is already history; the
order has to cross a book that exists now.

**Long only, and the two sides are not symmetric.** A bullish cross with no
position opens one — once, at the cross. If that IOC takes nothing, the trade
is missed and the strategy waits for the next signal rather than chasing a
market that has moved.

A bearish cross closes the position, and keeps trying on every bar that stays
bearish until it is flat. The asymmetry is deliberate: an entry that does not
fill costs an opportunity, while an exit that does not fill leaves a position
nobody decided to keep. Only one of those grows.

A bearish cross while flat does nothing — it is not an instruction to go short.

**Where the position comes from, and it depends on the market.**

On a *contract*, the venue is the authority. The session's automatic recon
reports what is already open, so a session that starts on an account already
holding this instrument starts long rather than flat, and one that starts on a
flat account holds. From then on ``on_position_update`` maintains it — a
position moves on funding, ADL and liquidation, none of which arrive as a fill,
so a tally of this strategy's own fills would drift from the truth exactly when
being wrong is most expensive. The exit is sized from that figure and sent
``reduce_only``: the intent is *be flat*, and the flag makes the venue refuse
an overshoot rather than turn it into a short.

Nothing is sent until recon has answered. This is a second gate, independent of
the warm-up: the indicator is often ready first, and an entry sized against a
position the strategy cannot see is the one mistake this ordering exists to
prevent.

An account already *short* fails the session. Long-only has no state for it,
and a bullish cross would place a buy that shrinks somebody else's short
instead of opening a long — wrong, and silently so.

On *spot* none of that applies. There are no positions, only balances, and a
holding of the base asset belongs to whoever put it there. The strategy starts
flat, counts its own fills, and never sends ``reduce_only`` — TD refuses a spot
order carrying it, which is the correct answer to asking for a guarantee spot
cannot give.

**Rebuild.** Off. The position is real and recon would report it, but reasoning
about a restored position against an indicator rebuilt from a different stretch
of tape is a decision this strategy has not been given. See
:meth:`~mftik.strategy.Strategy.on_rebuild`.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from mftik.exchange.models import (
    AggTrade,
    BestQuote,
    Fill,
    Order,
    OrderType,
    Side,
    TimeInForce,
    Trade,
    is_terminal,
)
from mftik.exchange.oms import Position
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.protocol import CancelReject, OrderReject, ReconDone, SymbolInfo
from mftik.strategy import Strategy

logger = logging.getLogger(__name__)

ZERO = Decimal(0)

DEFAULT_FEED = "aggtrade"
DEFAULT_FAST = 12
DEFAULT_SLOW = 26
DEFAULT_SIGNAL = 9

#: How far through the touch an IOC is priced, in basis points. This is the
#: slippage the strategy agrees to in advance: too small and the order takes
#: only the top level before expiring, too large and it pays for depth it did
#: not need. It is not a fee — an IOC that fills at the touch pays the touch.
DEFAULT_CROSS_BPS = Decimal("5")

BPS = Decimal("10000")

#: A quote older than this is not priced off. At book speed a quote goes stale
#: in well under a second, and an IOC aimed at a book that has moved is either
#: a miss or a fill somewhere nobody chose.
QUOTE_MAX_AGE_S = 5.0

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
    """One closed dollar bar — a fixed amount of quote currency traded."""

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
    """Folds trade prints into dollar bars."""

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


class MacdDollarBars(Strategy):
    """MACD on dollar bars. Long only, IOC through the touch."""

    name = "macd_dollar"
    id = 7
    rebuildable = False

    def __init__(self) -> None:
        super().__init__()
        self._ticker: UniversalTicker | None = None
        #: Whether this instrument has positions at all. Decides where the
        #: position comes from and what an exit means — see the class
        #: docstring on the two markets.
        self._contract = False
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
        #: Whether :attr:`_position` yet describes anything real. True from the
        #: start on spot, where zero is the truth and this strategy's own fills
        #: are what change it. False on a contract until recon answers: the
        #: account may already be holding something, and an entry sent before
        #: that is known would be sizing against a position it cannot see.
        self._position_known = False
        self._pending_cid: str | None = None
        #: Latest top of book, for pricing an IOC through it.
        self._quote: BestQuote | None = None
        #: Set by a bearish cross, cleared once flat. While it is on, every
        #: bar that stays bearish tries the exit again — an IOC can take
        #: nothing, and one unfilled attempt must not leave the position.
        self._exiting = False

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
        paras["cross_bps"] = _positive_decimal(
            paras.get("cross_bps", DEFAULT_CROSS_BPS), "cross_bps"
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
                "macd_dollar trades one account; attach exactly one td, got "
                f"{self.session.td_api_ids}"
            )
            return

        self._builder = _BarBuilder(self.paras["bar_quote_volume"])
        self._fast = _Ema(self.paras["fast"])
        self._slow = _Ema(self.paras["slow"])
        self._signal = _Ema(self.paras["signal"])
        self._required_bars = self.paras["slow"] + self.paras["signal"]

        self._contract = self._ticker.category is not Category.SPOT
        if self._contract:
            # Nothing is known until recon answers. The session sends it
            # automatically on the first lease ack, so this is a wait rather
            # than a request.
            self._position_known = False
            await self.log(
                f"{self._ticker.category} account — waiting for recon to say "
                "what position is already open"
            )
        else:
            # Spot has no position to inherit. A holding of the base asset is
            # a balance, and it belongs to whoever put it there — this
            # strategy starts flat and counts only its own fills.
            self._position_known = True
            self._position = ZERO

        await self._warm_up()

    async def on_stop(self) -> None:
        # Nothing rests — every order this strategy sends is an IOC, which the
        # venue either fills or cancels — so there is nothing to cancel here.
        # The position, if any, is left open on purpose: closing it would turn
        # "stop this session" into "sell my inventory into whatever the book
        # has right now", which is an operator's decision, not a teardown step.
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

    async def on_best_quote(self, quote: BestQuote) -> None:
        """Keep the touch an order will be priced through.

        Not folded into the bars: this feed says what the book looks like, not
        that anything traded, and a bar is a measure of what traded.
        """
        if self._ticker is None or quote.universal_ticker != str(self._ticker):
            return
        self._quote = quote

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
        if not self._position_known:
            # A second gate, independent of the warm-up above: the indicator
            # can be ready long before recon has said what the account holds.
            # Acting now would size an entry against a position it cannot see.
            await self.log(
                "signal ignored — still waiting for recon to report the "
                "position",
                level="warn",
            )
            return

        if previous <= ZERO < delta:
            self._exiting = False
            await self._enter()
        elif previous >= ZERO > delta:
            # Arm the exit here rather than only acting once. An IOC can take
            # nothing, and the cross that told us to get out happens once.
            self._exiting = True
            await self._exit("bearish cross")
        elif self._exiting and delta < ZERO:
            # Still bearish, still holding: the previous attempt did not
            # finish the job. Entries get no such second chance — see the
            # module docstring on why the two sides are not symmetric.
            await self._exit("still bearish")

    # --- orders ------------------------------------------------------------

    async def _enter(self) -> None:
        if self._position > ZERO or self._pending_cid is not None:
            return
        info = await self._instrument()
        if info is None:
            return
        price = self._crossing_price(Side.BUY, info)
        if price is None:
            return
        qty = info.qty_for_notional(self.paras["qty_quote"], price)
        if qty <= ZERO or not info.meets_minimums(qty, price):
            await self.log(
                f"bullish cross skipped — {self.paras['qty_quote']} at "
                f"{price} rounds to {qty}, under the venue's minimums",
                level="warn",
            )
            return
        await self._submit(Side.BUY, qty, price, "bullish cross")

    async def _exit(self, why: str) -> None:
        if self._position <= ZERO:
            # Long only: a bearish cross while flat is not an instruction to
            # go short.
            self._exiting = False
            return
        if self._pending_cid is not None:
            return
        info = await self._instrument()
        if info is None:
            return
        price = self._crossing_price(Side.SELL, info)
        if price is None:
            return
        # On a contract the size is the venue's open position and the order is
        # reduce-only: the intent is "be flat", not "sell the amount I think I
        # bought". Those differ whenever funding, ADL or another session has
        # moved the position, and reduce_only makes the venue refuse an
        # overshoot rather than turn it into a short.
        #
        # On spot there is no position to reduce and no flag to send — the
        # tally of this strategy's own fills is the whole truth.
        qty = info.round_qty(self._position)
        if qty <= ZERO:
            await self.log(
                f"{why}: position {self._position} rounds to nothing at this "
                "venue's lot step — treating as flat",
                level="warn",
            )
            self._exiting = False
            return
        await self._submit(
            Side.SELL, qty, price, why, reduce_only=self._contract
        )

    def _crossing_price(self, side: Side, info: SymbolInfo) -> Decimal | None:
        """Price an IOC through the touch, snapped to the venue's tick.

        Returns None when there is no usable quote — which is a reason not to
        send anything. An order priced off a stale book is a fill at a price
        nobody agreed to.
        """
        quote = self._quote
        if quote is None:
            logger.warning(
                "macd_dollar has no quote yet for %s", self._ticker
            )
            return None
        age = time.time() - quote.ts
        if age > QUOTE_MAX_AGE_S:
            logger.warning(
                "macd_dollar quote for %s is %.1fs old — not pricing off it",
                self._ticker,
                age,
            )
            return None

        offset = self.paras["cross_bps"] / BPS
        if side is Side.BUY:
            touch = quote.ask
            if touch <= ZERO:
                return None
            raw = touch * (Decimal(1) + offset)
            # round_price floors, which for a buy can land *under* the ask and
            # turn a crossing order into one that rests and then expires. Step
            # back up until it crosses again.
            price = info.round_price(raw)
            tick = info.price_tick
            if tick is not None and price < touch:
                price = info.round_price(touch + tick)
            return price if price > ZERO else None

        touch = quote.bid
        if touch <= ZERO:
            return None
        # Flooring is already the aggressive direction for a sell.
        price = info.round_price(touch * (Decimal(1) - offset))
        return price if price > ZERO else None

    async def _submit(
        self,
        side: Side,
        qty: Decimal,
        price: Decimal,
        why: str,
        *,
        reduce_only: bool = False,
    ) -> None:
        api_id = self.session.td_api_ids[0]
        accepted = await self.oms.submit_order(
            api_id,
            ticker=self._ticker,
            side=side,
            qty=qty,
            type=OrderType.LIMIT,
            price=price,
            tif=TimeInForce.IOC,
            reduce_only=reduce_only,
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
            f"{why} ({self._bars_seen} bars) → IOC {side} {qty} @ {price} "
            f"(+{self.paras['cross_bps']}bps through the touch"
            f"{', reduce-only' if reduce_only else ''}) cid={cid}"
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

    async def on_recon_done(self, msg: ReconDone) -> None:
        """Take the venue's word for what is already open.

        Contract only. This is the one moment the strategy can learn about a
        position it did not place — carried over from a previous session, or
        opened by something else on the same account — and starting long is
        the correct state to start in when the account is long.

        Spot never gets here: a base-asset balance is not this strategy's
        position, and treating one as inherited inventory would have it sell
        coins somebody else is holding.
        """
        if not self._contract or self._ticker is None:
            return
        position = msg.oms.positions.get(str(self._ticker))
        qty = ZERO if position is None else position.qty
        self._position_known = True

        if qty < ZERO:
            # Long-only has no answer for an account that is already short.
            # A bullish cross here would buy, and that buy would shrink
            # somebody else's short rather than open this strategy's long —
            # the position would be wrong and the accounting silently so.
            self.fail(
                f"account is short {qty} of {self._ticker}; macd_dollar is "
                "long only and will not trade against a position it did not "
                "open"
            )
            return

        self._position = qty
        if qty > ZERO:
            await self.log(
                f"recon: already long {qty} {self._ticker} — starting from "
                "that position rather than flat"
            )
        else:
            await self.log(f"recon: flat on {self._ticker}")

    async def on_position_update(self, api_id: int, position: Position) -> None:
        """The venue's own figure, and on a contract it wins.

        A position moves on funding, ADL and liquidation, none of which arrive
        as a fill — so a tally of this strategy's own fills drifts from the
        truth exactly when it matters most. On spot this never fires.
        """
        if not self._contract or self._ticker is None:
            return
        if position.universal_ticker != str(self._ticker):
            return
        self._position_known = True
        previous = self._position
        self._position = max(position.qty, ZERO)
        if position.qty < ZERO:
            await self.log(
                f"venue reports a short of {position.qty} — this strategy "
                "does not open shorts; treating as flat and leaving it alone",
                level="error",
            )
        elif self._position != previous:
            await self.log(
                f"venue position {previous} → {self._position}"
            )
        if self._position <= ZERO:
            self._exiting = False

    async def on_fill(self, api_id: int, fill: Fill) -> None:
        if not self.owns(fill.client_order_id):
            return
        if self._contract:
            # The venue is the authority here and says so through
            # on_position_update. Counting fills as well would give two
            # answers that disagree the first time funding moves the position.
            await self.log(
                f"fill {fill.side} {fill.qty} @ {fill.price} "
                f"(position from the venue: {self._position})"
            )
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
        if self._position <= ZERO:
            # Flat: whatever the exit was chasing, it is done.
            self._exiting = False
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
        # Nothing here ever cancels — an IOC is already gone — so one of these
        # is somebody else's, or a bug worth seeing.
        if self.owns(reject.client_order_id):
            await self.log(
                f"unexpected cancel reject for our cid: {reject.reason}",
                level="warn",
            )

    # --- helpers -----------------------------------------------------------

    def _resolve_feed(self) -> None:
        """Pin the instrument, and refuse a subscription it cannot trade on.

        Two feeds are required and they do different jobs: the trade feed
        builds the bars, the quote feed prices the orders. A session with only
        the first would compute every signal correctly and then have no book to
        cross, which is a failure worth having at start rather than at the
        first cross.
        """
        md_ids = list(self.session.md_ids) if self.session is not None else []
        wanted = self.paras["feed"]
        other = "trade" if wanted == "aggtrade" else "aggtrade"

        mine: list[UniversalTicker] = []
        clashing: list[str] = []
        quoted: list[UniversalTicker] = []
        for feed in md_ids:
            topic, _, rest = feed.partition(".")
            if topic == wanted:
                mine.append(UniversalTicker.resolve(rest))
            elif topic == other:
                clashing.append(feed)
            elif topic == "bestquote":
                quoted.append(UniversalTicker.resolve(rest))

        if clashing:
            raise ValueError(
                f"subscribed to both {wanted} and {clashing} — they report the "
                "same matches, so every bar would count its volume twice. "
                f"Keep {wanted} and drop the other."
            )
        if not mine:
            raise ValueError(
                f"no {wanted} feed in md {md_ids}; macd_dollar builds its bars "
                f"from {wanted} prints"
            )
        if len(mine) > 1:
            raise ValueError(
                f"macd_dollar trades one instrument, got {wanted} feeds for "
                f"{[str(t) for t in mine]}"
            )
        ticker = mine[0]
        if ticker not in quoted:
            raise ValueError(
                f"no bestquote feed for {ticker} in md {md_ids}; macd_dollar "
                "prices its IOCs through the touch and has no book without one"
            )
        self._ticker = ticker


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
