"""Chase order — rest passively, follow the book, finish the size.

One TD account, one best-quote feed. The strategy keeps a single post-only
order resting near the touch and repriced as the market moves, until the whole
size is filled. It never crosses on its own: taking liquidity is what
``must_exec`` does at the end, deliberately, and nowhere else.

**The price it posts.** A BUY references the *ask* — what it would have to pay
to take — and posts ``gap_bps`` below it, which lands inside the spread. A SELL
mirrors it: reference the bid, post ``gap_bps`` above. Referencing the far side
rather than its own is what makes the order lean toward getting filled instead
of resting behind the whole queue.

**Repricing.** On each ``refresh_interval_ms`` tick the target price is
recomputed; if the resting order has drifted more than ``gap_bps`` from it, the
order is cancelled and reposted at the new price. The replacement waits for the
cancel to be confirmed rather than going out immediately — two live orders for
the same remaining size can both fill, and that overfills the position.

**How it ends.** Only a complete fill is the ordinary ending. Two guards cut it
short:

* ``expiry_s`` — seconds from ``on_start``. Anchored at start rather than at
  the first order so a feed that never delivers still terminates the session.
* ``extreme_bps`` — how far the reference price may run away from where it was
  when the chase began, in the direction that costs us. Past that, chasing is
  buying a worse and worse price.

At either, whatever is still resting is cancelled. ``must_exec`` then decides
what that means: ``false`` leaves the remainder undone, ``true`` takes it —
including the remainder of a partial fill, which is the case that would
otherwise leave a half-sized position behind.

**How ``must_exec`` takes it.** In IOC slices priced at the far touch, not in
one market order. A market order walks as deep into the book as it needs in a
single shot, and the tail of that walk is the worst price of the whole
execution. An IOC at the touch takes only what is resting there, pauses, and
re-reads the book — so the size is worked off as liquidity refills. It is
slower, and it can end with size still unfilled after ``IOC_MAX_SLICES``, which
is logged as an error because the guarantee did not hold.

Sizing the slices as limit orders has a second effect worth knowing: it is what
lets ``must_exec`` work on the **buy** side of ``Gate`` at all. Gate sizes
a spot *market* buy in quote currency, which the shared order interface cannot
express, so its adapter refuses one outright — a limit IOC sidesteps that
entirely.

Size comes from ``qty`` (base units) or ``qty_quote`` (quote currency), exactly
one. It is fixed the first time it is computed, so the target does not drift
with the price the rest of the way.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

from mftik.exchange.models import (
    BestQuote,
    Fill,
    Order,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
)
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    CancelReject,
    OrderReject,
    ReconDone,
    RejectCode,
    SymbolInfo,
    Topics,
)
from mftik.protocol.reject_codes import describe, is_normalized
from mftik.strategy import Strategy
from mftik.strategy.timer import TimerToken

DEFAULT_REFRESH_INTERVAL_MS = 1000
DEFAULT_GAP_BPS = Decimal("10")

BPS = Decimal("10000")

#: Quotes between trail heartbeats. The book moves faster than anyone reads.
LOG_EVERY = 20

#: How long to wait for an order to reach a terminal state before giving up on
#: it, and how often to look. Only bounds the shutdown path; ordinary
#: repricing waits for the same confirmation on the next tick instead.
CANCEL_WAIT_S = 2.0
CANCEL_POLL_S = 0.05

#: ``must_exec`` takes the remainder in IOC slices at the touch rather than in
#: one market order. A market order walks however deep it must in a single
#: shot; an IOC at the far touch takes only what is resting there and leaves
#: the rest to the next quote, so the book refills between slices instead of
#: being swept. It costs a pause per slice, which is the trade being made.
IOC_MAX_SLICES = 10
IOC_SLICE_PAUSE_S = 0.25

#: Keys the two unrecoverable facts are kept under. Both are set once and
#: never change, which is what makes keeping them safe: neither has a version
#: that can go stale between being written and being read.
_FACT_STARTED_MS = "started_ms"
_FACT_REF_START = "ref_start"

#: Statuses that mean the venue is done with an order and it is safe to place
#: its replacement.
_TERMINAL = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED}
)


def _positive(paras: dict[str, Any], name: str) -> Decimal:
    """Read a required positive Decimal, saying which knob is wrong and why."""
    raw = paras.get(name)
    if raw is None:
        raise ValueError(f"{name} is required")
    try:
        value = Decimal(str(raw))
    except ArithmeticError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


#: What TD prefixed a pre-lock refusal with before it carried a code. Kept as
#: a fallback so an older TD, or one whose ack we could not read, still lands
#: on the sharper exit reason. Anything unrecognised still ends the session,
#: just under the generic name — drifting out of sync degrades, not breaks.
_NO_FUNDS = "insufficient balance"


def _refusal_reason(code: int | str, reason: str) -> str:
    """Exit reason for a TD refusal, naming the one cause worth singling out."""
    if code == RejectCode.TD_INSUFFICIENT_BALANCE:
        return "chase_insufficient_balance"
    if not is_normalized(code) and _NO_FUNDS in reason.lower():
        return "chase_insufficient_balance"
    return "chase_refused"


def _floor_hint(info: SymbolInfo, price: Decimal | None) -> str:
    """What the venue's smallest order would cost, for an unsizeable config.

    A size that rounds to zero is almost always a ``qty_quote`` below one lot:
    Bybit's ETHUSDT perp steps in 0.01 ETH, so nothing under ~20 USDT can be
    expressed however the number is written. Saying which number would work
    turns the failure into a fix.
    """
    floor = max(
        info.filter("qty_step") or Decimal("0"),
        info.filter("min_qty") or Decimal("0"),
    )
    if floor <= 0 or price is None or price <= 0:
        return ""
    notional = floor * price
    min_notional = info.filter("min_notional")
    if min_notional is not None and min_notional > notional:
        notional = min_notional
    return (
        f" — its smallest order is {_fmt(floor)} "
        f"(about {_fmt(notional.quantize(Decimal('0.01')))} at {_fmt(price)})"
    )


def _fmt(value: object) -> str:
    """Compact Decimal for logs — drop trailing zeros from Numeric(38, 18)."""
    if value is None:
        return "?"
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(d.normalize(), "f")


def _bps(value: Decimal | None) -> str:
    """Two decimals. A bps ratio carries 28 significant digits of nobody's business."""
    if value is None:
        return "?"
    return format(value.quantize(Decimal("0.01")), "f")


class ChaseOrder(Strategy):
    name = "chase"
    id = 2
    #: Restorable: `on_rebuild` takes back the clock and the slippage anchor,
    #: and recon says what is still resting.
    rebuildable = True

    def __init__(self) -> None:
        super().__init__()
        self._tick_token: TimerToken | None = None
        self._ticker: UniversalTicker | None = None
        self._info: SymbolInfo | None = None
        #: Latest price on the side we would have to cross to take.
        self._ref: Decimal | None = None
        #: That same price when the chase armed — the anchor for slippage.
        self._ref_start: Decimal | None = None
        self._quotes = 0
        self._starved_ticks = 0
        self._started_ms: int | None = None
        #: Total base qty to execute, fixed once so it cannot drift with price.
        self._target_qty: Decimal | None = None
        #: The order we currently believe is live, and where we put it.
        self._open_cid: str | None = None
        self._open_price: Decimal | None = None
        #: Set between asking for a cancel and the venue confirming it. No
        #: replacement goes out while it is true.
        self._canceling = False
        #: cid → cumulative filled qty. Summed across replacements, because a
        #: cancelled order can still have filled part of its size.
        self._filled: dict[str, Decimal] = {}
        #: Set once TD recon has landed and the tick timer is really running.
        self._armed = False
        #: ``ts`` of the order adopted on rebuild, so a recon snapshot holding
        #: both an order and its replacement keeps the newer one.
        self._adopted_ts = 0.0
        #: Set when this session ran before. Recon then means "here is what
        #: you left resting", not "here is a clean account".
        self._restoring = False
        self._done = False

    # --- parameters --------------------------------------------------------

    @classmethod
    def on_initialized(cls, params: Any) -> dict[str, Any]:
        out = super().on_initialized(params)

        raw_side = str(out.get("side", "")).strip().lower()
        if raw_side not in (Side.BUY.value, Side.SELL.value):
            raise ValueError(
                f"side must be {Side.BUY.value!r} or {Side.SELL.value!r}, "
                f"got {out.get('side')!r}"
            )
        out["side"] = Side(raw_side)

        # Exactly one sizing knob. Both would be two different answers to the
        # same question, and neither leaves nothing to send.
        has_qty = out.get("qty") is not None
        has_quote = out.get("qty_quote") is not None
        if has_qty and has_quote:
            raise ValueError(
                "set either qty or qty_quote, not both: qty is in base units, "
                "qty_quote in the pair's quote currency"
            )
        if not has_qty and not has_quote:
            raise ValueError("one of qty or qty_quote is required")
        if has_qty:
            qty = Decimal(str(out["qty"]))
            if qty <= 0:
                raise ValueError(f"qty must be positive, got {qty}")
            out["qty"] = qty
        else:
            qty_quote = Decimal(str(out["qty_quote"]))
            if qty_quote <= 0:
                raise ValueError(f"qty_quote must be positive, got {qty_quote}")
            out["qty_quote"] = qty_quote

        gap_bps = Decimal(str(out.get("gap_bps", DEFAULT_GAP_BPS)))
        if gap_bps <= 0:
            # At zero the order posts exactly at the far touch, which is the
            # one price post-only is guaranteed to refuse.
            raise ValueError(f"gap_bps must be positive, got {gap_bps}")
        out["gap_bps"] = gap_bps

        # Both guards are required: without them a chase that never fills has
        # no ending, and the session would sit there repricing forever.
        out["expiry_s"] = _positive(out, "expiry_s")
        out["extreme_bps"] = _positive(out, "extreme_bps")

        # Defaults to false: finishing with a market order spends money the
        # config did not ask to spend, so it has to be asked for.
        out["must_exec"] = bool(out.get("must_exec", False))

        interval = int(
            out.get("refresh_interval_ms", DEFAULT_REFRESH_INTERVAL_MS)
        )
        if interval <= 0:
            raise ValueError(
                f"refresh_interval_ms must be positive, got {interval}"
            )
        out["refresh_interval_ms"] = interval
        return out

    # --- lifecycle ---------------------------------------------------------

    async def on_start(self) -> None:
        self._resolve_feed()
        await self.log(
            f"ChaseOrder started ticker={self._ticker} "
            f"side={self.paras['side'].value} "
            f"gap_bps={_fmt(self.paras['gap_bps'])} "
            f"expiry_s={_fmt(self.paras['expiry_s'])} "
            f"extreme_bps={_fmt(self.paras['extreme_bps'])} "
            f"must_exec={self.paras['must_exec']} "
            f"size={self._size_description()} "
            f"refresh_interval_ms={self.paras['refresh_interval_ms']}"
        )
        self._tick_token = self.timer.token()
        # Nothing is placed until recon lands, because the balance check that
        # gates every order reads TD's ledger and the ledger is not real until
        # then. Until it does, the token holds a one-shot deadline so a recon
        # that never arrives ends the session instead of hanging it.
        self._tick_token.register(
            self.timer.now_ms() + int(self.paras["expiry_s"] * 1000),
            0,
            self._on_recon_timeout,
        )

    async def on_ready(self) -> None:
        await self.log("ChaseOrder ready — waiting for TD recon to arm")

    async def on_rebuild(self, remembered: dict[str, str]) -> None:
        """This chase ran before. Take back the two facts it cannot re-derive.

        Everything else waits for recon, which is the only thing that knows
        what is actually resting at the venue now — an order can have filled
        or been cancelled while STS was away, and acting on a remembered copy
        of it would be acting on something that is no longer true.

        The clock and the anchor are different: nothing outside the process
        ever saw them, and neither changes once set. Without them a rebuilt
        chase restarts its expiry budget and re-anchors its slippage guard on
        wherever the market has since moved to — quietly granting itself a
        fresh allowance of both.
        """
        self._restoring = True
        started = remembered.get(_FACT_STARTED_MS)
        if started is not None:
            try:
                self._started_ms = int(started)
            except ValueError:
                await self.log(
                    f"ChaseOrder ignoring unreadable {_FACT_STARTED_MS}="
                    f"{started!r} — the expiry budget restarts",
                    level="warn",
                )
        anchor = remembered.get(_FACT_REF_START)
        if anchor is not None:
            try:
                self._ref_start = Decimal(anchor)
            except ArithmeticError:
                await self.log(
                    f"ChaseOrder ignoring unreadable {_FACT_REF_START}="
                    f"{anchor!r} — slippage re-anchors on the next quote",
                    level="warn",
                )
        await self.log(
            f"ChaseOrder restoring — anchor={_fmt(self._ref_start)} "
            f"elapsed={self._elapsed_s()}s of {_fmt(self.paras['expiry_s'])}s"
        )

    def _elapsed_s(self) -> str:
        if self._started_ms is None:
            return "?"
        return str(int((self.timer.now_ms() - self._started_ms) / 1000))

    async def _adopt(self, msg: ReconDone) -> None:
        """Take back the orders this session left resting at the venue.

        Read from recon rather than from anything remembered: this is the one
        account of the world that is current. An order this chase placed may
        have filled or been cancelled while STS was away, and only the venue
        knows which.
        """
        adopted = 0
        for cid, order in msg.oms.orders.items():
            if not self.owns(cid):
                continue
            if order.filled_qty > 0:
                self._filled[cid] = order.filled_qty
            if order.status in _TERMINAL:
                continue
            # Only one order rests at a time, but recon is a snapshot and a
            # replacement may have crossed with a cancel. Keep the newest and
            # let the tick reconcile the rest.
            if self._open_cid is None or order.ts >= self._adopted_ts:
                self._open_cid = cid
                self._open_price = order.price
                self._adopted_ts = order.ts
            adopted += 1
        await self.log(
            f"ChaseOrder adopted {adopted} resting order(s) "
            f"cid={self._open_cid} at {_fmt(self._open_price)}, "
            f"filled={_fmt(self._filled_qty())}"
        )

    async def on_recon_done(self, msg: ReconDone) -> None:
        """TD's ledger is real now: start chasing, and start the clock."""
        if msg.api_id != self._primary_api_id():
            return
        if self._restoring and not self._armed:
            # Restored, so the clock and the anchor are already set and must
            # not be reset. What recon adds is what is actually resting.
            self._armed = True
            await self._adopt(msg)
            self._arm_timer()
            await self.log(
                f"ChaseOrder resumed by recon api_id={msg.api_id} — "
                f"{self._elapsed_s()}s already spent"
            )
            return
        if self._armed:
            # Recon runs again after a venue reconnect. The chase is already
            # under way by then, and re-anchoring would hand it a fresh
            # expiry budget it did not earn.
            return
        self._armed = True
        self._started_ms = self.timer.now_ms()
        # Written now, not on the way out: a process killed outright runs no
        # shutdown code, and this is the number a rebuilt chase cannot work
        # out for itself — the expiry budget it has already spent.
        await self.remember(_FACT_STARTED_MS, str(self._started_ms))
        self._arm_timer()
        await self.log(
            f"ChaseOrder armed by recon api_id={msg.api_id} — "
            f"expiry_s={_fmt(self.paras['expiry_s'])} starts now"
        )

    async def _on_recon_timeout(self) -> None:
        if self._armed or self._done:
            return
        self._done = True
        await self.log(
            f"ChaseOrder never saw TD recon within "
            f"{_fmt(self.paras['expiry_s'])}s — exiting without placing",
            level="error",
        )
        self.fail("chase_no_recon")

    async def on_stop(self) -> None:
        self._cancel_timer()
        # Whatever the reason for stopping, an order of ours must not outlive
        # the session that can no longer manage it.
        api_id = self._primary_api_id()
        if api_id is not None and self._open_cid is not None:
            await self._cancel_open(api_id)
        await self.log("ChaseOrder stopped")

    async def on_pause(self) -> None:
        await super().on_pause()
        self._cancel_timer()
        await self.log(
            "ChaseOrder paused — the resting order stays, but stops being "
            "repriced"
        )

    async def on_resume(self) -> None:
        await super().on_resume()
        if self._tick_token is None:
            self._tick_token = self.timer.token()
        # Only the chase resumes. Before recon there is no ledger to check an
        # order against, so re-arming here would place the first one blind.
        if self._armed:
            self._arm_timer()
        await self.log("ChaseOrder resumed")

    # --- market data -------------------------------------------------------

    async def on_best_quote(self, quote: BestQuote) -> None:
        price = self._reference(quote)
        if price is None or price <= 0:
            return
        self._ref = price
        self._quotes += 1

        if self._ref_start is None:
            self._ref_start = price
            # The anchor `_slippage_bps` measures against. Nothing outside
            # this process ever sees it, so without keeping it a rebuilt chase
            # would re-anchor on wherever the market is now and forget how far
            # it has already run — silently widening its own guard.
            await self.remember(_FACT_REF_START, _fmt(price))
            await self.log(
                f"ChaseOrder armed at {_fmt(price)} — "
                f"target {_fmt(self._target_price())}"
            )
            return

        if self._quotes % LOG_EVERY == 0:
            await self.log(
                f"ChaseOrder quoting ref={_fmt(price)} "
                f"target={_fmt(self._target_price())} "
                f"resting={_fmt(self._open_price)} "
                f"slip={_bps(self._slippage_bps())}bps "
                f"filled={_fmt(self._filled_qty())} quotes={self._quotes}"
            )

    def _reference(self, quote: BestQuote) -> Decimal | None:
        """The side we would have to cross — a buy reads the ask, a sell the bid."""
        if self.paras["side"] is Side.BUY:
            return quote.ask
        return quote.bid

    def _target_price(self) -> Decimal | None:
        """Where the order belongs now: ``gap_bps`` inside the far touch."""
        if self._ref is None:
            return None
        gap = self.paras["gap_bps"] / BPS
        if self.paras["side"] is Side.BUY:
            return self._ref * (1 - gap)
        return self._ref * (1 + gap)

    def _slippage_bps(self) -> Decimal | None:
        """How far the reference has run against us since arming, in bps."""
        if self._ref is None or self._ref_start is None or self._ref_start <= 0:
            return None
        move = (self._ref - self._ref_start) / self._ref_start * BPS
        # A buy is hurt by the ask rising, a sell by the bid falling.
        return move if self.paras["side"] is Side.BUY else -move

    # --- execution ---------------------------------------------------------

    async def _on_tick(self) -> None:
        if self._done or not self._armed:
            # Belt and braces: the tick timer is only armed by recon. But the
            # balance check every order goes through reads TD's ledger, and
            # before recon that ledger reads empty — which would look exactly
            # like having no money.
            return
        api_id = self._primary_api_id()
        if api_id is None:
            await self.log("ChaseOrder has no TD api_id — exiting", level="warn")
            self.fail("chase_no_td")
            return

        # Checked before the quote guard: a feed that never arrives must still
        # end the session rather than tick forever.
        if self._expired():
            await self.log(
                f"ChaseOrder expired after {_fmt(self.paras['expiry_s'])}s "
                f"filled={_fmt(self._filled_qty())}"
            )
            await self._finish(api_id, "chase_expired")
            return

        if self._ref is None:
            if self._starved_ticks % LOG_EVERY == 0:
                await self.log(
                    "ChaseOrder tick skipped — no quote yet "
                    f"(ticks={self._starved_ticks + 1})"
                )
            self._starved_ticks += 1
            return

        slip = self._slippage_bps()
        if slip is not None and slip > self.paras["extreme_bps"]:
            await self.log(
                f"ChaseOrder slipped {_bps(slip)}bps past "
                f"{_fmt(self.paras['extreme_bps'])}bps "
                f"(ref {_fmt(self._ref_start)} → {_fmt(self._ref)}) "
                f"filled={_fmt(self._filled_qty())}"
            )
            await self._finish(api_id, "chase_slipped")
            return

        info = await self._instrument()
        if info is None:
            self.fail("chase_no_instrument")
            return
        if self._target_qty is None and not await self._set_target(info):
            self.fail("chase_unsizeable")
            return

        if self._remaining() <= 0:
            await self._complete()
            return

        if self._canceling:
            # The venue has not confirmed the cancel yet. Placing now risks two
            # live orders for the same remaining size.
            return

        target = self._target_price()
        assert target is not None
        if self._open_cid is None:
            await self._place(api_id, info, target)
            return
        if self._drifted(target):
            await self.log(
                f"ChaseOrder reprice {_fmt(self._open_price)} → {_fmt(target)} "
                f"cid={self._open_cid}"
            )
            await self._cancel_open(api_id)

    def _drifted(self, target: Decimal) -> bool:
        """Whether the resting price is more than ``gap_bps`` off the target."""
        if self._open_price is None or target <= 0:
            return False
        drift = abs(target - self._open_price) / target * BPS
        return drift > self.paras["gap_bps"]

    async def _shortfall(
        self, api_id: int, info: SymbolInfo, qty: Decimal, price: Decimal
    ) -> str | None:
        """Why the ledger cannot fund this order, or None if it can.

        Mirrors what TD pre-locks (``reservation_for``): a sell commits base,
        a buy commits ``qty * price`` of quote. Asking here rather than
        letting TD refuse is what turns "not enough money" into one answer
        instead of one per tick — and ``available`` already nets off the
        pre-locks of orders this session has in flight.
        """
        if self.paras["side"] is Side.SELL:
            asset, need = info.base, qty
        else:
            asset, need = info.quote, qty * price
        have = await self.ledger.available(asset, api_id)
        if have >= need:
            return None
        return f"need {_fmt(need)} {asset}, available {_fmt(have)}"

    async def _place(
        self, api_id: int, info: SymbolInfo, raw_price: Decimal
    ) -> None:
        price = info.round_price(raw_price)
        if price <= 0:
            return
        qty = info.round_qty(self._remaining())
        if qty <= 0 or not info.meets_minimums(qty, price):
            # The remainder is too small for the venue to accept. Nothing more
            # can be done passively, so stop rather than tick against a wall.
            await self.log(
                f"ChaseOrder remainder {_fmt(qty)} @ {_fmt(price)} is below "
                f"venue minimums — finishing",
                level="warn",
            )
            await self._finish(api_id, "chase_dust")
            return

        short = await self._shortfall(api_id, info, qty, price)
        if short is not None:
            await self.log(
                f"ChaseOrder cannot fund {_fmt(qty)} @ {_fmt(price)}: "
                f"{short} — exiting",
                level="error",
            )
            self._done = True
            self._cancel_timer()
            self.fail("chase_insufficient_balance")
            return

        accepted = await self.oms.submit_order(
            api_id,
            ticker=info.ticker,
            side=self.paras["side"],
            qty=qty,
            type=OrderType.LIMIT,
            price=price,
            tif=TimeInForce.POST_ONLY,
        )
        cid = self.oms.last_client_order_id
        if not accepted:
            # TD never sent it, so there is nothing resting to cancel — and
            # nothing to wait for either. Its refusals are standing conditions
            # (no funds, no TD, not attached), so the next tick would be
            # refused identically: stop instead of retrying until expiry.
            reason = self.oms.last_reject_reason
            code = self.oms.last_reject_code
            await self.log(
                f"ChaseOrder place refused by TD cid={cid} "
                f"[{describe(code)}]: {reason or 'no reason given'} — exiting",
                level="error",
            )
            self._done = True
            self._cancel_timer()
            self.fail(_refusal_reason(code, reason))
            return
        self._open_cid = cid
        self._open_price = price
        await self.log(
            f"ChaseOrder POST {self.paras['side'].value.upper()} {info.symbol} "
            f"{_fmt(price)}@{_fmt(qty)} cid={cid}"
        )

    async def _cancel_open(self, api_id: int) -> None:
        cid = self._open_cid
        if cid is None:
            return
        # Set before the await: a fill arriving mid-cancel must not find the
        # strategy thinking it is free to place a replacement.
        self._canceling = True
        try:
            if not await self.oms.cancel_order(api_id, cid):
                await self.log(
                    f"ChaseOrder cancel not accepted by TD cid={cid} "
                    f"[{describe(self.oms.last_reject_code)}]: "
                    f"{self.oms.last_reject_reason or 'no reason given'}",
                    level="warn",
                )
                self._clear_open(cid)
        except Exception:
            await self.log(f"ChaseOrder cancel failed cid={cid}", level="warn")
            self._clear_open(cid)

    def _clear_open(self, cid: str | None = None) -> None:
        """Forget the resting order — it is terminal, or was never placed."""
        if cid is not None and cid != self._open_cid:
            return
        self._open_cid = None
        self._open_price = None
        self._canceling = False

    # --- endings -----------------------------------------------------------

    async def _complete(self) -> None:
        """The whole size traded. The ordinary ending."""
        if self._done:
            return
        self._done = True
        self._cancel_timer()
        await self.log(
            f"ChaseOrder filled {_fmt(self._filled_qty())} — exiting"
        )
        self.exit("chase_filled")

    async def _finish(self, api_id: int, reason: str) -> None:
        """Cut the chase short, honouring ``must_exec`` on the way out."""
        if self._done:
            return
        self._done = True
        self._cancel_timer()

        if self._open_cid is not None:
            await self._cancel_open(api_id)
            # Wait for the venue to confirm before sizing the remainder: an
            # order cancelled but still live can fill, and a market order sized
            # against a stale remaining would overshoot the target.
            await self._await_cancel()

        remaining = self._remaining()
        if remaining <= 0:
            await self.log(f"ChaseOrder {reason} — already complete")
            self.exit(reason)
            return
        if not self.paras["must_exec"]:
            await self.log(
                f"ChaseOrder {reason} with {_fmt(remaining)} unfilled — "
                f"must_exec is false, leaving it undone"
            )
            self.exit(reason)
            return
        await self._sweep(api_id, reason)
        self.exit(reason)

    async def _await_terminal(self, seconds: float) -> bool:
        """Wait for the tracked order to finish. True if it reached terminal.

        Bounded by poll count rather than by ``timer.now_ms()``: the strategy
        clock is injectable and may not advance at all, while the sleep between
        polls is always real time. Mixing the two would wait forever.
        """
        for _ in range(int(seconds / CANCEL_POLL_S)):
            if self._open_cid is None:
                return True
            await asyncio.sleep(CANCEL_POLL_S)
        return self._open_cid is None

    async def _await_cancel(self) -> None:
        if await self._await_terminal(CANCEL_WAIT_S):
            return
        await self.log(
            f"ChaseOrder cancel unconfirmed after {CANCEL_WAIT_S}s "
            f"cid={self._open_cid} — sizing the remainder anyway",
            level="warn",
        )

    async def _sweep(self, api_id: int, reason: str) -> None:
        """Take the rest in IOC slices at the touch. Where we cross, on purpose.

        Each slice prices at the far touch, so it takes what is resting there
        and no deeper; whatever is left waits for the book to refill. The loop
        re-reads the reference price every pass, because quotes keep arriving
        on their own task while this one sleeps.
        """
        info = await self._instrument()
        if info is None:
            await self.log(
                f"ChaseOrder must_exec cannot resolve the instrument — "
                f"{_fmt(self._remaining())} left unfilled",
                level="error",
            )
            return

        for slice_no in range(1, IOC_MAX_SLICES + 1):
            remaining = self._remaining()
            if remaining <= 0:
                await self.log(
                    f"ChaseOrder must_exec complete in {slice_no - 1} slices"
                )
                return

            price = self._ref
            if price is None or price <= 0:
                await self.log(
                    "ChaseOrder must_exec has no quote to cross — "
                    f"{_fmt(remaining)} left unfilled",
                    level="error",
                )
                return

            qty = info.round_qty(remaining)
            # Priced at the far touch, which crosses: a buy pays the ask, a
            # sell hits the bid. IOC keeps what fills and drops the rest.
            limit = info.round_price(price)
            if qty <= 0 or not info.meets_minimums(qty, limit):
                await self.log(
                    f"ChaseOrder must_exec remainder {_fmt(qty)} @ "
                    f"{_fmt(limit)} is below venue minimums; "
                    f"{_fmt(remaining)} left unfilled",
                    level="error",
                )
                return

            short = await self._shortfall(api_id, info, qty, limit)
            if short is not None:
                await self.log(
                    f"ChaseOrder must_exec cannot fund slice {slice_no}: "
                    f"{short}; {_fmt(remaining)} left unfilled",
                    level="error",
                )
                return

            accepted = await self.oms.submit_order(
                api_id,
                ticker=info.ticker,
                side=self.paras["side"],
                qty=qty,
                type=OrderType.LIMIT,
                price=limit,
                tif=TimeInForce.IOC,
            )
            cid = self.oms.last_client_order_id
            if not accepted:
                # Same standing conditions as a passive place: the remaining
                # slices would be refused too, so stop rather than spend the
                # rest of the budget being told no.
                await self.log(
                    f"ChaseOrder must_exec slice {slice_no} refused by TD "
                    f"cid={cid} [{describe(self.oms.last_reject_code)}]: "
                    f"{self.oms.last_reject_reason or 'no reason given'} — "
                    f"{_fmt(remaining)} left unfilled",
                    level="error",
                )
                return
            else:
                # Tracked like any other order so the fill bookkeeping and the
                # terminal wait both work unchanged.
                self._open_cid = cid
                self._open_price = limit
                await self.log(
                    f"ChaseOrder must_exec IOC {slice_no}/{IOC_MAX_SLICES} "
                    f"{self.paras['side'].value.upper()} {info.symbol} "
                    f"{_fmt(qty)} @ {_fmt(limit)} cid={cid} ({reason})"
                )
                await self._await_terminal(CANCEL_WAIT_S)
                self._clear_open(cid)

            # Let the book refill before taking again — the whole reason this
            # is sliced rather than one market order.
            await asyncio.sleep(IOC_SLICE_PAUSE_S)

        left = self._remaining()
        if left > 0:
            # must_exec promised this would trade. It did not.
            await self.log(
                f"ChaseOrder must_exec gave up after {IOC_MAX_SLICES} slices "
                f"— {_fmt(left)} left unfilled",
                level="error",
            )

    # --- order events ------------------------------------------------------

    async def on_order_update(self, api_id: int, order: Order) -> None:
        cid = order.client_order_id
        if not self.owns(cid):
            return
        self._filled[str(cid)] = order.filled_qty
        if order.status in _TERMINAL:
            self._clear_open(str(cid))
        if self._target_qty is not None and self._remaining() <= 0:
            await self._complete()

    async def on_fill(self, api_id: int, fill: Fill) -> None:
        if not self.owns(fill.client_order_id):
            return
        await self.log(
            f"ChaseOrder fill cid={fill.client_order_id} "
            f"{_fmt(fill.price)}@{_fmt(fill.qty)} "
            f"total={_fmt(self._filled_qty())}"
        )

    async def on_order_reject(self, api_id: int, reject: OrderReject) -> None:
        if not self.owns(reject.client_order_id):
            return
        # Expected, not exceptional: post-only is refused whenever the price
        # crossed. The next tick reprices off a fresher quote — which is also
        # the right move for a reject that is not a crossed post-only, since
        # the tick re-reads the book and the balance before it tries again.
        crossed = reject.error_code == RejectCode.VENUE_POST_ONLY_WOULD_CROSS
        await self.log(
            f"ChaseOrder {'post-only' if crossed else 'order'} refused "
            f"cid={reject.client_order_id} "
            f"[{describe(reject.error_code)}] {reject.reason} — repricing",
            level="info" if crossed else "warn",
        )
        self._clear_open(str(reject.client_order_id))

    async def on_cancel_reject(
        self, api_id: int, reject: CancelReject
    ) -> None:
        if not self.owns(reject.client_order_id):
            return
        # The order was already gone — filled or cancelled. Either way it is
        # not ours to wait on any more; the order update carries the truth.
        await self.log(
            f"ChaseOrder cancel refused cid={reject.client_order_id} "
            f"[{describe(reject.error_code)}] {reject.reason}",
            level="warn",
        )
        self._clear_open(str(reject.client_order_id))

    # --- size and progress -------------------------------------------------

    async def _set_target(self, info: SymbolInfo) -> bool:
        """Fix the total size once, from ``qty`` or ``qty_quote``."""
        configured = self.paras.get("qty")
        if configured is not None:
            target = info.round_qty(configured)
        else:
            price = self._target_price()
            if price is None or price <= 0:
                return False
            target = info.qty_for_notional(self.paras["qty_quote"], price)
        if target <= 0:
            await self.log(
                f"ChaseOrder cannot size an order from "
                f"{self._size_description()} on {info.universal_ticker}"
                f"{_floor_hint(info, self._target_price())}",
                level="error",
            )
            return False
        self._target_qty = target
        await self.log(f"ChaseOrder target size {_fmt(target)} {info.symbol}")
        return True

    def _filled_qty(self) -> Decimal:
        """Filled across every order this chase has placed."""
        return sum(self._filled.values(), Decimal("0"))

    def _remaining(self) -> Decimal:
        if self._target_qty is None:
            return Decimal("0")
        return self._target_qty - self._filled_qty()

    def _expired(self) -> bool:
        if self._started_ms is None:
            return False
        elapsed_ms = self.timer.now_ms() - self._started_ms
        return elapsed_ms >= int(self.paras["expiry_s"] * 1000)

    def _size_description(self) -> str:
        if self.paras.get("qty") is not None:
            return f"qty={_fmt(self.paras['qty'])}"
        return f"qty_quote={_fmt(self.paras['qty_quote'])}"

    # --- plumbing ----------------------------------------------------------

    async def _instrument(self) -> SymbolInfo | None:
        """Instrument metadata, fetched once and cached for the session."""
        if self._info is not None:
            return self._info
        if self._ticker is None:
            await self.log(
                "ChaseOrder has no md feed to derive an instrument from",
                level="warn",
            )
            return None
        try:
            self._info = await self.symbols.get(self._ticker)
        except Exception as exc:
            await self.log(
                f"ChaseOrder cannot resolve {self._ticker}: {exc}",
                level="error",
            )
            return None
        return self._info

    def _resolve_feed(self) -> None:
        """Venue and symbol come from the md feed key in strategy.yml."""
        md_ids = list(self.session.md_ids) if self.session is not None else []
        if not md_ids:
            return
        try:
            _topic, self._ticker = Topics.parse_md_feed(md_ids[0])
        except ValueError:
            return

    def _primary_api_id(self) -> int | None:
        if self.session is None or not self.session.td_api_ids:
            return None
        return self.session.td_api_ids[0]

    def _arm_timer(self) -> None:
        if self._tick_token is None:
            return
        interval = int(self.paras["refresh_interval_ms"])
        self._tick_token.register(
            self.timer.now_ms() + interval, interval, self._on_tick
        )

    def _cancel_timer(self) -> None:
        if self._tick_token is not None:
            self._tick_token.cancel()
