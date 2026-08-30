"""One-cancel-other — rest two orders, keep whichever fills, drop the other.

Two legs, each with its own ``side``, ``price`` and ``qty``. Both go to the
venue together; the first one to fill completely wins, and the loser is
cancelled on the spot. Then the session ends. There is no repricing, no
expiry and no second attempt: an OCO states a pair of prices it is willing to
trade at and waits.

**The single quote.** The pair is checked against one quote, asked for once —
``self.mds.fetch_best_quote`` — rather than taken from a feed. An OCO is not
chasing the market: it needs the touch at one moment and never again, and a
check that kept updating would turn a pair that was legal when it was placed
into one that is not, with two live orders already resting on it.

It is asked for *after* recon arms, which is as late as it can be had and
still be before the orders go out. Subscribing to ``bestquote`` for this
bought a feed for the life of the session, read its first message, dropped
every one after it — and that first message was the oldest quote available
rather than the newest. This strategy needs no market-data subscription at
all.

**What makes a pair illegal.** A leg must be able to rest. A BUY at or above
the ask, or a SELL at or below the bid, would execute the moment it arrived —
and a pair where either leg trades on arrival is not a choice between two
prices, it is one order plus a decoration. So: either leg marketable against
that first quote, either leg below the venue's minimums, or a price that
rounds away to nothing, and the whole thing is refused before anything is
sent. Nothing is placed and the session exits.

Both legs may sit on the same side — two entries at different prices, taking
whichever the market reaches first, is a normal use. What is checked is that
neither can trade immediately, not which way they point.

**Configuring it.** ``ticker`` in ``st_paras`` names the instrument, as one
universal ticker — ``Gate_Spot_ETHUSDT``. Leaving it out falls back to the
first key in ``md_ids``, which is how this was configured when the quote came
from a feed — but no subscription is needed now, and a session running this
can have none at all.

**Post-only, on purpose.** The legality check reads a quote from a moment
before the orders go out. Sending them post-only is what makes that check
hold: if the book moved in between and a leg would now take, the venue
refuses it instead of quietly doing the thing the check existed to prevent. A
refused leg ends the pair — see below.

**How it ends.**

* A leg fills completely — the other is cancelled, and the session exits
  ``oco_filled``. A partial fill does not decide anything; both legs stay.
* A leg is lost — rejected by the venue, or cancelled by someone else. The
  pair no longer offers a choice, so the survivor is cancelled too and the
  session exits rather than leaving one naked order resting.
* Nothing arrives to arm it within ``arm_timeout_s`` — no TD recon, or no
  quote on the feed — and it exits without placing.

The one case this cannot rule out: both legs filling in the same instant, on
opposite sides of a market that jumped through both prices. The cancel goes
out as soon as the first fill is seen, and there is nothing faster available
from here.
"""

from __future__ import annotations

from dataclasses import dataclass
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
    MdBestQuoteResult,
    OrderReject,
    ReconDone,
    SymbolInfo,
    Topics,
)
from mftik.protocol.reject_codes import describe
from mftik.strategy import Strategy
from mftik.strategy.timer import TimerToken

#: How long to wait for the two things that arm the pair — TD recon and the
#: first quote. Past this nothing has been placed and nothing will be, so the
#: session ends rather than sitting on a feed that is not coming.
DEFAULT_ARM_TIMEOUT_S = Decimal("30")

#: How long to wait before re-asking when the book had nothing on a side. Short
#: because the arm timeout is what really bounds this, and a pair waiting on a
#: quote is a session doing nothing.
QUOTE_RETRY_MS = 500

#: An OCO has exactly two legs. One is an order; three is not an OCO.
LEG_COUNT = 2

#: Statuses that mean the venue is finished with an order.
_TERMINAL = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED}
)


def _fmt(value: object) -> str:
    """Compact Decimal for logs — drop trailing zeros from Numeric(38, 18)."""
    if value is None:
        return "?"
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(d.normalize(), "f")


@dataclass(frozen=True)
class Leg:
    """One side of the pair: what to send, and whether it may rest."""

    side: Side
    price: Decimal
    qty: Decimal

    def rounded(self, info: SymbolInfo) -> Leg:
        """This leg snapped to the venue's tick and step.

        Both round down, which moves a SELL toward the bid and a BUY away
        from the ask. Only the SELL can be made marketable by that, and it is
        why the legality check runs on the rounded prices rather than the
        configured ones.
        """
        return Leg(
            side=self.side,
            price=info.round_price(self.price),
            qty=info.round_qty(self.qty),
        )

    def marketable(self, quote: BestQuote) -> bool:
        """Whether this leg would trade on arrival rather than rest."""
        if self.side is Side.BUY:
            return self.price >= quote.ask
        return self.price <= quote.bid

    def commits(self, info: SymbolInfo) -> tuple[str, Decimal]:
        """The asset this leg ties up, and how much — as TD pre-locks it."""
        if self.side is Side.SELL:
            return info.base, self.qty
        return info.quote, self.qty * self.price

    def __str__(self) -> str:
        return f"{self.side.value.upper()} {_fmt(self.qty)} @ {_fmt(self.price)}"


def _leg(raw: Any, index: int) -> Leg:
    """Read one configured leg, saying which one is wrong and why."""
    where = f"orders[{index}]"
    if not isinstance(raw, dict):
        raise TypeError(f"{where} must be a mapping, got {type(raw).__name__}")

    side = str(raw.get("side", "")).strip().lower()
    if side not in (Side.BUY.value, Side.SELL.value):
        raise ValueError(
            f"{where}.side must be {Side.BUY.value!r} or {Side.SELL.value!r}, "
            f"got {raw.get('side')!r}"
        )

    return Leg(
        side=Side(side),
        price=_positive(raw, "price", where),
        qty=_positive(raw, "qty", where),
    )


def _positive(raw: dict[str, Any], name: str, where: str) -> Decimal:
    value = raw.get(name)
    if value is None:
        raise ValueError(f"{where}.{name} is required")
    try:
        out = Decimal(str(value))
    except ArithmeticError as exc:
        raise ValueError(
            f"{where}.{name} must be a number, got {value!r}"
        ) from exc
    if out <= 0:
        raise ValueError(f"{where}.{name} must be positive, got {out}")
    return out


class OneCancelOther(Strategy):
    name = "oco"
    id = 3
    #: Restorable. Nothing has to be remembered: the pair is in ``st_paras``
    #: and what became of it is in recon — see :meth:`on_rebuild`.
    rebuildable = True

    def __init__(self) -> None:
        super().__init__()
        self._arm_token: TimerToken | None = None
        self._quote_token: TimerToken | None = None
        self._ticker: UniversalTicker | None = None
        self._info: SymbolInfo | None = None
        #: The one quote the pair is judged against. Asked for once armed.
        self._quote: BestQuote | None = None
        #: Set once TD recon has landed and its ledger is real.
        self._armed = False
        #: cid → the leg it came from. Both legs land here at placement.
        self._legs: dict[str, Leg] = {}
        #: cids we believe the venue still has resting.
        self._open: set[str] = set()
        #: cid → filled qty, so a complete fill is recognisable.
        self._filled: dict[str, Decimal] = {}
        self._placed = False
        #: Set when this session ran before. Recon then reports the pair this
        #: session left at the venue, not somebody else's orders.
        self._restoring = False
        self._done = False

    # --- parameters --------------------------------------------------------

    @classmethod
    def on_initialized(cls, params: Any) -> dict[str, Any]:
        out = super().on_initialized(params)

        raw = out.get("orders")
        if not isinstance(raw, list):
            raise ValueError(
                f"orders is required and must be a list of {LEG_COUNT} legs, "
                f"each with side / price / qty"
            )
        if len(raw) != LEG_COUNT:
            raise ValueError(
                f"orders must have exactly {LEG_COUNT} legs, got {len(raw)}"
            )
        out["orders"] = [_leg(row, i) for i, row in enumerate(raw)]

        named = out.get("ticker")
        if named:
            # Resolved here rather than at first use: a typo in the instrument
            # should refuse the deployment, not surface once the pair is live
            # and has already been told there is nothing to quote against.
            out["ticker"] = str(UniversalTicker.resolve(str(named)))

        timeout = Decimal(str(out.get("arm_timeout_s", DEFAULT_ARM_TIMEOUT_S)))
        if timeout <= 0:
            raise ValueError(f"arm_timeout_s must be positive, got {timeout}")
        out["arm_timeout_s"] = timeout
        return out

    # --- lifecycle ---------------------------------------------------------

    async def on_start(self) -> None:
        try:
            self.session.td_sole()
        except RuntimeError as exc:
            self.fail(str(exc))
            return
        self._resolve_market()
        first, second = self.paras["orders"]
        await self.log(
            f"OneCancelOther started ticker={self._ticker} "
            f"A=[{first}] B=[{second}] "
            f"arm_timeout_s={_fmt(self.paras['arm_timeout_s'])}"
        )
        # A one-shot deadline, not a tick: recon arms the pair, the quote is
        # asked for once armed, and placement follows. If either never lands,
        # this is what ends the session instead of hanging it.
        self._arm_token = self.timer.token()
        self._arm_token.register(
            self.timer.now_ms() + int(self.paras["arm_timeout_s"] * 1000),
            0,
            self._on_arm_timeout,
        )

    async def on_ready(self) -> None:
        await self.log("OneCancelOther ready — waiting for TD recon")

    async def on_rebuild(self, remembered: dict[str, str]) -> None:
        """This pair ran before. Nothing to take back — recon has all of it.

        ``remembered`` is empty by design. An OCO's whole state is either
        configuration, which ``st_paras`` still holds, or the fate of two
        orders, which only the venue knows: recon reports what is resting,
        what filled and what is gone. There is no third kind of fact here the
        way there is in a chase, whose clock and slippage anchor no order
        carries.

        What this does change is the meaning of what recon brings. Any order
        of ours in it is one *this session* left behind, to be taken back
        rather than watched from the outside.
        """
        self._restoring = True
        await self.log("OneCancelOther restoring — waiting for recon")

    async def _adopt(self, api_id: int, msg: ReconDone) -> bool:
        """Take back the pair this session left at the venue.

        Returns whether the pair is still live and being watched. False means
        nothing of ours survived and placement should run as usual — which is
        the ordinary case, because ``on_stop`` cancels both legs and a
        shutdown that got that far leaves nothing behind.
        """
        info = await self._instrument()
        if info is None:
            # Matching an order back to a leg needs the rounded leg prices,
            # so without the instrument there is no way to tell what is ours.
            # That is not the same as finding nothing: placing again could
            # double a pair still resting at the venue. Stop and say so.
            self._done = True
            await self.log(
                "OneCancelOther cannot be restored: no instrument to match "
                "resting orders against, and placing again might double the "
                "pair",
                level="error",
            )
            self.fail("oco_restore_no_instrument")
            return True
        by_leg = {
            (leg.side, leg.price): leg
            for leg in (raw.rounded(info) for raw in self.paras["orders"])
        }

        for cid, order in msg.oms.orders.items():
            if not self.owns(cid):
                continue
            leg = by_leg.get((order.side, order.price))
            if leg is None:
                # Ours by slot but not one of these legs — an earlier session
                # of the same strategy, or a leg whose config has since
                # changed. Not something this pair can reason about.
                continue
            # Restoring `_legs` is what makes the rest work: every order
            # handler ignores a cid it has no leg for.
            self._legs[cid] = leg
            self._filled[cid] = order.filled_qty
            if order.status not in _TERMINAL:
                self._open.add(cid)

        if not self._legs:
            return False

        self._placed = True
        await self.log(
            f"OneCancelOther adopted {len(self._legs)} leg(s), "
            f"{len(self._open)} still resting"
        )

        # A leg that filled while STS was away has already decided the pair.
        for cid, filled in self._filled.items():
            if filled >= self._legs[cid].qty:
                await self._settle(api_id, cid)
                return True

        if not self._open:
            # Both gone without either filling — cancelled while we were away,
            # or rejected. There is nothing left to cancel and nothing to win.
            await self._abort("oco_legs_lost_while_away")
            return True

        if len(self._open) < LEG_COUNT:
            # A lone survivor is not an OCO, exactly as it is not at runtime.
            await self._abort("oco_leg_lost")
            return True
        return False

    async def on_recon_done(self, msg: ReconDone) -> None:
        """TD's ledger is real now — half of what placement waits for."""
        try:
            sole = self.session.td_sole()
        except RuntimeError as exc:
            self.fail(str(exc))
            return
        if msg.api_id != sole:
            return
        if self._restoring and not self._armed:
            self._armed = True
            if await self._adopt(msg.api_id, msg):
                return
            # Nothing survived, so this is an ordinary placement again: wait
            # for a quote and judge the pair against the market as it is now.
            await self.log(
                "OneCancelOther found nothing of its own resting — "
                "placing again once a quote arrives"
            )
            await self._request_quote()
            return
        if self._armed:
            # Recon runs again after a venue reconnect. By then the pair is
            # placed, and re-running placement would double it.
            return
        self._armed = True
        await self.log(f"OneCancelOther armed by recon api_id={msg.api_id}")
        # Asked for only now: this is the last moment before the orders go
        # out, so it is the freshest the legality check can be.
        await self._request_quote()

    async def _on_arm_timeout(self) -> None:
        if self._placed or self._done:
            return
        self._done = True
        missing = []
        if not self._armed:
            missing.append("TD recon")
        if self._quote is None:
            missing.append("a best quote")
        await self.log(
            f"OneCancelOther saw no {' and no '.join(missing)} within "
            f"{_fmt(self.paras['arm_timeout_s'])}s — exiting without placing",
            level="error",
        )
        self.fail("oco_not_armed")

    async def on_stop(self) -> None:
        self._cancel_timer()
        # Whatever ended the session, neither leg may outlive it: nothing is
        # left watching for the fill that would cancel the other.
        if self.session is not None and len(self.session.td_api_ids) == 1:
            api_id = self.session.td_sole()
            for cid in list(self._open):
                await self._cancel(api_id, cid, "session stopping")
        await self.log("OneCancelOther stopped")

    # --- market data -------------------------------------------------------

    async def _request_quote(self) -> None:
        """Ask for the touch the pair will be judged against."""
        if self._done or self._placed or self._quote is not None:
            return
        if self._ticker is None:
            await self.log(
                "OneCancelOther has no instrument to quote", level="error"
            )
            self._done = True
            self.fail("oco_no_market")
            return
        query_id = await self.mds.fetch_best_quote(self._ticker)
        if query_id is None:
            # The query never left. Nothing will arrive to place against, and
            # the arm timeout would only turn that into a slower failure.
            await self.log(
                f"OneCancelOther could not ask for a quote: "
                f"{self.mds.last_reject_reason}",
                level="error",
            )
            self._done = True
            self.fail("oco_no_quote")

    async def on_fetch_bestquote(self, result: MdBestQuoteResult) -> None:
        """Take the quote the pair is judged against, and place on it."""
        if self._quote is not None or self._done or self._placed:
            return
        quote = result.quote
        if not result.ok or quote is None or quote.bid <= 0 or quote.ask <= 0:
            # A one-sided or empty book says nothing about whether a leg can
            # rest, and a failed read says nothing at all. Ask again rather
            # than judging the pair on it; the arm timeout bounds this.
            await self.log(
                f"OneCancelOther has no usable quote yet "
                f"({result.reason or 'one side empty'}) — asking again"
            )
            self._retry_quote()
            return
        self._quote = quote
        await self.log(
            f"OneCancelOther took its reference quote "
            f"bid={_fmt(quote.bid)} ask={_fmt(quote.ask)}"
        )
        await self._maybe_place()

    def _retry_quote(self) -> None:
        """Re-ask shortly. Bounded by the arm timeout, which still runs."""
        if self._done or self._placed:
            return
        self._quote_token = self.timer.token()
        self._quote_token.register(
            self.timer.now_ms() + QUOTE_RETRY_MS, 0, self._request_quote
        )

    # --- placement ---------------------------------------------------------

    async def _maybe_place(self) -> None:
        """Place the pair once recon, a quote and a running session all hold."""
        if self._done or self._placed:
            return
        if not self._armed or self._quote is None:
            return
        self._placed = True
        try:
            await self._place()
        except Exception as exc:
            # A half-placed pair is the one state with no owner, so unwind
            # whatever did go out before giving up on it.
            await self.log(
                f"OneCancelOther could not place the pair: {exc}", level="error"
            )
            await self._abort("oco_place_failed")

    async def _place(self) -> None:
        try:
            api_id = self.session.td_sole()
        except RuntimeError:
            await self.log("OneCancelOther has no TD api_id", level="error")
            self._done = True
            self.fail("oco_no_td")
            return

        info = await self._instrument()
        if info is None:
            self._done = True
            self.fail("oco_no_instrument")
            return

        legs = [leg.rounded(info) for leg in self.paras["orders"]]
        illegal = self._illegal(legs, info)
        if illegal is not None:
            self._done = True
            if self._restoring:
                # Not the pair's fault. It was legal when it was placed; the
                # market moved through a leg while STS was down, and posting
                # it now would be refused by the venue anyway. Said plainly,
                # because "my OCO failed because you restarted" is a surprise.
                await self.log(
                    f"OneCancelOther cannot be restored: {illegal} — the "
                    f"market moved through the pair while STS was away",
                    level="error",
                )
                self.fail("oco_illegal_on_restart")
                return
            await self.log(
                f"OneCancelOther pair is not placeable: {illegal} — exiting "
                f"without placing anything",
                level="error",
            )
            self.fail("oco_illegal")
            return

        short = await self._shortfall(api_id, info, legs)
        if short is not None:
            await self.log(
                f"OneCancelOther cannot fund both legs: {short} — exiting",
                level="error",
            )
            self._done = True
            self.fail("oco_insufficient_balance")
            return

        self._cancel_timer()
        for leg in legs:
            if not await self._submit(api_id, info, leg):
                # One leg out and the other refused is not an OCO. Pull the
                # survivor rather than leave a single order nobody asked for.
                await self._abort("oco_refused")
                return
        resting = " | ".join(
            f"{cid}=[{leg}]" for cid, leg in self._legs.items()
        )
        await self.log(f"OneCancelOther placed both legs {resting}")

    def _illegal(self, legs: list[Leg], info: SymbolInfo) -> str | None:
        """Why this pair cannot be placed, or None if it can.

        Judged against the one reference quote and the venue's own filters.
        Prices are the rounded ones: what would actually be sent.
        """
        quote = self._quote
        assert quote is not None
        for index, leg in enumerate(legs):
            where = f"orders[{index}] {leg}"
            if leg.price <= 0 or leg.qty <= 0:
                return f"{where} rounds to nothing at this venue's precision"
            if not info.meets_minimums(leg.qty, leg.price):
                return f"{where} is below the venue's minimums"
            if leg.marketable(quote):
                touch = "ask" if leg.side is Side.BUY else "bid"
                against = quote.ask if leg.side is Side.BUY else quote.bid
                return (
                    f"{where} would trade on arrival against {touch} "
                    f"{_fmt(against)}, so it cannot rest"
                )
        return None

    async def _shortfall(
        self, api_id: int, info: SymbolInfo, legs: list[Leg]
    ) -> str | None:
        """Why the ledger cannot fund *both* legs, or None if it can.

        Both, not either: the venue holds margin for each resting order, so a
        pair that can only afford one of them is one leg plus a rejection —
        and finding that out here costs nothing, while finding it out from TD
        costs an order that has to be unwound.
        """
        need: dict[str, Decimal] = {}
        for leg in legs:
            asset, amount = leg.commits(info)
            need[asset] = need.get(asset, Decimal("0")) + amount
        for asset, amount in need.items():
            have = await self.ledger.available(asset, api_id)
            if have < amount:
                return f"need {_fmt(amount)} {asset}, available {_fmt(have)}"
        return None

    async def _submit(
        self, api_id: int, info: SymbolInfo, leg: Leg
    ) -> bool:
        """Send one leg. False means TD did not take it."""
        accepted = await self.oms.submit_order(
            api_id,
            ticker=info.ticker,
            side=leg.side,
            qty=leg.qty,
            type=OrderType.LIMIT,
            price=leg.price,
            # Post-only is what holds the legality check together: if the
            # book moved since the reference quote, the venue refuses rather
            # than filling a leg that was supposed to rest.
            tif=TimeInForce.POST_ONLY,
        )
        cid = self.oms.last_client_order_id
        if not accepted:
            await self.log(
                f"OneCancelOther leg [{leg}] refused by TD cid={cid} "
                f"[{describe(self.oms.last_reject_code)}]: "
                f"{self.oms.last_reject_reason or 'no reason given'}",
                level="error",
            )
            return False
        key = str(cid)
        self._legs[key] = leg
        self._open.add(key)
        await self.log(f"OneCancelOther POST [{leg}] cid={key}")
        return True

    # --- order events ------------------------------------------------------

    async def on_order_update(self, api_id: int, order: Order) -> None:
        cid = str(order.client_order_id)
        if not self.owns(order.client_order_id) or cid not in self._legs:
            return
        self._filled[cid] = order.filled_qty
        if order.status not in _TERMINAL:
            return
        self._open.discard(cid)
        if self._done:
            # Almost always the loser's own cancel coming back. Nothing to
            # decide: this pair is already settled.
            return
        if order.status is OrderStatus.FILLED:
            await self._settle(api_id, cid)
            return
        # Terminal without filling — rejected, or cancelled by someone else.
        # A lone survivor is not an OCO, so it goes too.
        await self.log(
            f"OneCancelOther leg [{self._legs[cid]}] ended "
            f"{order.status.value} without filling cid={cid}",
            level="warn",
        )
        await self._abort("oco_leg_lost")

    async def on_fill(self, api_id: int, fill: Fill) -> None:
        cid = str(fill.client_order_id)
        if not self.owns(fill.client_order_id) or cid not in self._legs:
            return
        # Logged, not acted on. A partial fill leaves both legs live by
        # design; only a complete one decides the pair.
        await self.log(
            f"OneCancelOther fill cid={cid} "
            f"{_fmt(fill.price)}@{_fmt(fill.qty)} "
            f"filled={_fmt(self._filled.get(cid))} of "
            f"{_fmt(self._legs[cid].qty)}"
        )

    async def on_order_reject(self, api_id: int, reject: OrderReject) -> None:
        cid = str(reject.client_order_id)
        if not self.owns(reject.client_order_id) or cid not in self._legs:
            return
        self._open.discard(cid)
        if self._done:
            return
        await self.log(
            f"OneCancelOther leg [{self._legs[cid]}] rejected by the venue "
            f"cid={cid} [{describe(reject.error_code)}] {reject.reason}",
            level="error",
        )
        await self._abort("oco_leg_rejected")

    async def on_cancel_reject(
        self, api_id: int, reject: CancelReject
    ) -> None:
        cid = str(reject.client_order_id)
        if not self.owns(reject.client_order_id) or cid not in self._legs:
            return
        # The order was already gone — filled, or cancelled before us. Either
        # way it is not resting, which is all the cancel wanted.
        self._open.discard(cid)
        await self.log(
            f"OneCancelOther cancel refused cid={cid} "
            f"[{describe(reject.error_code)}] {reject.reason}",
            level="warn",
        )

    # --- endings -----------------------------------------------------------

    async def _settle(self, api_id: int, winner: str) -> None:
        """One leg filled. Cancel the other and end the session."""
        self._done = True
        self._cancel_timer()
        loser = self._other(winner)
        await self.log(
            f"OneCancelOther leg [{self._legs[winner]}] filled cid={winner} — "
            f"cancelling the other"
        )
        if loser is not None:
            await self._cancel(api_id, loser, "the other leg filled")
        # Which leg won is the whole result of an OCO, so it goes on the row
        # rather than only in the log: `oco_filled` alone leaves the reader
        # asking the one question the strategy exists to answer.
        self.exit(f"oco_filled: {self._legs[winner]}")

    async def _abort(self, reason: str) -> None:
        """End the pair without a winner, leaving nothing resting behind.

        Every caller got here because the pair could not be run as asked, so
        this is a failure: the session ends ``failed`` with ``reason``.
        """
        if self._done:
            return
        self._done = True
        self._cancel_timer()
        if self.session is not None and len(self.session.td_api_ids) == 1:
            api_id = self.session.td_sole()
            for cid in list(self._open):
                await self._cancel(api_id, cid, reason)
        self.fail(reason)

    async def _cancel(self, api_id: int, cid: str, why: str) -> None:
        try:
            if not await self.oms.cancel_order(api_id, cid):
                await self.log(
                    f"OneCancelOther cancel not accepted by TD cid={cid} "
                    f"({why}) [{describe(self.oms.last_reject_code)}]: "
                    f"{self.oms.last_reject_reason or 'no reason given'}",
                    level="warn",
                )
                self._open.discard(cid)
        except Exception as exc:
            await self.log(
                f"OneCancelOther cancel failed cid={cid} ({why}): {exc}",
                level="warn",
            )
            self._open.discard(cid)

    def _other(self, cid: str) -> str | None:
        """The leg that is not ``cid``."""
        for other in self._legs:
            if other != cid:
                return other
        return None

    # --- plumbing ----------------------------------------------------------

    async def _instrument(self) -> SymbolInfo | None:
        """Instrument metadata, fetched once and cached for the session."""
        if self._info is not None:
            return self._info
        if self._ticker is None:
            await self.log(
                "OneCancelOther has no md feed to derive an instrument from",
                level="error",
            )
            return None
        try:
            self._info = await self.symbols.get(self._ticker)
        except Exception as exc:
            await self.log(
                f"OneCancelOther cannot resolve {self._ticker}: {exc}",
                level="error",
            )
            return None
        return self._info

    def _resolve_market(self) -> None:
        """Which instrument this pair trades.

        ``ticker`` in ``st_paras`` says it outright — one universal ticker,
        resolved leniently because a person wrote it. A feed key in ``md_ids``
        is still read when it is absent, which is how this was configured
        before the quote came from a query — but a subscription is no longer
        needed for anything here, and naming the market directly is the honest
        way to say so.
        """
        named = self.paras.get("ticker")
        if named:
            try:
                self._ticker = UniversalTicker.resolve(str(named))
                return
            except Exception:
                # Falls through to the feed key rather than failing here; the
                # caller learns about it from ``_instrument`` either way.
                pass
        md_ids = list(self.session.md_ids) if self.session is not None else []
        if not md_ids:
            return
        try:
            _topic, self._ticker = Topics.parse_md_feed(md_ids[0])
        except ValueError:
            return

    def _cancel_timer(self) -> None:
        if self._arm_token is not None:
            self._arm_token.cancel()
        if self._quote_token is not None:
            self._quote_token.cancel()
