"""One-cancel-other — rest two orders, keep whichever fills, drop the other.

Two legs, each with its own ``side``, ``price`` and ``qty``. Both go to the
venue together; the first one to fill completely wins, and the loser is
cancelled on the spot. Then the session ends. There is no repricing, no
expiry and no second attempt: an OCO states a pair of prices it is willing to
trade at and waits.

**The single quote.** The strategy subscribes to ``bestquote`` but reads
exactly one message from it — the first usable quote, which is what the pair
is checked against. Later quotes are dropped without being looked at. That is
deliberate: an OCO is not chasing the market, and a check that kept updating
would turn a pair that was legal when it was placed into one that is not,
with two live orders already resting on it.

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

from mft.exchange.models import (
    BestQuote,
    Fill,
    Order,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
)
from mft.protocol import (
    CancelReject,
    OrderReject,
    ReconDone,
    SymbolInfo,
    Topics,
)
from mft.protocol.reject_codes import describe

from mft_sts.strategy import Strategy
from mft_sts.timer import TimerToken

#: How long to wait for the two things that arm the pair — TD recon and the
#: first quote. Past this nothing has been placed and nothing will be, so the
#: session ends rather than sitting on a feed that is not coming.
DEFAULT_ARM_TIMEOUT_S = Decimal("30")

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
        self._venue: str | None = None
        self._symbol: str | None = None
        self._info: SymbolInfo | None = None
        #: The one quote the pair is judged against. Set once; later quotes
        #: are dropped without being read.
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

        timeout = Decimal(str(out.get("arm_timeout_s", DEFAULT_ARM_TIMEOUT_S)))
        if timeout <= 0:
            raise ValueError(f"arm_timeout_s must be positive, got {timeout}")
        out["arm_timeout_s"] = timeout
        return out

    # --- lifecycle ---------------------------------------------------------

    async def on_start(self) -> None:
        self._resolve_feed()
        first, second = self.paras["orders"]
        await self.log(
            f"OneCancelOther started venue={self._venue} "
            f"symbol={self._symbol} A=[{first}] B=[{second}] "
            f"arm_timeout_s={_fmt(self.paras['arm_timeout_s'])}"
        )
        # A one-shot deadline, not a tick: the pair is placed once, by
        # whichever of recon and the first quote lands second. If one of them
        # never does, this is what ends the session instead of hanging it.
        self._arm_token = self.timer.token()
        self._arm_token.register(
            self.timer.now_ms() + int(self.paras["arm_timeout_s"] * 1000),
            0,
            self._on_arm_timeout,
        )

    async def on_ready(self) -> None:
        await self.log(
            "OneCancelOther ready — waiting for TD recon and the first quote"
        )

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
        if msg.api_id != self._primary_api_id():
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
            await self._maybe_place()
            return
        if self._armed:
            # Recon runs again after a venue reconnect. By then the pair is
            # placed, and re-running placement would double it.
            return
        self._armed = True
        await self.log(f"OneCancelOther armed by recon api_id={msg.api_id}")
        await self._maybe_place()

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
        api_id = self._primary_api_id()
        if api_id is not None:
            for cid in list(self._open):
                await self._cancel(api_id, cid, "session stopping")
        await self.log("OneCancelOther stopped")

    async def on_pause(self) -> None:
        await super().on_pause()
        if self._placed:
            # The legs stay, and so does the cancel-the-other rule: leaving
            # both live through a pause is the one outcome an OCO must not
            # have. Only placing something new is held back.
            await self.log(
                "OneCancelOther paused — both legs stay resting, and a fill "
                "still cancels the other"
            )
            return
        await self.log("OneCancelOther paused before placing")

    async def on_resume(self) -> None:
        await super().on_resume()
        await self.log("OneCancelOther resumed")
        await self._maybe_place()

    # --- market data -------------------------------------------------------

    async def on_best_quote(self, quote: BestQuote) -> None:
        """Keep the first usable quote and ignore every one after it."""
        if self._quote is not None:
            return
        if quote.bid <= 0 or quote.ask <= 0:
            # A one-sided or empty book says nothing about whether a leg can
            # rest. Wait for a real one rather than judging the pair on it.
            return
        self._quote = quote
        await self.log(
            f"OneCancelOther took its reference quote "
            f"bid={_fmt(quote.bid)} ask={_fmt(quote.ask)} — "
            f"later quotes are ignored"
        )
        await self._maybe_place()

    # --- placement ---------------------------------------------------------

    async def _maybe_place(self) -> None:
        """Place the pair once recon, a quote and a running session all hold."""
        if self._done or self._placed or self.paused:
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
        api_id = self._primary_api_id()
        if api_id is None:
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
            symbol=info.symbol,
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
        api_id = self._primary_api_id()
        if api_id is not None:
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
        if self._venue is None or self._symbol is None:
            await self.log(
                "OneCancelOther has no md feed to derive an instrument from",
                level="error",
            )
            return None
        try:
            self._info = await self.symbols.get(self._venue, self._symbol)
        except Exception as exc:
            await self.log(
                f"OneCancelOther cannot resolve {self._venue}/{self._symbol}: "
                f"{exc}",
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
            venue, _topic, symbol = Topics.parse_md_feed(md_ids[0])
        except ValueError:
            return
        self._venue = venue
        self._symbol = symbol

    def _primary_api_id(self) -> int | None:
        if self.session is None or not self.session.td_api_ids:
            return None
        return self.session.td_api_ids[0]

    def _cancel_timer(self) -> None:
        if self._arm_token is not None:
            self._arm_token.cancel()
