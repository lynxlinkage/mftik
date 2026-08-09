"""Cross-venue maker/hedge — quote PostOnly on one account, IOC-hedge on another.

Two TD accounts, two ``bestquote`` feeds. ``td[0]`` + ``quote_ticker`` rest
passively; ``td[1]`` + ``hedge_ticker`` take when a quote order fills.

**Pricing.** Quote prices are anchored on the *hedge* book's touch, not the
quote book's. A SELL posts ``x_mid`` bps above the hedge ask; a BUY posts
``x_mid`` bps below the hedge bid. ``x_mid`` is the midpoint of the configured
``[x_lo_bps, x_hi_bps]`` band — the edge the strategy is willing to capture,
ignoring fees. Both quote legs are ``POST_ONLY``.

**Repricing.** On each hedge-quote update the resting edge of every open leg
is recomputed against the new touch. Outside ``[x_lo, x_hi]`` the leg is
cancelled; the replacement waits for the cancel to go terminal, then posts at
the fresh ``x_mid`` price on a later quote.

**Hedging.** The first ``PARTIALLY_FILLED`` or ``FILLED`` on a quote
``client_order_id`` fires one IOC for the *full configured* ``qty`` on the
hedge account — not the incremental fill. Partials can be dust that a venue
would refuse to take, and this strategy assumes a partial is always followed
by a fill of the rest. Later updates for the same cid are logged only, so a
second fill cannot double-hedge. Quote SELL → hedge BUY at ask + ``2*x_hi``;
quote BUY → hedge SELL at bid − ``2*x_hi``. That buffer is the slippage pad.

**Sides.** ``side: [buy]``, ``[sell]``, or ``[buy, sell]``. On a fill of one
leg the other resting quote (if any) is cancelled; the next best-quote push
that finds nothing open re-arms every configured side.

**Balances.** Before a quote goes out, both books are checked: the quote
account must fund the PostOnly, and the hedge account must fund the IOC that
would answer a fill of that leg. Either shortfall skips the leg — it does not
fail the session. A hedge IOC that still cannot fund at fill time is logged
and not retried.

**Pause / resume.** Pause cancels every resting quote and stops placing; MD
keeps updating the hedge touch so resume can re-arm immediately. Resume is
just ``_maintain_quotes`` again. A fill that races a pause cancel is still
hedged — inventory risk does not wait for resume.

**Rebuild.** Equivalent to a fresh start with the same config and cid slot:
no facts are restored, leftover owned quote orders from recon are cancelled,
and quoting begins again once both ledgers are live. Missed fills while STS
was away are not hunted — same as a redeploy.
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
from mft.exchange.tickers import UniversalTicker
from mft.protocol import (
    CancelReject,
    OrderReject,
    ReconDone,
    SymbolInfo,
)
from mft.protocol.reject_codes import describe

from mft_sts.strategy import Strategy

BPS = Decimal("10000")

_TERMINAL = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED}
)

_FILL_STATUSES = frozenset(
    {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}
)

LOG_EVERY = 20


def _fmt(value: object) -> str:
    if value is None:
        return "?"
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(d.normalize(), "f")


def _positive(paras: dict[str, Any], name: str) -> Decimal:
    raw = paras.get(name)
    if raw is None:
        raise ValueError(f"{name} is required")
    try:
        out = Decimal(str(raw))
    except ArithmeticError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if out <= 0:
        raise ValueError(f"{name} must be positive, got {out}")
    return out


def x_mid_bps(x_lo: Decimal, x_hi: Decimal) -> Decimal:
    return (x_lo + x_hi) / 2


def quote_raw_price(side: Side, hedge: BestQuote, mid_bps: Decimal) -> Decimal:
    """PostOnly price on the quote venue from the hedge touch."""
    frac = mid_bps / BPS
    if side is Side.SELL:
        return hedge.ask * (1 + frac)
    return hedge.bid * (1 - frac)


def hedge_raw_price(side: Side, hedge: BestQuote, x_hi: Decimal) -> Decimal:
    """IOC limit on the hedge venue — opposite the filled quote side."""
    pad = (2 * x_hi) / BPS
    if side is Side.BUY:
        # Quote sold → buy the hedge book through the ask.
        return hedge.ask * (1 + pad)
    return hedge.bid * (1 - pad)


def edge_bps(side: Side, price: Decimal, hedge: BestQuote) -> Decimal | None:
    """Implied edge of a resting quote vs the current hedge touch, in bps."""
    if side is Side.SELL:
        if hedge.ask <= 0:
            return None
        return (price / hedge.ask - 1) * BPS
    if hedge.bid <= 0:
        return None
    return (1 - price / hedge.bid) * BPS


def edge_in_band(
    side: Side,
    price: Decimal,
    hedge: BestQuote,
    x_lo: Decimal,
    x_hi: Decimal,
) -> bool:
    edge = edge_bps(side, price, hedge)
    if edge is None:
        return False
    return x_lo <= edge <= x_hi


@dataclass
class _OpenLeg:
    """One resting quote order we still believe the venue has."""

    cid: str
    side: Side
    price: Decimal
    canceling: bool = False


class CrossArb(Strategy):
    name = "cross_arb"
    id = 4
    #: Restorable as a clean restart — see :meth:`on_rebuild`.
    rebuildable = True

    def __init__(self) -> None:
        super().__init__()
        self._quote_ticker: UniversalTicker | None = None
        self._hedge_ticker: UniversalTicker | None = None
        self._quote_info: SymbolInfo | None = None
        self._hedge_info: SymbolInfo | None = None
        self._hedge_quote: BestQuote | None = None
        #: api_ids that have delivered recon — need both before placing.
        self._recon: set[int] = set()
        self._armed = False
        #: True between :meth:`on_rebuild` and the first full arm.
        self._restoring = False
        #: side → resting quote leg
        self._open: dict[Side, _OpenLeg] = {}
        #: quote cids already handed a hedge IOC
        self._hedged: set[str] = set()
        #: cid → last filled_qty seen, for log-only reconciliation
        self._filled: dict[str, Decimal] = {}
        self._quotes = 0
        self._stopping = False

    # --- parameters --------------------------------------------------------

    @classmethod
    def on_initialized(cls, params: Any) -> dict[str, Any]:
        out = super().on_initialized(params)

        out["quote_ticker"] = str(
            UniversalTicker.resolve(str(out.get("quote_ticker", "")).strip())
        )
        out["hedge_ticker"] = str(
            UniversalTicker.resolve(str(out.get("hedge_ticker", "")).strip())
        )
        if out["quote_ticker"] == out["hedge_ticker"]:
            raise ValueError(
                "quote_ticker and hedge_ticker must name different instruments"
            )

        raw_sides = out.get("side")
        if not isinstance(raw_sides, list) or not raw_sides:
            raise ValueError(
                "side must be a non-empty list of 'buy' and/or 'sell', "
                f"got {raw_sides!r}"
            )
        sides: list[Side] = []
        seen: set[Side] = set()
        for item in raw_sides:
            value = str(item).strip().lower()
            if value not in (Side.BUY.value, Side.SELL.value):
                raise ValueError(
                    f"side entries must be {Side.BUY.value!r} or "
                    f"{Side.SELL.value!r}, got {item!r}"
                )
            side = Side(value)
            if side in seen:
                continue
            seen.add(side)
            sides.append(side)
        out["side"] = sides

        qty = _positive(out, "qty")
        out["qty"] = qty

        x_lo = _positive(out, "x_lo_bps")
        x_hi = _positive(out, "x_hi_bps")
        if x_lo > x_hi:
            raise ValueError(
                f"x_lo_bps ({x_lo}) must be <= x_hi_bps ({x_hi})"
            )
        out["x_lo_bps"] = x_lo
        out["x_hi_bps"] = x_hi
        out["x_mid_bps"] = x_mid_bps(x_lo, x_hi)
        return out

    # --- lifecycle ---------------------------------------------------------

    async def on_start(self) -> None:
        self._quote_ticker = UniversalTicker.parse(self.paras["quote_ticker"])
        self._hedge_ticker = UniversalTicker.parse(self.paras["hedge_ticker"])
        if self.session is None or len(self.session.td_api_ids) < 2:
            await self.log(
                "CrossArb needs two TD accounts (quote then hedge) — exiting",
                level="error",
            )
            self.fail("cross_arb_need_two_td")
            return
        sides = ",".join(s.value for s in self.paras["side"])
        await self.log(
            f"CrossArb started quote={self._quote_ticker} "
            f"hedge={self._hedge_ticker} side=[{sides}] "
            f"qty={_fmt(self.paras['qty'])} "
            f"x_lo={_fmt(self.paras['x_lo_bps'])} "
            f"x_hi={_fmt(self.paras['x_hi_bps'])} "
            f"x_mid={_fmt(self.paras['x_mid_bps'])} "
            f"td_quote={self._quote_api_id()} td_hedge={self._hedge_api_id()}"
        )

    async def on_ready(self) -> None:
        await self.log("CrossArb ready — waiting for both TD recons to arm")

    async def on_rebuild(self, remembered: dict[str, str]) -> None:
        """STS restarted this session. Treat it as a clean start.

        Nothing in ``remembered`` is needed — config lives in ``st_paras`` and
        quoting state is rebuilt from the next hedge touch. Leftover quote
        orders are cancelled when quote-account recon lands.
        """
        self._restoring = True
        self._open.clear()
        self._hedged.clear()
        self._filled.clear()
        self._hedge_quote = None
        self._recon.clear()
        self._armed = False
        await self.log(
            "CrossArb restoring as a restart — will cancel leftovers on "
            "recon, then quote again"
        )

    async def on_pause(self) -> None:
        await super().on_pause()
        api_id = self._quote_api_id()
        if api_id is not None:
            for side in list(self._open):
                await self._cancel_leg(api_id, side)
        await self.log(
            "CrossArb paused — quotes cancelled; still listening to bestquote"
        )

    async def on_resume(self) -> None:
        await super().on_resume()
        await self.log("CrossArb resumed — re-arming quotes")
        if self._armed and self._hedge_quote is not None:
            await self._maintain_quotes()

    async def on_stop(self) -> None:
        self._stopping = True
        api_id = self._quote_api_id()
        if api_id is not None:
            for side in list(self._open):
                await self._cancel_leg(api_id, side)
        await self.log("CrossArb stopped")

    async def on_recon_done(self, msg: ReconDone) -> None:
        quote_id = self._quote_api_id()
        hedge_id = self._hedge_api_id()
        if msg.api_id not in (quote_id, hedge_id):
            return
        self._recon.add(msg.api_id)
        await self.log(
            f"CrossArb recon api_id={msg.api_id} "
            f"({len(self._recon)}/2)"
        )

        # Rebuild path: drop anything this session left resting on the quote
        # book before arming, so the next place is a true restart.
        if self._restoring and msg.api_id == quote_id:
            await self._cancel_recon_leftovers(msg)

        if self._armed:
            return
        if quote_id in self._recon and hedge_id in self._recon:
            self._armed = True
            self._restoring = False
            await self.log("CrossArb armed — both ledgers are live")

    async def _cancel_recon_leftovers(self, msg: ReconDone) -> None:
        """Cancel owned non-terminal quote orders reported by recon."""
        api_id = msg.api_id
        cancelled = 0
        for cid, order in msg.oms.orders.items():
            if not self.owns(cid):
                continue
            if order.status in _TERMINAL:
                continue
            try:
                if await self.oms.cancel_order(api_id, cid):
                    cancelled += 1
                else:
                    await self.log(
                        f"CrossArb restart cancel refused cid={cid} "
                        f"[{describe(self.oms.last_reject_code)}]: "
                        f"{self.oms.last_reject_reason or 'no reason given'}",
                        level="warn",
                    )
            except Exception:
                await self.log(
                    f"CrossArb restart cancel failed cid={cid}",
                    level="warn",
                )
        if cancelled:
            await self.log(
                f"CrossArb restart cancelled {cancelled} leftover quote "
                f"order(s)"
            )

    # --- market data -------------------------------------------------------

    async def on_best_quote(self, quote: BestQuote) -> None:
        if self._stopping or not self._armed:
            return
        if self._hedge_ticker is None or self._quote_ticker is None:
            return

        key = str(quote.ticker)
        if key == str(self._hedge_ticker):
            if quote.bid <= 0 or quote.ask <= 0 or quote.ask < quote.bid:
                return
            self._hedge_quote = quote
            self._quotes += 1
            if self._quotes % LOG_EVERY == 0:
                await self.log(
                    f"CrossArb hedge touch bid={_fmt(quote.bid)} "
                    f"ask={_fmt(quote.ask)} open={list(self._open)} "
                    f"quotes={self._quotes}"
                )
            # Paused: keep the touch warm, but do not place or reprice.
            if not self.paused:
                await self._maintain_quotes()
            return

        if key == str(self._quote_ticker):
            # Quote-venue BBO is not used for pricing; a push still re-arms
            # after a fill cleared the book, once the hedge touch is known.
            if self._hedge_quote is not None and not self.paused:
                await self._maintain_quotes()

    # --- quoting -----------------------------------------------------------

    async def _maintain_quotes(self) -> None:
        if self.paused or self._stopping:
            return
        hedge = self._hedge_quote
        api_id = self._quote_api_id()
        if hedge is None or api_id is None:
            return
        info = await self._instrument(quote=True)
        if info is None:
            return

        x_lo = self.paras["x_lo_bps"]
        x_hi = self.paras["x_hi_bps"]
        mid = self.paras["x_mid_bps"]

        for side in self.paras["side"]:
            leg = self._open.get(side)
            if leg is not None:
                if leg.canceling:
                    continue
                if not edge_in_band(side, leg.price, hedge, x_lo, x_hi):
                    edge = edge_bps(side, leg.price, hedge)
                    await self.log(
                        f"CrossArb {side.value} edge {_fmt(edge)}bps "
                        f"outside [{_fmt(x_lo)}, {_fmt(x_hi)}] — cancel "
                        f"cid={leg.cid}"
                    )
                    await self._cancel_leg(api_id, side)
                continue

            raw = quote_raw_price(side, hedge, mid)
            await self._place_quote(api_id, info, side, raw)

    async def _place_quote(
        self,
        api_id: int,
        info: SymbolInfo,
        side: Side,
        raw_price: Decimal,
    ) -> None:
        if self.paused or self._stopping:
            return
        if side in self._open:
            return
        price = info.round_price(raw_price)
        qty = info.round_qty(self.paras["qty"])
        if price <= 0 or qty <= 0 or not info.meets_minimums(qty, price):
            await self.log(
                f"CrossArb skip {side.value} {_fmt(qty)} @ {_fmt(price)} "
                f"— below venue minimums",
                level="warn",
            )
            return

        short = await self._shortfall(api_id, info, side, qty, price)
        if short is not None:
            await self.log(
                f"CrossArb skip {side.value} {_fmt(qty)} @ {_fmt(price)}: "
                f"quote {short}",
                level="warn",
            )
            return

        # Do not rest a quote we could not hedge — inventory risk with no
        # answer is worse than a skipped tick.
        hedge_short = await self._hedge_shortfall_for_quote(side, qty)
        if hedge_short is not None:
            await self.log(
                f"CrossArb skip {side.value} {_fmt(qty)} @ {_fmt(price)}: "
                f"hedge {hedge_short}",
                level="warn",
            )
            return

        accepted = await self.oms.submit_order(
            api_id,
            ticker=info.ticker,
            side=side,
            qty=qty,
            type=OrderType.LIMIT,
            price=price,
            tif=TimeInForce.POST_ONLY,
        )
        cid = self.oms.last_client_order_id
        if not accepted or cid is None:
            await self.log(
                f"CrossArb place refused {side.value} "
                f"[{describe(self.oms.last_reject_code)}]: "
                f"{self.oms.last_reject_reason or 'no reason given'}",
                level="warn",
            )
            return
        self._open[side] = _OpenLeg(cid=str(cid), side=side, price=price)
        await self.log(
            f"CrossArb POST {side.value.upper()} {info.symbol} "
            f"{_fmt(price)}@{_fmt(qty)} cid={cid}"
        )

    async def _cancel_leg(self, api_id: int, side: Side) -> None:
        leg = self._open.get(side)
        if leg is None:
            return
        leg.canceling = True
        try:
            if not await self.oms.cancel_order(api_id, leg.cid):
                await self.log(
                    f"CrossArb cancel not accepted cid={leg.cid} "
                    f"[{describe(self.oms.last_reject_code)}]: "
                    f"{self.oms.last_reject_reason or 'no reason given'}",
                    level="warn",
                )
                self._open.pop(side, None)
        except Exception:
            await self.log(
                f"CrossArb cancel failed cid={leg.cid}", level="warn"
            )
            self._open.pop(side, None)

    # --- hedge -------------------------------------------------------------

    async def _hedge_fill(self, quote_side: Side, cid: str) -> None:
        """Fire one full-qty IOC opposite the filled quote side."""
        if cid in self._hedged:
            return
        self._hedged.add(cid)

        # Drop the filled leg and cancel the sibling, if any.
        self._open.pop(quote_side, None)
        quote_api = self._quote_api_id()
        if quote_api is not None:
            for side in list(self._open):
                await self._cancel_leg(quote_api, side)

        hedge = self._hedge_quote
        hedge_api = self._hedge_api_id()
        info = await self._instrument(quote=False)
        if hedge is None or hedge_api is None or info is None:
            await self.log(
                f"CrossArb cannot hedge cid={cid} — missing hedge quote "
                f"or instrument",
                level="error",
            )
            return

        hedge_side = Side.BUY if quote_side is Side.SELL else Side.SELL
        raw = hedge_raw_price(hedge_side, hedge, self.paras["x_hi_bps"])
        price = info.round_price(raw)
        qty = info.round_qty(self.paras["qty"])
        if price <= 0 or qty <= 0 or not info.meets_minimums(qty, price):
            await self.log(
                f"CrossArb hedge {_fmt(qty)} @ {_fmt(price)} below minimums "
                f"(quote cid={cid})",
                level="error",
            )
            return

        short = await self._shortfall(
            hedge_api, info, hedge_side, qty, price
        )
        if short is not None:
            await self.log(
                f"CrossArb hedge cannot fund {hedge_side.value} "
                f"{_fmt(qty)} @ {_fmt(price)}: {short} (quote cid={cid})",
                level="error",
            )
            return

        accepted = await self.oms.submit_order(
            hedge_api,
            ticker=info.ticker,
            side=hedge_side,
            qty=qty,
            type=OrderType.LIMIT,
            price=price,
            tif=TimeInForce.IOC,
        )
        hcid = self.oms.last_client_order_id
        if not accepted:
            await self.log(
                f"CrossArb hedge refused [{describe(self.oms.last_reject_code)}]: "
                f"{self.oms.last_reject_reason or 'no reason given'} "
                f"(quote cid={cid})",
                level="error",
            )
            return
        await self.log(
            f"CrossArb IOC {hedge_side.value.upper()} {info.symbol} "
            f"{_fmt(price)}@{_fmt(qty)} cid={hcid} hedges quote cid={cid}"
        )

    # --- order events ------------------------------------------------------

    async def on_order_update(self, api_id: int, order: Order) -> None:
        cid = order.client_order_id
        if not self.owns(cid) or cid is None:
            return
        key = str(cid)
        self._filled[key] = order.filled_qty

        quote_api = self._quote_api_id()
        if api_id == quote_api:
            side = self._side_of(key)
            if key in self._hedged:
                await self.log(
                    f"CrossArb quote update after hedge cid={key} "
                    f"status={order.status.value} "
                    f"filled={_fmt(order.filled_qty)}/"
                    f"{_fmt(order.qty)}"
                )
                if order.status in _TERMINAL and side is not None:
                    self._open.pop(side, None)
                return

            if order.status in _FILL_STATUSES:
                fill_side = side if side is not None else order.side
                await self.log(
                    f"CrossArb quote {order.status.value} cid={key} "
                    f"{fill_side.value} filled={_fmt(order.filled_qty)}/"
                    f"{_fmt(order.qty)} — hedging full qty"
                )
                await self._hedge_fill(fill_side, key)
                return

            if order.status in _TERMINAL and side is not None:
                self._open.pop(side, None)
            return

        # Hedge-account updates: log only.
        if api_id == self._hedge_api_id():
            await self.log(
                f"CrossArb hedge update cid={key} "
                f"status={order.status.value} "
                f"filled={_fmt(order.filled_qty)}/{_fmt(order.qty)}"
            )

    async def on_fill(self, api_id: int, fill: Fill) -> None:
        if not self.owns(fill.client_order_id):
            return
        await self.log(
            f"CrossArb fill api={api_id} cid={fill.client_order_id} "
            f"{_fmt(fill.price)}@{_fmt(fill.qty)}"
        )

    async def on_order_reject(self, api_id: int, reject: OrderReject) -> None:
        if not self.owns(reject.client_order_id):
            return
        cid = str(reject.client_order_id)
        side = self._side_of(cid)
        if side is not None:
            self._open.pop(side, None)
        await self.log(
            f"CrossArb order refused cid={cid} "
            f"[{describe(reject.error_code)}] {reject.reason}",
            level="warn",
        )

    async def on_cancel_reject(
        self, api_id: int, reject: CancelReject
    ) -> None:
        if not self.owns(reject.client_order_id):
            return
        cid = str(reject.client_order_id)
        side = self._side_of(cid)
        if side is not None:
            # Already gone at the venue — stop waiting on it.
            self._open.pop(side, None)
        await self.log(
            f"CrossArb cancel refused cid={cid} "
            f"[{describe(reject.error_code)}] {reject.reason}",
            level="warn",
        )

    # --- helpers -----------------------------------------------------------

    def _side_of(self, cid: str) -> Side | None:
        for side, leg in self._open.items():
            if leg.cid == cid:
                return side
        return None

    async def _shortfall(
        self,
        api_id: int,
        info: SymbolInfo,
        side: Side,
        qty: Decimal,
        price: Decimal,
    ) -> str | None:
        if side is Side.SELL:
            asset, need = info.base, qty
        else:
            asset, need = info.quote, qty * price
        have = await self.ledger.available(asset, api_id)
        if have >= need:
            return None
        return f"need {_fmt(need)} {asset}, available {_fmt(have)}"

    async def _hedge_shortfall_for_quote(
        self, quote_side: Side, qty: Decimal
    ) -> str | None:
        """Why the hedge account could not IOC-answer this quote, or None."""
        hedge = self._hedge_quote
        hedge_api = self._hedge_api_id()
        info = await self._instrument(quote=False)
        if hedge is None or hedge_api is None or info is None:
            return "no hedge quote or instrument"
        hedge_side = Side.BUY if quote_side is Side.SELL else Side.SELL
        raw = hedge_raw_price(hedge_side, hedge, self.paras["x_hi_bps"])
        price = info.round_price(raw)
        hedge_qty = info.round_qty(qty)
        if price <= 0 or hedge_qty <= 0:
            return f"unusable hedge size {_fmt(hedge_qty)} @ {_fmt(price)}"
        return await self._shortfall(
            hedge_api, info, hedge_side, hedge_qty, price
        )

    async def _instrument(self, *, quote: bool) -> SymbolInfo | None:
        if quote:
            if self._quote_info is not None:
                return self._quote_info
            ticker = self._quote_ticker
        else:
            if self._hedge_info is not None:
                return self._hedge_info
            ticker = self._hedge_ticker
        if ticker is None or self.symbols is None:
            await self.log(
                "CrossArb cannot resolve instrument — no ticker/symbols",
                level="error",
            )
            return None
        try:
            info = await self.symbols.get(ticker)
        except Exception as exc:
            await self.log(
                f"CrossArb cannot resolve {ticker}: {exc}",
                level="error",
            )
            return None
        if quote:
            self._quote_info = info
        else:
            self._hedge_info = info
        return info

    def _quote_api_id(self) -> int | None:
        if self.session is None or len(self.session.td_api_ids) < 1:
            return None
        return self.session.td_api_ids[0]

    def _hedge_api_id(self) -> int | None:
        if self.session is None or len(self.session.td_api_ids) < 2:
            return None
        return self.session.td_api_ids[1]
