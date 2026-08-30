"""TWAP — slice a size across evenly spaced IOC takes at the touch.

One TD account, one best-quote feed. After the first quote and TD recon both
arrive, the strategy schedules ``num_round`` IOC attempts across
``exec_interval_s * num_round`` seconds, firing first at half an interval so
the slices sit in the middle of each bucket rather than bunched at the ends.

Each tick crosses the book at the far touch (buy pays the ask, sell hits the
bid) for ``qty_per_round`` / ``qty_quote_per_round``. A round that fills any
size — full or partial — counts as one success; a zero-fill IOC does not.
IOC never rests, so the filled total is at most the configured target. The
session ends when ``num_round`` successes land, or when the end time passes.

Works on Spot and Perp. Perp arms only after ``ledger.ensure_leverage`` so TD
can size pre-locks as ``notional / leverage``; funding and reduce-only are
out of scope — this is the same IOC slicer with margin-aware funding checks.
"""

from __future__ import annotations

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
from mftik.exchange.reservations import commitment_for
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.protocol import (
    OrderReject,
    ReconDone,
    RejectCode,
    SymbolInfo,
    Topics,
)
from mftik.protocol.reject_codes import describe, is_normalized
from mftik.strategy import Strategy
from mftik.strategy.timer import TimerToken

_TERMINAL = frozenset({OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED})

_NO_FUNDS = "insufficient balance"


def _positive_int(paras: dict[str, Any], name: str) -> int:
    raw = paras.get(name)
    if raw is None:
        raise ValueError(f"{name} is required")
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _positive_decimal(paras: dict[str, Any], name: str) -> Decimal:
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


def _fmt(value: object) -> str:
    """Compact Decimal for logs — drop trailing zeros from Numeric(38, 18)."""
    if value is None:
        return "?"
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(d.normalize(), "f")


def _refusal_reason(code: int | str, reason: str) -> str:
    if code == RejectCode.TD_INSUFFICIENT_BALANCE:
        return "twap_insufficient_balance"
    if not is_normalized(code) and _NO_FUNDS in reason.lower():
        return "twap_insufficient_balance"
    return "twap_refused"


class TwapStrategy(Strategy):
    name = "twap"
    id = 5

    def __init__(self) -> None:
        super().__init__()
        self._tick_token: TimerToken | None = None
        self._ticker: UniversalTicker | None = None
        self._info: SymbolInfo | None = None
        #: Far-touch price we would cross — ask for buys, bid for sells.
        self._ref: Decimal | None = None
        self._recon_done = False
        self._armed = False
        self._done = False
        self._end_ms: int | None = None
        #: Successful rounds (any fill on a terminal IOC).
        self._successes = 0
        #: cid of the IOC still waiting for a terminal update, if any.
        self._open_cid: str | None = None
        #: cid → cumulative filled qty for that order.
        self._filled: dict[str, Decimal] = {}
        #: cids already counted toward ``_successes``.
        self._counted: set[str] = set()

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

        has_qty = out.get("qty_per_round") is not None
        has_quote = out.get("qty_quote_per_round") is not None
        if has_qty and has_quote:
            raise ValueError(
                "set either qty_per_round or qty_quote_per_round, not both: "
                "qty_per_round is in base units, qty_quote_per_round in the "
                "pair's quote currency"
            )
        if not has_qty and not has_quote:
            raise ValueError("one of qty_per_round or qty_quote_per_round is required")
        if has_qty:
            out["qty_per_round"] = _positive_decimal(out, "qty_per_round")
        else:
            out["qty_quote_per_round"] = _positive_decimal(out, "qty_quote_per_round")

        interval_s = _positive_decimal(out, "exec_interval_s")
        num_round = _positive_int(out, "num_round")
        out["exec_interval_s"] = interval_s
        out["num_round"] = num_round
        # Total window the slices are spaced across. Kept derived so the
        # schedule always matches interval × rounds.
        out["exec_total_s"] = interval_s * num_round
        return out

    # --- lifecycle ---------------------------------------------------------

    async def on_start(self) -> None:
        try:
            self.session.td_sole()
        except RuntimeError as exc:
            self.fail(str(exc))
            return
        self._resolve_feed()
        await self.log(
            f"TwapStrategy started ticker={self._ticker} "
            f"side={self.paras['side'].value} "
            f"exec_interval_s={_fmt(self.paras['exec_interval_s'])} "
            f"num_round={self.paras['num_round']} "
            f"exec_total_s={_fmt(self.paras['exec_total_s'])} "
            f"size={self._size_description()}"
        )
        self._tick_token = self.timer.token()

    async def on_ready(self) -> None:
        await self.log("TwapStrategy ready — waiting for first best_quote and TD recon")

    async def on_stop(self) -> None:
        self._cancel_timer()
        await self.log(
            f"TwapStrategy stopped filled={_fmt(self._filled_qty())} "
            f"successes={self._successes}/{self.paras['num_round']}"
        )

    async def on_recon_done(self, msg: ReconDone) -> None:
        try:
            sole = self.session.td_sole()
        except RuntimeError as exc:
            self.fail(str(exc))
            return
        if msg.api_id != sole:
            return
        if self._recon_done:
            return
        self._recon_done = True
        await self.log(f"TwapStrategy recon ready api_id={msg.api_id}")
        await self._maybe_arm()

    # --- market data -------------------------------------------------------

    async def on_best_quote(self, quote: BestQuote) -> None:
        price = self._reference(quote)
        if price is None or price <= 0:
            return
        first = self._ref is None
        self._ref = price
        if first:
            await self.log(
                f"TwapStrategy first quote ref={_fmt(price)} "
                f"side={self.paras['side'].value}"
            )
            await self._maybe_arm()

    def _reference(self, quote: BestQuote) -> Decimal | None:
        if self.paras["side"] is Side.BUY:
            return quote.ask
        return quote.bid

    # --- arming ------------------------------------------------------------

    async def _maybe_arm(self) -> None:
        """Start the schedule once both recon and a quote have landed."""
        if self._armed or self._done:
            return
        if not self._recon_done or self._ref is None:
            return

        if not await self._ensure_perp_leverage():
            return

        self._armed = True
        self._successes = 0
        self._filled.clear()
        self._counted.clear()

        now = self.timer.now_ms()
        total_ms = int(self.paras["exec_total_s"] * 1000)
        interval_ms = int(self.paras["exec_interval_s"] * 1000)
        self._end_ms = now + total_ms
        # First fire in the middle of the first bucket; then every interval.
        first_ms = now + interval_ms // 2
        if self._tick_token is None:
            self._tick_token = self.timer.token()
        self._tick_token.register(first_ms, interval_ms, self._on_tick)
        await self.log(
            f"TwapStrategy armed end_ms={self._end_ms} "
            f"first_ms={first_ms} interval_ms={interval_ms} "
            f"num_round={self.paras['num_round']}"
        )

    async def _ensure_perp_leverage(self) -> bool:
        """For Perp, ask TD to cache leverage before any IOC can pre-lock.

        Spot returns True immediately. Failure fails the session — without a
        figure TD would reserve at 1x, and the strategy's own shortfall check
        would disagree with that guess.
        """
        if self._ticker is None or self._ticker.category is not Category.PERP:
            return True

        try:
            api_id = self.session.td_sole()
        except RuntimeError:
            await self.log("TwapStrategy has no TD api_id — exiting", level="warn")
            self._done = True
            self.fail("twap_no_td")
            return False

        info = await self._instrument()
        if info is None:
            self._done = True
            self.fail("twap_no_instrument")
            return False

        lev = await self.ledger.ensure_leverage(info.ticker, api_id)
        if lev is None:
            reason = self.ledger.last_reject_reason or "leverage unavailable"
            await self.log(
                f"TwapStrategy ensure_leverage failed ticker={info.ticker}: "
                f"{reason} — exiting",
                level="error",
            )
            self._done = True
            self.fail("twap_leverage_unavailable")
            return False

        await self.log(
            f"TwapStrategy leverage={_fmt(lev)} ticker={info.ticker}"
        )
        return True

    # --- execution ---------------------------------------------------------

    async def _on_tick(self) -> None:
        if self._done or not self._armed:
            return

        if self._end_ms is not None and self.timer.now_ms() >= self._end_ms:
            await self._finish("twap_time_up")
            return

        if self._successes >= self.paras["num_round"]:
            await self._finish("twap_done")
            return

        if self._open_cid is not None:
            # Previous IOC has not gone terminal yet — skip rather than stack.
            return

        try:
            api_id = self.session.td_sole()
        except RuntimeError:
            await self.log("TwapStrategy has no TD api_id — exiting", level="warn")
            self._done = True
            self._cancel_timer()
            self.fail("twap_no_td")
            return

        if self._ref is None or self._ref <= 0:
            await self.log("TwapStrategy tick skipped — no quote", level="warn")
            return

        info = await self._instrument()
        if info is None:
            self._done = True
            self._cancel_timer()
            self.fail("twap_no_instrument")
            return

        await self._place_ioc(api_id, info)

    async def _place_ioc(self, api_id: int, info: SymbolInfo) -> None:
        price = info.round_price(self._ref) if self._ref is not None else None
        if price is None or price <= 0:
            return

        qty = await self._round_qty(info, price)
        if qty is None:
            return

        short = await self._shortfall(api_id, info, qty, price)
        if short is not None:
            await self.log(
                f"TwapStrategy cannot fund {_fmt(qty)} @ {_fmt(price)}: "
                f"{short} — exiting",
                level="error",
            )
            self._done = True
            self._cancel_timer()
            self.fail("twap_insufficient_balance")
            return

        accepted = await self.oms.submit_order(
            api_id,
            ticker=info.ticker,
            side=self.paras["side"],
            qty=qty,
            type=OrderType.LIMIT,
            price=price,
            tif=TimeInForce.IOC,
        )
        cid = self.oms.last_client_order_id
        if not accepted:
            reason = self.oms.last_reject_reason
            code = self.oms.last_reject_code
            await self.log(
                f"TwapStrategy IOC refused by TD cid={cid} "
                f"[{describe(code)}]: {reason or 'no reason given'} — exiting",
                level="error",
            )
            self._done = True
            self._cancel_timer()
            self.fail(_refusal_reason(code, reason))
            return

        self._open_cid = cid
        await self.log(
            f"TwapStrategy IOC {self.paras['side'].value.upper()} "
            f"{info.symbol} {_fmt(qty)} @ {_fmt(price)} cid={cid} "
            f"round={self._successes + 1}/{self.paras['num_round']}"
        )

    async def _round_qty(self, info: SymbolInfo, price: Decimal) -> Decimal | None:
        configured = self.paras.get("qty_per_round")
        if configured is not None:
            qty = info.round_qty(configured)
        else:
            qty = info.qty_for_notional(self.paras["qty_quote_per_round"], price)
        if qty <= 0 or not info.meets_minimums(qty, price):
            # Not a hard fail: the next tick may see a price that clears the
            # notional floor. Skipping keeps the schedule honest.
            await self.log(
                f"TwapStrategy skip {_fmt(qty)} @ {_fmt(price)} — below venue minimums",
                level="warn",
            )
            return None
        return qty

    async def _shortfall(
        self, api_id: int, info: SymbolInfo, qty: Decimal, price: Decimal
    ) -> str | None:
        """Why the ledger cannot fund this IOC, or None if it can.

        The commitment is TD's arithmetic, asked here rather than reproduced:
        a copy that drifted would size against a figure TD does not enforce.
        """
        held = commitment_for(
            category=info.category,
            side=self.paras["side"],
            order_type=OrderType.LIMIT,
            base=info.base,
            quote=info.quote,
            qty=qty,
            price=price,
            leverage=self.ledger.leverage(info.ticker, api_id),
        )
        if held is None:
            return None
        asset, need = held
        have = await self.ledger.available(asset, api_id)
        if have >= need:
            return None
        return f"need {_fmt(need)} {asset}, available {_fmt(have)}"

    # --- order events ------------------------------------------------------

    async def on_order_update(self, api_id: int, order: Order) -> None:
        cid = order.client_order_id
        if not self.owns(cid):
            return
        key = str(cid)
        self._filled[key] = order.filled_qty
        if order.status not in _TERMINAL:
            return
        if self._open_cid == key:
            self._open_cid = None

        if key not in self._counted and order.filled_qty > 0:
            self._counted.add(key)
            self._successes += 1
            await self.log(
                f"TwapStrategy round ok cid={key} "
                f"filled={_fmt(order.filled_qty)} "
                f"successes={self._successes}/{self.paras['num_round']} "
                f"total={_fmt(self._filled_qty())}"
            )
            if self._successes >= self.paras["num_round"]:
                await self._finish("twap_done")
                return

        if order.filled_qty <= 0:
            await self.log(
                f"TwapStrategy IOC zero-fill cid={key} status={order.status.value}",
                level="warn",
            )

    async def on_fill(self, api_id: int, fill: Fill) -> None:
        if not self.owns(fill.client_order_id):
            return
        await self.log(
            f"TwapStrategy fill cid={fill.client_order_id} "
            f"{_fmt(fill.price)}@{_fmt(fill.qty)} "
            f"total={_fmt(self._filled_qty())}"
        )

    async def on_order_reject(self, api_id: int, reject: OrderReject) -> None:
        if not self.owns(reject.client_order_id):
            return
        cid = str(reject.client_order_id)
        if self._open_cid == cid:
            self._open_cid = None
        await self.log(
            f"TwapStrategy order refused cid={cid} "
            f"[{describe(reject.error_code)}] {reject.reason}",
            level="warn",
        )

    # --- endings -----------------------------------------------------------

    async def _finish(self, reason: str) -> None:
        if self._done:
            return
        self._done = True
        self._cancel_timer()
        await self.log(
            f"TwapStrategy {reason} "
            f"filled={_fmt(self._filled_qty())} "
            f"successes={self._successes}/{self.paras['num_round']}"
        )
        self.exit(reason)

    def _filled_qty(self) -> Decimal:
        return sum(self._filled.values(), Decimal("0"))

    def _size_description(self) -> str:
        if self.paras.get("qty_per_round") is not None:
            return f"qty_per_round={_fmt(self.paras['qty_per_round'])}"
        return f"qty_quote_per_round={_fmt(self.paras['qty_quote_per_round'])}"

    # --- plumbing ----------------------------------------------------------

    async def _instrument(self) -> SymbolInfo | None:
        if self._info is not None:
            return self._info
        if self._ticker is None:
            await self.log(
                "TwapStrategy has no md feed to derive an instrument from",
                level="warn",
            )
            return None
        try:
            self._info = await self.symbols.get(self._ticker)
        except Exception as exc:
            await self.log(
                f"TwapStrategy cannot resolve {self._ticker}: {exc}",
                level="error",
            )
            return None
        return self._info

    def _resolve_feed(self) -> None:
        md_ids = list(self.session.md_ids) if self.session is not None else []
        if not md_ids:
            return
        try:
            _topic, self._ticker = Topics.parse_md_feed(md_ids[0])
        except ValueError:
            return

    def _cancel_timer(self) -> None:
        if self._tick_token is not None:
            self._tick_token.cancel()
