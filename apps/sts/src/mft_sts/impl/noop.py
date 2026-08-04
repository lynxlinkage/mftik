"""Example strategy — walk three quote levels per side, then exit.

A twelve-step sequence, one action per ``exec_interval_ms`` tick. First as BUY,
then the same three prices as SELL; after the last step the strategy exits.
``on_stop`` cancels any order still resting (e.g. if stopped mid-cycle).

1. PLACE BUY  at ``mid * (1 - gap)``
2. CANCEL
3. PLACE BUY  at ``mid``
4. CANCEL
5. PLACE BUY  at ``mid * (1 + gap)``
6. CANCEL
7. PLACE SELL at ``mid * (1 - gap)``
8. CANCEL
9. PLACE SELL at ``mid``
10. CANCEL
11. PLACE SELL at ``mid * (1 + gap)``
12. CANCEL → exit

Size is ``qty_quote / price`` of the pair's quote currency. There is no
configured mid: the price comes from the order book subscription.

Prices and sizes are rounded through the symbol plane before submission. TD
does not validate orders against venue filters, so that is this strategy's job.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

from mft.exchange.models import (
    Balance,
    Fill,
    Order,
    OrderBook,
    OrderType,
    Side,
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

DEFAULT_EXEC_INTERVAL_MS = 1000
DEFAULT_GAP_BPS = Decimal("10")
DEFAULT_QTY_QUOTE = Decimal("100")

BPS = Decimal("10000")

_PriceFn = Callable[[Decimal, Decimal], Decimal]


def _fmt(value: object) -> str:
    """Compact Decimal for logs — drop trailing zeros from Numeric(38, 18)."""
    if value is None:
        return "?"
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(d.normalize(), "f")

_PRICE_LEVELS: tuple[_PriceFn, ...] = (
    lambda mid, gap: mid * (1 - gap),
    lambda mid, gap: mid,
    lambda mid, gap: mid * (1 + gap),
)


def _build_steps() -> tuple[tuple[Side, _PriceFn] | None, ...]:
    """PLACE/CANCEL for each level as BUY, then again as SELL."""
    steps: list[tuple[Side, _PriceFn] | None] = []
    for side in (Side.BUY, Side.SELL):
        for price_fn in _PRICE_LEVELS:
            steps.append((side, price_fn))
            steps.append(None)  # cancel
    return tuple(steps)


# PLACE steps carry (side, price_fn); CANCEL steps are None.
_STEPS = _build_steps()


class NoopStrategy(Strategy):
    name = "noop"
    id = 1

    def __init__(self) -> None:
        super().__init__()
        self._tick_token: TimerToken | None = None
        self._venue: str | None = None
        self._symbol: str | None = None
        self._info: SymbolInfo | None = None
        self._mid: Decimal | None = None
        self._book_updates = 0
        self._step = 0
        self._open_cid: str | None = None

    @classmethod
    def on_initialized(cls, params: Any) -> dict[str, Any]:
        """Validate the three knobs. There is deliberately no ``mid``."""
        out = super().on_initialized(params)
        if "mid" in out:
            raise ValueError(
                "noop no longer takes a mid; it reads one from the order book"
            )

        interval = int(out.get("exec_interval_ms", DEFAULT_EXEC_INTERVAL_MS))
        if interval <= 0:
            raise ValueError(f"exec_interval_ms must be positive, got {interval}")

        gap_bps = Decimal(str(out.get("gap_bps", DEFAULT_GAP_BPS)))
        if gap_bps < 0:
            raise ValueError(f"gap_bps must not be negative, got {gap_bps}")

        if "qty_usd" in out:
            raise ValueError(
                "qty_usd is now qty_quote; the size is in the pair's quote "
                "currency, which is not always a dollar"
            )
        qty_quote = Decimal(str(out.get("qty_quote", DEFAULT_QTY_QUOTE)))
        if qty_quote <= 0:
            raise ValueError(f"qty_quote must be positive, got {qty_quote}")

        out["exec_interval_ms"] = interval
        out["gap_bps"] = gap_bps
        out["qty_quote"] = qty_quote
        return out

    # --- lifecycle ---------------------------------------------------------

    async def on_start(self) -> None:
        self._resolve_feed()
        await self.log(
            f"NoopStrategy started venue={self._venue} symbol={self._symbol} "
            f"exec_interval_ms={self.paras['exec_interval_ms']} "
            f"gap_bps={_fmt(self.paras['gap_bps'])} "
            f"qty_quote={_fmt(self.paras['qty_quote'])}"
        )
        self._tick_token = self.timer.token()
        self._arm_timer()

    async def on_ready(self) -> None:
        await self.log("NoopStrategy ready — waiting for the first order book")

    async def on_stop(self) -> None:
        self._cancel_timer()
        api_id = self._primary_api_id()
        if api_id is not None:
            await self._cancel_open(api_id)
        await self.log("NoopStrategy stopped")

    async def on_pause(self) -> None:
        await super().on_pause()
        self._cancel_timer()
        await self.log("NoopStrategy paused")

    async def on_resume(self) -> None:
        await super().on_resume()
        if self._tick_token is None:
            self._tick_token = self.timer.token()
        self._arm_timer()
        await self.log("NoopStrategy resumed")

    # --- market data -------------------------------------------------------

    async def on_order_book(self, book: OrderBook) -> None:
        if not book.bids or not book.asks:
            return

        self._mid = (book.bids[0].price + book.asks[0].price) / 2
        self._book_updates += 1
        if self._book_updates == 1:
            await self.log(
                f"NoopStrategy first book {book.symbol} "
                f"bid={_fmt(book.bids[0].price)} ask={_fmt(book.asks[0].price)} "
                f"mid={_fmt(self._mid)} — quoting starts"
            )

    # --- execution ---------------------------------------------------------

    async def _on_tick(self) -> None:
        api_id = self._primary_api_id()
        if api_id is None:
            await self.log(
                "NoopStrategy has no TD api_id — exiting", level="warn"
            )
            self.exit("noop_no_td")
            return
        if self._mid is None:
            await self.log("NoopStrategy tick skipped — no book yet")
            return
        if self._step >= len(_STEPS):
            self.exit("noop_done")
            return

        step = self._step
        action = _STEPS[step]
        if action is None:
            await self._cancel_open(api_id)
        else:
            side, price_fn = action
            info = await self._instrument()
            if info is None:
                return
            gap = self.paras["gap_bps"] / BPS
            await self._quote(api_id, info, side, price_fn(self._mid, gap))

        self._step = step + 1
        if self._step >= len(_STEPS):
            await self.log("NoopStrategy cycle complete — exiting")
            self._cancel_timer()
            self.exit("noop_done")

    async def _quote(
        self,
        api_id: int,
        info: SymbolInfo,
        side: Side,
        raw_price: Decimal,
    ) -> None:
        """Round through the plane, then submit — TD checks none of this."""
        price = info.round_price(raw_price)
        if price <= 0:
            return
        qty = info.qty_for_notional(self.paras["qty_quote"], price)
        if qty <= 0 or not info.meets_minimums(qty, price):
            await self.log(
                f"NoopStrategy skip {side.value} {_fmt(price)}@{_fmt(qty)} — "
                f"below venue minimums",
                level="warn",
            )
            return

        accepted = await self.oms.submit_order(
            api_id,
            symbol=info.symbol,
            side=side,
            qty=qty,
            type=OrderType.LIMIT,
            price=price,
        )
        cid = self.oms.last_client_order_id
        if not accepted:
            # TD never took it, so there is no order to cancel on the next step.
            await self.log(
                f"NoopStrategy PLACE rejected by TD cid={cid} step={self._step}",
                level="warn",
            )
            return
        self._open_cid = cid
        await self.log(
            f"NoopStrategy PLACE {side.value.upper()} {info.symbol} "
            f"{_fmt(price)}@{_fmt(qty)} cid={cid} step={self._step}"
        )

    async def _cancel_open(self, api_id: int) -> None:
        cid = self._open_cid
        if cid is None:
            return
        try:
            if await self.oms.cancel_order(api_id, cid):
                await self.log(f"NoopStrategy CANCEL cid={cid} step={self._step}")
            else:
                await self.log(
                    f"NoopStrategy cancel not accepted by TD cid={cid}",
                    level="warn",
                )
        except Exception:
            await self.log(
                f"NoopStrategy cancel failed cid={cid}", level="warn"
            )
        self._open_cid = None

    async def _instrument(self) -> SymbolInfo | None:
        """Instrument metadata, fetched once and cached for the session."""
        if self._info is not None:
            return self._info
        if self._venue is None or self._symbol is None:
            await self.log(
                "NoopStrategy has no md feed to derive an instrument from",
                level="warn",
            )
            return None
        try:
            self._info = await self.symbols.get(self._venue, self._symbol)
        except Exception as exc:
            await self.log(
                f"NoopStrategy cannot resolve {self._venue}/{self._symbol}: "
                f"{exc}",
                level="error",
            )
            return None
        await self.log(
            f"NoopStrategy instrument {self._venue}/{self._symbol} "
            f"tick={_fmt(self._info.price_tick)} step={_fmt(self._info.qty_step)} "
            f"min_notional={_fmt(self._info.filter('min_notional'))}"
        )
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

    # --- reporting ---------------------------------------------------------

    async def on_recon_done(self, msg: ReconDone) -> None:
        recon_bals = dict(msg.oms.balances)
        # TD's ledger is the authority; read it rather than keeping a copy.
        local_bals = await self.ledger.balances(msg.api_id)

        def _bal_key(bals: dict[str, Balance]) -> dict[str, tuple[str, str]]:
            return {
                a: (_fmt(b.free), _fmt(b.locked))
                for a, b in sorted(bals.items())
            }

        await self.log(
            f"NoopStrategy recon done api_id={msg.api_id} "
            f"orders={len(msg.oms.orders)} "
            f"balances={ {a: _fmt(b.free) for a, b in recon_bals.items()} }"
        )
        if _bal_key(local_bals) != _bal_key(recon_bals):
            await self.log(
                f"NoopStrategy OMS balance mismatch "
                f"local={ {a: _fmt(b.free) for a, b in local_bals.items()} } "
                f"recon={ {a: _fmt(b.free) for a, b in recon_bals.items()} }",
                level="warn",
            )

    async def on_order_update(self, api_id: int, order: Order) -> None:
        if not self.owns(order.client_order_id):
            return
        await self.log(
            f"NoopStrategy on_order_update api_id={api_id} "
            f"cid={order.client_order_id} status={order.status} "
            f"{order.side} {order.symbol} "
            f"{_fmt(order.price)}@{_fmt(order.qty)}"
        )

    async def on_fill(self, api_id: int, fill: Fill) -> None:
        if not self.owns(fill.client_order_id):
            return
        await self.log(
            f"NoopStrategy on_fill api_id={api_id} "
            f"cid={fill.client_order_id} {fill.side} "
            f"{fill.symbol} {_fmt(fill.price)}@{_fmt(fill.qty)}"
        )

    async def on_order_reject(self, api_id: int, reject: OrderReject) -> None:
        await self.log(
            f"NoopStrategy on_order_reject api_id={api_id} "
            f"cid={reject.client_order_id} "
            f"[{describe(reject.error_code)}] reason={reject.reason}",
            level="warn",
        )

    async def on_cancel_reject(self, api_id: int, reject: CancelReject) -> None:
        await self.log(
            f"NoopStrategy on_cancel_reject api_id={api_id} "
            f"cid={reject.client_order_id} "
            f"[{describe(reject.error_code)}] reason={reject.reason}",
            level="warn",
        )

    async def on_balance_update(self, api_id: int, balance: Balance) -> None:
        await self.log(
            f"NoopStrategy on_balance_update api_id={api_id} "
            f"{balance.asset} free={_fmt(balance.free)} "
            f"locked={_fmt(balance.locked)}"
        )

    # --- timer -------------------------------------------------------------

    def _arm_timer(self) -> None:
        if self._tick_token is None:
            return
        interval = int(self.paras["exec_interval_ms"])
        self._tick_token.register(
            self.timer.now_ms() + interval, interval, self._on_tick
        )

    def _cancel_timer(self) -> None:
        if self._tick_token is not None:
            self._tick_token.cancel()

    def _primary_api_id(self) -> int | None:
        if self.session is None or not self.session.td_api_ids:
            return None
        return self.session.td_api_ids[0]
