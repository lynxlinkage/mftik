"""Example strategy — quote three levels around the live mid, on a timer.

Every tick it places at ``mid * (1 - gap)``, ``mid``, and ``mid * (1 + gap)``,
each sized to ``qty_quote`` of the pair's quote currency, then cancels them on
the next tick. There is no configured mid: the price comes from the order book
subscription, which is the only thing that knows what the market is doing.

Prices and sizes are rounded through the symbol plane before submission. TD
does not validate orders against venue filters, so that is this strategy's job.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from mft.exchange.models import Balance, OrderBook, OrderType, Side
from mft.protocol import ReconDone, SymbolInfo, Topics, UntypedEnvelope

from mft_sts.strategy import Strategy
from mft_sts.timer import TimerToken

DEFAULT_EXEC_INTERVAL_MS = 1000
DEFAULT_GAP_BPS = Decimal("10")
DEFAULT_QTY_QUOTE = Decimal("100")

BPS = Decimal("10000")


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
        self._open_cids: list[str] = []

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
            f"gap_bps={self.paras['gap_bps']} qty_quote={self.paras['qty_quote']}"
        )
        self._tick_token = self.timer.token()
        self._arm_timer()

    async def on_ready(self) -> None:
        await self.log("NoopStrategy ready — waiting for the first order book")

    async def on_stop(self) -> None:
        self._cancel_timer()
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

    async def on_order_book(self, msg: UntypedEnvelope) -> None:
        try:
            book = OrderBook.model_validate(msg.payload)
        except Exception:
            await self.log(
                "NoopStrategy on_order_book invalid payload", level="warn"
            )
            return
        if not book.bids or not book.asks:
            return

        self._mid = (book.bids[0].price + book.asks[0].price) / 2
        self._book_updates += 1
        if self._book_updates == 1:
            await self.log(
                f"NoopStrategy first book {book.symbol} "
                f"bid={book.bids[0].price} ask={book.asks[0].price} "
                f"mid={self._mid} — quoting starts"
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

        await self._cancel_open(api_id)

        info = await self._instrument()
        if info is None:
            return

        gap = self.paras["gap_bps"] / BPS
        mid = self._mid
        levels = (
            (Side.BUY, mid * (1 - gap)),
            (Side.BUY, mid),
            (Side.SELL, mid * (1 + gap)),
        )
        for side, raw_price in levels:
            await self._quote(api_id, info, side, raw_price)

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
                f"NoopStrategy skip {side.value} {price}@{qty} — "
                f"below venue minimums",
                level="warn",
            )
            return

        cid = await self.oms.submit_order(
            api_id,
            symbol=info.symbol,
            side=side,
            qty=qty,
            type=OrderType.LIMIT,
            price=price,
        )
        self._open_cids.append(cid)
        await self.log(
            f"NoopStrategy PLACE {side.value.upper()} {info.symbol} "
            f"{price}@{qty} cid={cid}"
        )

    async def _cancel_open(self, api_id: int) -> None:
        for cid in self._open_cids:
            try:
                await self.oms.cancel_order(api_id, cid)
            except Exception:
                await self.log(
                    f"NoopStrategy cancel failed cid={cid}", level="warn"
                )
        if self._open_cids:
            await self.log(f"NoopStrategy CANCEL {len(self._open_cids)} orders")
        self._open_cids.clear()

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
            f"tick={self._info.price_tick} step={self._info.qty_step} "
            f"min_notional={self._info.filter('min_notional')}"
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
        local = self.oms.get(msg.api_id)
        recon_bals = dict(msg.oms.balances)
        local_bals = dict(local.balances) if local is not None else {}

        def _bal_key(bals: dict[str, Balance]) -> dict[str, tuple[str, str]]:
            return {
                a: (str(b.free), str(b.locked))
                for a, b in sorted(bals.items())
            }

        await self.log(
            f"NoopStrategy recon done api_id={msg.api_id} "
            f"orders={len(msg.oms.orders)} "
            f"balances={ {a: str(b.free) for a, b in recon_bals.items()} }"
        )
        if _bal_key(local_bals) != _bal_key(recon_bals):
            await self.log(
                f"NoopStrategy OMS balance mismatch "
                f"local={ {a: str(b.free) for a, b in local_bals.items()} } "
                f"recon={ {a: str(b.free) for a, b in recon_bals.items()} }",
                level="warn",
            )

    async def on_order_update(self, api_id: int, msg: UntypedEnvelope) -> None:
        p = msg.payload
        if not self.owns(p.get("client_order_id")):
            return
        await self.log(
            f"NoopStrategy on_order_update api_id={api_id} "
            f"cid={p.get('client_order_id')} status={p.get('status')} "
            f"{p.get('side')} {p.get('symbol')} {p.get('price')}@{p.get('qty')}"
        )

    async def on_fill(self, api_id: int, msg: UntypedEnvelope) -> None:
        p = msg.payload
        if not self.owns(p.get("client_order_id")):
            return
        await self.log(
            f"NoopStrategy on_fill api_id={api_id} "
            f"cid={p.get('client_order_id')} {p.get('side')} "
            f"{p.get('symbol')} {p.get('price')}@{p.get('qty')}"
        )

    async def on_order_reject(self, api_id: int, msg: UntypedEnvelope) -> None:
        p = msg.payload
        await self.log(
            f"NoopStrategy on_order_reject api_id={api_id} "
            f"cid={p.get('client_order_id')} reason={p.get('reason')}",
            level="warn",
        )

    async def on_cancel_reject(self, api_id: int, msg: UntypedEnvelope) -> None:
        p = msg.payload
        await self.log(
            f"NoopStrategy on_cancel_reject api_id={api_id} "
            f"cid={p.get('client_order_id')} reason={p.get('reason')}",
            level="warn",
        )

    async def on_balance_update(self, api_id: int, msg: UntypedEnvelope) -> None:
        p = msg.payload
        await self.log(
            f"NoopStrategy on_balance_update api_id={api_id} "
            f"{p.get('asset')} free={p.get('free')} locked={p.get('locked')}"
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
