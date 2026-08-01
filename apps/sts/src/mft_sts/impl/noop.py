"""Example strategy — timer-driven place/cancel demo, then natural exit."""

from __future__ import annotations

from decimal import Decimal

from mft.exchange.models import Balance, OrderType, Side
from mft.protocol import ReconDone, UntypedEnvelope

from mft_sts.strategy import Strategy
from mft_sts.timer import TimerToken

_SYMBOL = "BTCUSDT"
_QTY = Decimal("1")
_DEFAULT_MID = Decimal("50000")  # paper BTCUSDT default
_TICK_MS = 1000


class NoopStrategy(Strategy):
    name = "noop"
    id = 1

    def __init__(self) -> None:
        super().__init__()
        self._tick_token: TimerToken | None = None
        self._step = 0
        self._last_cid: str | None = None
        self._plan: list[tuple[str, Side | None, Decimal | None]] = []

    def validate_paras(self, paras: dict) -> dict:
        out = dict(paras)
        mid = Decimal(str(out.get("mid", _DEFAULT_MID)))
        out["mid"] = mid
        return out

    async def on_start(self) -> None:
        mid = Decimal(str(self.paras.get("mid", _DEFAULT_MID)))
        prices = (mid - 1, mid, mid + 1)
        self._plan = []
        for side in (Side.BUY, Side.SELL):
            for price in prices:
                self._plan.append(("place", side, price))
                self._plan.append(("cancel", None, None))
        await self.log(
            f"NoopStrategy started mid={mid} prices={list(prices)}"
        )
        self._tick_token = self.timer.token()
        self._arm_timer()

    async def on_ready(self) -> None:
        await self.log("NoopStrategy ready")

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

    async def on_recon_done(self, msg: ReconDone) -> None:
        local = self.oms.get(msg.api_id)
        recon_bals = dict(msg.oms.balances)
        local_bals = dict(local.balances) if local is not None else {}

        def _bal_key(bals: dict[str, Balance]) -> dict[str, tuple[str, str]]:
            return {
                a: (str(b.free), str(b.locked))
                for a, b in sorted(bals.items())
            }

        same = _bal_key(local_bals) == _bal_key(recon_bals)
        await self.log(
            f"NoopStrategy recon done api_id={msg.api_id} "
            f"orders={len(msg.oms.orders)} "
            f"balances={ {a: str(b.free) for a, b in recon_bals.items()} }"
        )
        if same:
            await self.log(
                "NoopStrategy OMS balances match ReconDone snapshot"
            )
        else:
            await self.log(
                f"NoopStrategy OMS balance mismatch "
                f"local={ {a: str(b.free) for a, b in local_bals.items()} } "
                f"recon={ {a: str(b.free) for a, b in recon_bals.items()} }",
                level="warn",
            )

    async def on_order_update(self, api_id: int, msg: UntypedEnvelope) -> None:
        p = msg.payload
        await self.log(
            f"NoopStrategy on_order_update api_id={api_id} "
            f"cid={p.get('client_order_id')} status={p.get('status')} "
            f"{p.get('side')} {p.get('symbol')} {p.get('price')}@{p.get('qty')}"
        )

    async def on_fill(self, api_id: int, msg: UntypedEnvelope) -> None:
        p = msg.payload
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

    def _arm_timer(self) -> None:
        if self._tick_token is None:
            return
        self._tick_token.register(
            self.timer.now_ms() + 3 * _TICK_MS,
            _TICK_MS,
            self._on_tick,
        )

    def _cancel_timer(self) -> None:
        if self._tick_token is not None:
            self._tick_token.cancel()

    async def _on_tick(self) -> None:
        if self._step >= len(self._plan):
            await self.log("NoopStrategy sequence complete — exiting")
            self.exit("noop_sequence_done")
            return

        api_id = self._primary_api_id()
        if api_id is None:
            await self.log(
                "NoopStrategy has no TD api_id — exiting", level="warn"
            )
            self.exit("noop_no_td")
            return

        action, side, price = self._plan[self._step]
        self._step += 1

        if action == "place":
            assert side is not None and price is not None
            cid = await self.oms.submit_order(
                api_id,
                symbol=_SYMBOL,
                side=side,
                qty=_QTY,
                type=OrderType.LIMIT,
                price=price,
            )
            self._last_cid = cid
            await self.log(
                f"NoopStrategy PLACE {side.value.upper()} {_SYMBOL} "
                f"{price}@{_QTY} cid={cid}"
            )
        else:
            cid = self._last_cid
            if cid is None:
                await self.log(
                    "NoopStrategy CANCEL skipped (no open cid)", level="warn"
                )
            else:
                await self.oms.cancel_order(api_id, cid)
                await self.log(f"NoopStrategy CANCEL cid={cid}")
                self._last_cid = None

        if self._step >= len(self._plan):
            await self.log("NoopStrategy sequence complete — exiting")
            self.exit("noop_sequence_done")

    def _primary_api_id(self) -> int | None:
        if self.session is None or not self.session.td_api_ids:
            return None
        return self.session.td_api_ids[0]
