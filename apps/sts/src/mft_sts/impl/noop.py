"""Example strategy — no-op that logs lifecycle events."""

from __future__ import annotations

from mft.protocol import ReconDone

from mft_sts.strategy import Strategy


class NoopStrategy(Strategy):
    name = "noop"

    async def on_start(self) -> None:
        await self.log("NoopStrategy started")

    async def on_ready(self) -> None:
        await self.log("NoopStrategy ready")

    async def on_stop(self) -> None:
        await self.log("NoopStrategy stopped")

    async def on_pause(self) -> None:
        await super().on_pause()
        await self.log("NoopStrategy paused")

    async def on_resume(self) -> None:
        await super().on_resume()
        await self.log("NoopStrategy resumed")

    async def on_recon_done(self, msg: ReconDone) -> None:
        view = self.oms.get(msg.api_id)
        n_orders = len(view.orders) if view else 0
        n_balances = len(view.balances) if view else 0
        await self.log(
            f"NoopStrategy recon done api_id={msg.api_id} "
            f"orders={n_orders} balances={n_balances}"
        )
