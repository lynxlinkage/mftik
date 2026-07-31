"""Example strategy — no-op that logs lifecycle events."""

from __future__ import annotations

from mft.protocol import UntypedEnvelope
from mft.strategy import Strategy


class NoopStrategy(Strategy):
    name = "noop"

    async def on_start(self) -> None:
        await self.log("NoopStrategy started")

    async def on_stop(self) -> None:
        await self.log("NoopStrategy stopped")

    async def on_ticker(self, msg: UntypedEnvelope) -> None:
        await self.log(f"ticker: {msg.payload}", level="debug")
