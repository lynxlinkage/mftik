from __future__ import annotations

from typing import Any

from mft.broker import BrokerClient
from mft.protocol import MessageEnvelope, Topics


class Strategy:
    """Base class for user-defined trading algorithms."""

    name: str = "base"

    def __init__(self, broker: BrokerClient, session_id: str | None = None) -> None:
        self.broker = broker
        self.session_id = session_id
        self._paused = False

    async def on_start(self) -> None:
        """Called when the strategy process starts."""

    async def on_stop(self) -> None:
        """Called when the strategy process is shutting down."""

    async def on_pause(self) -> None:
        self._paused = True

    async def on_resume(self) -> None:
        self._paused = False

    async def on_ticker(self, msg: MessageEnvelope) -> None:
        """Handle ticker updates."""

    async def on_kline(self, msg: MessageEnvelope) -> None:
        """Handle kline / candle updates."""

    async def on_orderbook(self, msg: MessageEnvelope) -> None:
        """Handle order book updates."""

    async def on_trade(self, msg: MessageEnvelope) -> None:
        """Handle public trade prints."""

    async def on_balance_update(self, msg: MessageEnvelope) -> None:
        """Handle account balance updates."""

    async def on_order_update(self, msg: MessageEnvelope) -> None:
        """Handle order status updates."""

    async def on_fill(self, msg: MessageEnvelope) -> None:
        """Handle fill / execution reports."""

    async def log(self, message: str, *, level: str = "info", **extra: Any) -> None:
        if not self.session_id:
            return

        await self.broker.publish(
            Topics.log_session(self.session_id),
            MessageEnvelope(
                type="log",
                source=f"strategy.{self.name}",
                session_id=self.session_id,
                payload={"level": level, "message": message, **extra},
            ),
        )
