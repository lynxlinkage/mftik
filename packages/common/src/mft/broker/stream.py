"""Bidirectional stream — duplex channel composed of pub + sub."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from mft.protocol import Envelope, UntypedEnvelope

if TYPE_CHECKING:
    from mft.broker.client import Broker


class BidirectionalStream:
    """Duplex IPC stream: publish on ``tx``, subscribe on ``rx``.

    Two peers exchange roles of the same topic pair::

        # peer A
        a = broker.bistream(tx="session.1.up", rx="session.1.down")
        # peer B
        b = broker.bistream(tx="session.1.down", rx="session.1.up")

        await a.send(envelope)
        async for msg in b:  # receives what A sent
            ...
    """

    def __init__(
        self,
        broker: Broker,
        *,
        tx: str,
        rx: str,
    ) -> None:
        if not tx or not rx:
            raise ValueError("tx and rx topics are required")
        if tx == rx:
            raise ValueError("tx and rx must be different topics")
        self._broker = broker
        self.tx = tx
        self.rx = rx
        self._stop = asyncio.Event()
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def send(self, envelope: Envelope[Any]) -> int:
        """Publish an envelope on the outbound (tx) topic."""
        if self._closed:
            raise RuntimeError("BidirectionalStream is closed")
        return await self._broker.publish(self.tx, envelope)

    def __aiter__(self) -> AsyncIterator[UntypedEnvelope]:
        """Iterate inbound envelopes from the rx topic until closed."""
        if self._closed:
            raise RuntimeError("BidirectionalStream is closed")
        return self._broker.subscribe(self.rx, stop=self._stop)

    async def recv(self) -> UntypedEnvelope:
        """Receive the next inbound envelope (blocks until one arrives)."""
        async for envelope in self:
            return envelope
        raise RuntimeError("BidirectionalStream closed before receiving")

    def close(self) -> None:
        """Stop the inbound subscription loop."""
        self._closed = True
        self._stop.set()

    async def __aenter__(self) -> BidirectionalStream:
        return self

    async def __aexit__(self, *args: object) -> None:
        self.close()

    def peer(self) -> BidirectionalStream:
        """Return the complementary stream (tx/rx swapped) on the same broker."""
        return BidirectionalStream(self._broker, tx=self.rx, rx=self.tx)

    @staticmethod
    def topics(name: str) -> tuple[str, str]:
        """Canonical topic pair for a named bistream: ``(up, down)``."""
        return f"bistream.{name}.up", f"bistream.{name}.down"
