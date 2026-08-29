"""The COIN-M account feed — one socket, addressed by a listen key.

Spot subscribes to its user data on the authenticated WebSocket API
connection and is done. COIN-M has no such method: you ask the WebSocket
API for a **listen key**, open a *second* socket at
``dstream.binance.com/ws/<listenKey>``, and the account's events arrive
there with no request and no subscribe frame. The key *is* the
subscription, and it is also a credential.

dapi was not part of the 2026 ``/private`` split; the path is still
``/ws/<listenKey>``, not ``/private/ws``.

Three consequences shape this class:

* **The key expires 60 minutes after its last ping.**
* **Expiry is announced, not signalled.** Binance sends
  ``listenKeyExpired`` and leaves the connection up.
* **A reconnect needs a new key.**
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from mftik.exchange.binance.delivery import methods as m
from mftik.exchange.binance.delivery.models import (
    BinanceDeliveryAccountUpdate,
    BinanceDeliveryOrderTradeUpdate,
)
from mftik.exchange.binance.delivery.protocol import (
    BINANCE_DELIVERY_PRIVATE_STREAM_URL,
    BinanceResponse,
    user_stream_url,
)
from mftik.exchange.binance.socket import BinanceSocket
from mftik.exchange.stream import EventStream

logger = logging.getLogger(__name__)

T = TypeVar("T")

KEEPALIVE_SECONDS = 30 * 60


@dataclass
class _EventSub:
    """One filtered view of the events arriving on this socket."""

    event_type: str
    stream: EventStream[Any]
    parse: Callable[[dict[str, Any]], Any]


class BinanceDeliveryUserStream(BinanceSocket):
    """Binance COIN-M user data: order updates, balances and positions.

    Built with *callables* rather than credentials: the listen key is
    issued over the WebSocket API the private client already holds.
    """

    name = "binance.delivery.user"

    def __init__(
        self,
        *,
        start_key: Callable[[], Awaitable[str]],
        ping_key: Callable[[], Awaitable[None]] | None = None,
        base_url: str = BINANCE_DELIVERY_PRIVATE_STREAM_URL,
        keepalive_seconds: float = KEEPALIVE_SECONDS,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        keepalive: float = 20.0,
    ) -> None:
        super().__init__(
            base_url,
            ack_timeout=ack_timeout,
            reconnect=reconnect,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            max_retry_backoff=max_retry_backoff,
            keepalive=keepalive,
        )
        self.base_url = base_url
        self._start_key = start_key
        self._ping_key = ping_key
        self.keepalive_seconds = keepalive_seconds
        self.listen_key = ""
        self._subs: list[_EventSub] = []
        self._keepalive_task: asyncio.Task[None] | None = None
        self._recycling = False

    async def connect(self) -> None:
        await super().connect()
        if self._ping_key is not None and self._keepalive_task is None:
            self._keepalive_task = asyncio.create_task(
                self._keepalive_loop(), name=f"{self.name}-keepalive"
            )

    async def close(self) -> None:
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            await asyncio.gather(self._keepalive_task, return_exceptions=True)
            self._keepalive_task = None
        await super().close()

    async def _open(self) -> None:
        self.listen_key = await self._start_key()
        self.url = user_stream_url(self.listen_key, base=self.base_url)
        await super()._open()

    def _teardown(self) -> None:
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()

    async def _keepalive_loop(self) -> None:
        assert self._ping_key is not None
        while True:
            await asyncio.sleep(self.keepalive_seconds)
            try:
                await self._ping_key()
                logger.debug("%s listen key renewed", self.name)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("%s listen key renewal failed", self.name, exc_info=True)

    async def subscribe_order_updates(
        self,
    ) -> EventStream[BinanceDeliveryOrderTradeUpdate]:
        """``ORDER_TRADE_UPDATE`` — order lifecycle, and fills along with it."""
        return self._subscribe(
            m.ORDER_TRADE_UPDATE, BinanceDeliveryOrderTradeUpdate.model_validate
        )

    async def subscribe_account_updates(
        self,
    ) -> EventStream[BinanceDeliveryAccountUpdate]:
        """``ACCOUNT_UPDATE`` — the balances and positions an event moved."""
        return self._subscribe(
            m.ACCOUNT_UPDATE, BinanceDeliveryAccountUpdate.model_validate
        )

    def _subscribe(
        self, event_type: str, parse: Callable[[dict[str, Any]], T]
    ) -> EventStream[T]:
        self._ensure_connected()
        stream: EventStream[T] = EventStream(on_close=self._drop)
        self._subs.append(
            _EventSub(event_type=event_type, stream=stream, parse=parse)
        )
        return stream

    def _drop(self, stream: EventStream[Any]) -> None:
        self._subs = [s for s in self._subs if s.stream is not stream]

    def _push(self, resp: BinanceResponse) -> None:
        """Route one account event.

        The payload is bare — ``{"e": "ORDER_TRADE_UPDATE", ...}`` — rather
        than wrapped in the ``{"stream", "data"}`` envelope the market
        sockets use.
        """
        event = resp.raw
        event_type = str(event.get("e") or "")
        if not event_type:
            logger.debug("%s ignoring push %r", self.name, resp)
            return
        if event_type == m.LISTEN_KEY_EXPIRED:
            logger.warning("%s listen key expired; reconnecting", self.name)
            self._recycle()
            return
        for sub in [s for s in self._subs if s.event_type == event_type]:
            try:
                sub.stream.push(sub.parse(event))
            except Exception:
                logger.exception(
                    "%s failed to parse %s event: %r", self.name, event_type, event
                )

    def _recycle(self) -> None:
        conn = self._conn
        if conn is None or self._recycling:
            return
        self._recycling = True

        async def _drop_connection() -> None:
            try:
                with contextlib.suppress(Exception):
                    await conn.close()
            finally:
                self._recycling = False

        asyncio.create_task(_drop_connection(), name=f"{self.name}-recycle")


__all__ = ["KEEPALIVE_SECONDS", "BinanceDeliveryUserStream"]
