"""The futures account feed — one socket, addressed by a listen key.

Spot subscribes to its user data on the authenticated WebSocket API connection
and is done. Futures has no such method: you ask the WebSocket API for a
**listen key**, open a *second* socket at ``…/private/ws/<listenKey>``, and the
account's events arrive there with no request and no subscribe frame. The key
*is* the subscription, and it is also a credential — anyone holding it reads
the account.

Three consequences shape this class, and each of them is a way the feed dies
quietly if it is not handled:

* **The key expires 60 minutes after its last ping.** :meth:`connect` starts a
  keepalive loop; without it the socket stays open and simply stops carrying
  anything an hour in.
* **Expiry is announced, not signalled.** Binance sends a ``listenKeyExpired``
  event and leaves the connection up. So it is treated as a disconnect here —
  the socket is recycled, which fetches a fresh key on the way back.
* **A reconnect needs a new key.** :meth:`_open` asks for one every time
  rather than reusing the last, because the reason the socket dropped may well
  be that the key it was opened with is gone.

Nothing is *sent* on this connection — there is nothing to send — so the whole
class is a fan-out of typed views over the events that arrive.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from mft.exchange.binance.future import methods as m
from mft.exchange.binance.future.models import (
    BinanceFutureAccountUpdate,
    BinanceFutureOrderTradeUpdate,
)
from mft.exchange.binance.future.protocol import (
    BINANCE_FUTURE_PRIVATE_STREAM_URL,
    BinanceResponse,
    user_stream_url,
)
from mft.exchange.binance.socket import BinanceSocket
from mft.exchange.stream import EventStream

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: How often the listen key is renewed. Binance expires it at 60 minutes and
#: recommends every 30; the margin matters because a missed ping is not an
#: error anywhere — it is a feed that goes quiet.
KEEPALIVE_SECONDS = 30 * 60


@dataclass
class _EventSub:
    """One filtered view of the events arriving on this socket."""

    event_type: str
    stream: EventStream[Any]
    parse: Callable[[dict[str, Any]], Any]


class BinanceFutureUserStream(BinanceSocket):
    """Binance futures user data: order updates, balances and positions.

    Built with *callables* rather than credentials: the listen key is issued
    over the WebSocket API, which the private client already holds
    authenticated, and duplicating the logon here would mean a second
    authenticated connection for no reason.
    """

    name = "binance.future.user"

    def __init__(
        self,
        *,
        start_key: Callable[[], Awaitable[str]],
        ping_key: Callable[[], Awaitable[None]] | None = None,
        base_url: str = BINANCE_FUTURE_PRIVATE_STREAM_URL,
        keepalive_seconds: float = KEEPALIVE_SECONDS,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        keepalive: float = 20.0,
    ) -> None:
        # The URL is not known until a key has been issued; ``_open`` builds it.
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

    # --- lifecycle ---------------------------------------------------------

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
        """Get a listen key, then dial the socket it names.

        A fresh key every time: reusing one across a reconnect saves a round trip
        and risks opening a socket on a key that has already expired, which
        looks exactly like a healthy connection carrying nothing.
        """
        self.listen_key = await self._start_key()
        self.url = user_stream_url(self.listen_key, base=self.base_url)
        await super()._open()

    def _teardown(self) -> None:
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()

    async def _keepalive_loop(self) -> None:
        """Renew the listen key for as long as this socket is up."""
        assert self._ping_key is not None
        while True:
            await asyncio.sleep(self.keepalive_seconds)
            try:
                await self._ping_key()
                logger.debug("%s listen key renewed", self.name)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Not fatal on its own: the key lives another 30 minutes, and
                # the next lap may well succeed. If it does not, the venue
                # announces the expiry and the socket is recycled with a new
                # key — which is the recovery path either way.
                logger.warning("%s listen key renewal failed", self.name, exc_info=True)

    # --- subscriptions -----------------------------------------------------

    async def subscribe_order_updates(
        self,
    ) -> EventStream[BinanceFutureOrderTradeUpdate]:
        """``ORDER_TRADE_UPDATE`` — order lifecycle, and fills along with it."""
        return self._subscribe(
            m.ORDER_TRADE_UPDATE, BinanceFutureOrderTradeUpdate.model_validate
        )

    async def subscribe_account_updates(
        self,
    ) -> EventStream[BinanceFutureAccountUpdate]:
        """``ACCOUNT_UPDATE`` — the balances and positions an event moved."""
        return self._subscribe(
            m.ACCOUNT_UPDATE, BinanceFutureAccountUpdate.model_validate
        )

    def _subscribe(
        self, event_type: str, parse: Callable[[dict[str, Any]], T]
    ) -> EventStream[T]:
        """Register a view. Nothing goes on the wire — the key already asked.

        Which is why this is not an ``await``-ing subscribe like every other
        feed in this codebase: there is no subscribe frame on a futures user
        data socket, and pretending there is one would suggest a round trip
        that could fail.
        """
        self._ensure_connected()
        stream: EventStream[T] = EventStream(on_close=self._drop)
        self._subs.append(
            _EventSub(event_type=event_type, stream=stream, parse=parse)
        )
        return stream

    def _drop(self, stream: EventStream[Any]) -> None:
        self._subs = [s for s in self._subs if s.stream is not stream]

    # --- push routing ------------------------------------------------------

    def _push(self, resp: BinanceResponse) -> None:
        """Route one account event.

        The payload is bare — ``{"e": "ORDER_TRADE_UPDATE", ...}`` — rather
        than wrapped in the ``{"stream", "data"}`` envelope the market sockets
        use, because this connection carries exactly one stream and Binance
        does not name it on each message.
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
        """Drop the connection so the read loop rebuilds it with a new key.

        Closing the socket rather than reopening it inline: the reconnect path
        already knows how to back off, re-announce and replay, and
        :meth:`_open` already asks for a fresh key. Doing it here would be a
        second, less careful copy of that.
        """
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


__all__ = ["KEEPALIVE_SECONDS", "BinanceFutureUserStream"]
