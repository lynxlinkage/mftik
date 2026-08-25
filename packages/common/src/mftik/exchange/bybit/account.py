"""Bybit's private stream — ``wss://stream.bybit.com/v5/private``.

Everything that happens to an account, pushed: order lifecycle, executions,
wallet balances and positions. It is TD's report path, and the reason it is
first among Bybit's three sockets is that an order is not really placed until
this stream says so — :mod:`.trade` acknowledges receipt and nothing more (see
:class:`~mftik.exchange.bybit.models.BybitOrderAck`), so an adapter with order
entry and no private stream would be an adapter that cannot tell whether it
traded.

One connection covers **every category**. A unified account's spot fills and
perp fills arrive on the same socket, which is the whole point of the venue's
shape — and the reason each topic can be scoped: subscribing to ``order.spot``
rather than ``order`` means a spot session is not woken by perp updates it
would only filter out (:func:`~mftik.exchange.bybit.channels.scoped`).

Authentication is one ``op: auth`` per connection, signed with HMAC-SHA256 over
a deadline (:func:`~mftik.exchange.bybit.protocol.auth_frame`). It runs in
:meth:`_on_open`, so a reconnect re-authenticates before anything is replayed,
and a subscribe cannot go out on a socket that is not yet authenticated.

Subscriptions are **shared, not duplicated**. Two consumers asking for orders
get two :class:`~mftik.exchange.stream.EventStream` s fed by one subscription at
the venue, because Bybit answers a second subscribe to a live topic with an
error rather than a second feed.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from mftik.exchange.bybit import channels as ch
from mftik.exchange.bybit.models import (
    BybitExecution,
    BybitOrderUpdate,
    BybitPosition,
    BybitWallet,
)
from mftik.exchange.bybit.protocol import (
    AUTH,
    BYBIT_WS_PRIVATE_URL,
    SUBSCRIBE,
    BybitAuthError,
    BybitResponse,
    auth_frame,
    subscribe_frame,
)
from mftik.exchange.bybit.socket import DEFAULT_PING_INTERVAL, BybitSocket
from mftik.exchange.stream import EventStream
from mftik.exchange.wire import WireLedger, first_seen

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class _Sub:
    """One consumer's view of a topic: what it wants and how to parse it."""

    topic: str
    #: What the topic is called on the wire once scoped — ``order.spot``. Sent
    #: to the venue and replayed on reconnect; pushes come back under
    #: :attr:`topic` either way, which is what routing uses.
    wire_topic: str
    stream: EventStream[Any]
    parse: Callable[[dict[str, Any]], Any]


class BybitPrivateStream(BybitSocket):
    """Bybit account pushes, for one credential.

    ::

        stream = BybitPrivateStream(api_key=k, api_secret=s, product="spot")
        async with stream:
            orders = await stream.subscribe_orders()
            fills = await stream.subscribe_executions()
            async for order in orders:
                ...

    ``product`` scopes the order, execution and position topics to one of
    Bybit's books. ``None`` subscribes unscoped and receives every category on
    the account, which is what a process trading more than one book wants.
    """

    name = "bybit.private"

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        url: str = BYBIT_WS_PRIVATE_URL,
        product: str | None = None,
        auth_window_ms: int | None = None,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        ping_interval: float = DEFAULT_PING_INTERVAL,
    ) -> None:
        super().__init__(
            url,
            ack_timeout=ack_timeout,
            reconnect=reconnect,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            max_retry_backoff=max_retry_backoff,
            ping_interval=ping_interval,
        )
        if not api_key or not api_secret:
            raise BybitAuthError(
                "api_key and api_secret are required; Bybit's private stream "
                "authenticates before it will accept a subscribe"
            )
        self.api_key = api_key
        self._api_secret = api_secret
        self.product = product
        self.auth_window_ms = auth_window_ms
        self._subs: list[_Sub] = []
        #: Wire identities this socket has reserved or acked. Cleared on a
        #: drop, because a fresh connection is subscribed to nothing.
        self._ledger: WireLedger[str] = WireLedger()
        self._authenticated = False

    # --- lifecycle ---------------------------------------------------------

    @property
    def authenticated(self) -> bool:
        """Whether ``op: auth`` has succeeded on the current socket."""
        return self._authenticated

    async def _on_open(self) -> None:
        self._authenticated = False
        self._ledger.clear()
        kwargs: dict[str, Any] = {}
        if self.auth_window_ms is not None:
            kwargs["window_ms"] = self.auth_window_ms
        frame, req_id = auth_frame(
            api_key=self.api_key, api_secret=self._api_secret, **kwargs
        )
        await self.handshake(frame, req_id, op=AUTH)
        self._authenticated = True
        logger.info("%s authenticated key=%s…", self.name, self.api_key[:6])

    async def _restore(self) -> None:
        """Re-subscribe every topic something is still reading.

        One frame for all of them: Bybit takes a list, and a rejection that
        applies to one topic fails the batch loudly rather than leaving a feed
        silently dead. Topics nothing reads any more are not replayed — a
        socket carrying a firehose no consumer drains is worse than one that
        carries nothing.
        """
        wanted = self._wanted()
        if not wanted:
            return
        await self._send_subscribe(wanted)
        logger.info("%s resubscribed %s topics", self.name, len(wanted))

    def _teardown(self) -> None:
        self._authenticated = False
        self._ledger.clear()
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()

    # --- subscriptions -----------------------------------------------------

    async def subscribe_orders(self) -> EventStream[BybitOrderUpdate]:
        """``order`` — every state change, for orders placed anywhere.

        Includes orders this session did not place: Bybit reports the account,
        not the connection, so a fill on an order entered from the web UI
        arrives here too.
        """
        return await self._subscribe(ch.ORDER, BybitOrderUpdate.model_validate)

    async def subscribe_executions(self) -> EventStream[BybitExecution]:
        """``execution`` — fills, and the account movements that are not fills.

        Funding, ADL and delivery come down this topic as well; only a row with
        ``execType == "Trade"`` is an execution against an order. See
        :attr:`~mftik.exchange.bybit.models.BybitExecution.is_fill`.
        """
        return await self._subscribe(ch.EXECUTION, BybitExecution.model_validate)

    async def subscribe_wallets(self) -> EventStream[BybitWallet]:
        """``wallet`` — the balance sheet, as whole snapshots.

        Never scoped: a unified account has one wallet across every book.
        """
        return await self._subscribe(ch.WALLET, BybitWallet.model_validate)

    async def subscribe_positions(self) -> EventStream[BybitPosition]:
        """``position`` — open contracts. Silent on a spot-only account."""
        return await self._subscribe(ch.POSITION, BybitPosition.model_validate)

    async def subscribe_raw(self, topic: str) -> EventStream[dict[str, Any]]:
        """Subscribe by topic name and yield the raw rows.

        The escape hatch for topics not modelled here — ``greeks``, or
        ``execution.fast`` for a caller that wants latency over the fee field.
        """
        return await self._subscribe(topic, lambda row: row)

    async def _subscribe(
        self, topic: str, parse: Callable[[dict[str, Any]], T]
    ) -> EventStream[T]:
        self._ensure_connected()
        wire_topic = ch.scoped(topic, self.product)
        # Only if this socket is not already carrying it: Bybit refuses a
        # duplicate subscribe, and a second consumer of orders should get a
        # second stream rather than an error. Reservation happens inside
        # the ledger so two concurrent callers send one frame.
        await self._send_subscribe([wire_topic])
        stream: EventStream[T] = EventStream(on_close=self._drop)
        self._subs.append(
            _Sub(topic=topic, wire_topic=wire_topic, stream=stream, parse=parse)
        )
        return stream

    async def _send_subscribe(self, topics: list[str]) -> None:
        async def send(missing: list[str]) -> None:
            frame, req_id = subscribe_frame(list(missing))
            await self.request(frame, req_id, op=SUBSCRIBE)

        await self._ledger.acquire(topics, send)

    def _wanted(self) -> list[str]:
        """Wire topics with at least one live consumer, in subscribe order."""
        return first_seen(sub.wire_topic for sub in self._subs)

    def _drop(self, stream: EventStream[Any]) -> None:
        """Forget a closed consumer's stream.

        The venue-side subscription is left in place. Unsubscribing when the
        last reader of a topic goes away would be tidier and is not worth the
        race: TD closes and reopens the order stream around a reconnect, and a
        socket that had unsubscribed in between would miss whatever arrived in
        the gap. What an idle topic costs is bandwidth on a socket that is open
        anyway.
        """
        self._subs = [s for s in self._subs if s.stream is not stream]

    # --- push routing ------------------------------------------------------

    def _push(self, resp: BybitResponse) -> None:
        topic = ch.base_topic(resp.topic)
        rows = resp.rows()
        if not rows:
            logger.debug("%s ignoring push %r", self.name, resp)
            return
        for sub in [s for s in self._subs if s.topic == topic]:
            for row in rows:
                try:
                    sub.stream.push(sub.parse(row))
                except Exception:
                    logger.exception(
                        "%s failed to parse %s row: %r", self.name, resp.topic, row
                    )


__all__ = ["BybitPrivateStream"]
