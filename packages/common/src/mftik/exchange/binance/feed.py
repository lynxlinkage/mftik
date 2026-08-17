"""Subscription plumbing for Binance's market-stream sockets.

A market-stream socket is a :class:`~mftik.exchange.binance.socket.BinanceSocket`
that never asks anything except ``SUBSCRIBE``, and whose pushes arrive as
``{"stream": <name>, "data": {...}}``. That much is the same on spot and on
futures, so the subscribe/replay/fan-out half lives here and each product adds
only its typed ``subscribe_*`` methods and its own stream-name vocabulary.

Always the **combined** endpoint. Binance offers a raw one where the payload
arrives bare, and it is unusable for anything with more than a single
subscription on spot: partial depth carries no symbol at all, so telling two
symbols' books apart would mean one socket per symbol. The combined endpoint
wraps every push with the stream name — and with it the symbol, the window and
the update speed — on every message.

One ``subscribe_*`` call yields one stream carrying everything the named
streams push. Binance multiplexes per stream name rather than per symbol, so
listing several symbols in one call is cheaper than opening a stream each, and
each message says which symbol it is.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from mftik.exchange.binance.protocol import BinanceResponse, subscribe_frame
from mftik.exchange.binance.socket import BinanceSocket
from mftik.exchange.stream import EventStream

logger = logging.getLogger(__name__)

T = TypeVar("T")

SUBSCRIBE = "SUBSCRIBE"
UNSUBSCRIBE = "UNSUBSCRIBE"
LIST_SUBSCRIPTIONS = "LIST_SUBSCRIPTIONS"

#: ``(stream name, payload) -> model``. The name is passed because a payload
#: does not always carry its own symbol — spot's partial depth carries none —
#: and because the name is the only place the requested window and speed appear.
Parse = Callable[[str, dict[str, Any]], Any]


@dataclass
class _Sub:
    """One live subscribe call: its stream names, for replay, and its output."""

    names: tuple[str, ...]
    stream: EventStream[Any]
    parse: Parse
    #: Set of names, for the per-message routing test.
    index: frozenset[str] = field(default_factory=frozenset)


class BinanceStreamSocket(BinanceSocket):
    """One combined market-stream connection and the subscriptions on it.

    Public data only: Binance keeps its market pushes on hosts with no
    credential anywhere in the protocol, so this socket has no notion of an
    account and never grows one.
    """

    name = "binance.stream"

    def __init__(
        self,
        url: str,
        *,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        keepalive: float = 20.0,
    ) -> None:
        super().__init__(
            url,
            ack_timeout=ack_timeout,
            reconnect=reconnect,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            max_retry_backoff=max_retry_backoff,
            keepalive=keepalive,
        )
        self._subs: list[_Sub] = []

    # --- raw plumbing ------------------------------------------------------

    async def subscribe_raw(self, *names: str) -> EventStream[dict[str, Any]]:
        """Subscribe by stream name and yield the raw ``data`` payloads."""
        return await self.subscribe(names, lambda _name, row: row)

    async def unsubscribe(self, *names: str) -> None:
        """Unsubscribe stream names and close every stream reading them."""
        frame, req_id = subscribe_frame(UNSUBSCRIBE, list(names))
        await self.request(frame, req_id, method=UNSUBSCRIBE)
        wanted = frozenset(names)
        for sub in [s for s in self._subs if s.index & wanted]:
            # close() fires on_close → _drop, which does the removal.
            sub.stream.close()

    async def list_subscriptions(self) -> list[str]:
        """What this socket is currently subscribed to, per Binance."""
        frame, req_id = subscribe_frame(LIST_SUBSCRIPTIONS, [])
        resp = await self.request(frame, req_id, method=LIST_SUBSCRIPTIONS)
        return list(resp.result or [])

    async def subscribe(
        self,
        names: tuple[str, ...],
        parse: Parse,
    ) -> EventStream[T]:
        """Subscribe to stream names and route their pushes through ``parse``.

        The one entry point every typed ``subscribe_*`` is built on, here and
        in the classes that own several of these sockets at once.
        """
        frame, req_id = subscribe_frame(SUBSCRIBE, list(names))
        await self.request(frame, req_id, method=SUBSCRIBE)
        stream: EventStream[T] = EventStream(on_close=self._drop)
        self._subs.append(
            _Sub(
                names=names,
                stream=stream,
                parse=parse,
                index=frozenset(names),
            )
        )
        return stream

    def _drop(self, stream: EventStream[Any]) -> None:
        self._subs = [s for s in self._subs if s.stream is not stream]

    # --- socket hooks ------------------------------------------------------

    async def _restore(self) -> None:
        """Re-send every live subscription onto the fresh socket.

        One frame for all of them: Binance takes a list, and a rejection that
        applies to one name fails the batch loudly rather than leaving a feed
        silently dead. The ack is awaited — :meth:`.socket.BinanceSocket.request`
        reads it inline here, because the read loop has not resumed yet.
        """
        names = [name for sub in self._subs for name in sub.names]
        if not names:
            return
        frame, req_id = subscribe_frame(SUBSCRIBE, names)
        await self.request(frame, req_id, method=SUBSCRIBE)
        logger.info("%s resubscribed %s streams", self.name, len(names))

    def _push(self, resp: BinanceResponse) -> None:
        if not resp.stream or not isinstance(resp.data, dict):
            logger.debug("%s ignoring push %r", self.name, resp)
            return
        for sub in [s for s in self._subs if resp.stream in s.index]:
            try:
                sub.stream.push(sub.parse(resp.stream, resp.data))
            except Exception:
                logger.exception(
                    "%s failed to parse %s payload: %r",
                    self.name,
                    resp.stream,
                    resp.data,
                )

    def _teardown(self) -> None:
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()


__all__ = [
    "LIST_SUBSCRIPTIONS",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "BinanceStreamSocket",
    "Parse",
]
