"""OKX's private stream — ``wss://ws.okx.com:8443/ws/v5/private``.

Everything that happens to an account, pushed: order lifecycle, fills,
wallet balances and positions. One connection covers **every instType**. A
unified account's spot fills and perp fills arrive on the same socket, which
is the whole point of the venue's shape.

Authentication is one ``op: login`` per connection, signed with HMAC-SHA256
over unix-seconds + ``GET/users/self/verify``. It runs in :meth:`_on_open`,
so a reconnect re-authenticates before anything is replayed.

Subscriptions are **shared, not duplicated**. Two consumers asking for
orders get two :class:`~mftik.exchange.stream.EventStream` s fed by one
subscription at the venue.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from mftik.exchange.okx import channels as ch
from mftik.exchange.okx.models import (
    OkxAccount,
    OkxFill,
    OkxOrderUpdate,
    OkxPosition,
)
from mftik.exchange.okx.protocol import (
    LOGIN,
    SUBSCRIBE,
    OkxAuthError,
    OkxResponse,
    login_frame,
    private_url,
    subscribe_frame,
)
from mftik.exchange.okx.socket import DEFAULT_PING_INTERVAL, OkxSocket
from mftik.exchange.stream import EventStream

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class _Sub:
    key: tuple[str, str, str]
    stream: EventStream[Any]
    parse: Callable[[dict[str, Any]], Any]


class OkxPrivateStream(OkxSocket):
    """OKX account pushes, for one credential."""

    name = "okx.private"

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        passphrase: str,
        url: str | None = None,
        demo: bool = False,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        ping_interval: float = DEFAULT_PING_INTERVAL,
    ) -> None:
        super().__init__(
            url or private_url(demo=demo),
            ack_timeout=ack_timeout,
            reconnect=reconnect,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            max_retry_backoff=max_retry_backoff,
            ping_interval=ping_interval,
        )
        if not api_key or not api_secret or not passphrase:
            raise OkxAuthError(
                "api_key, api_secret and passphrase are required; OKX's "
                "private stream authenticates before it will accept a subscribe"
            )
        self.api_key = api_key
        self._api_secret = api_secret
        self.passphrase = passphrase
        self._subs: list[_Sub] = []
        self._subscribed: set[tuple[str, str, str]] = set()
        self._authenticated = False

    @property
    def authenticated(self) -> bool:
        return self._authenticated

    async def _on_open(self) -> None:
        self._authenticated = False
        self._subscribed.clear()
        frame, req_id = login_frame(
            api_key=self.api_key,
            api_secret=self._api_secret,
            passphrase=self.passphrase,
        )
        await self.handshake(frame, req_id, op=LOGIN)
        self._authenticated = True
        logger.info("%s authenticated key=%s…", self.name, self.api_key[:6])

    async def _restore(self) -> None:
        wanted = self._wanted_args()
        if not wanted:
            return
        await self._send_subscribe(wanted)
        logger.info("%s resubscribed %s channels", self.name, len(wanted))

    def _teardown(self) -> None:
        self._authenticated = False
        self._subscribed.clear()
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()

    async def subscribe_orders(self) -> EventStream[OkxOrderUpdate]:
        return await self._subscribe(ch.orders(), OkxOrderUpdate.model_validate)

    async def subscribe_fills(self) -> EventStream[OkxFill]:
        return await self._subscribe(ch.fills(), OkxFill.model_validate)

    async def subscribe_account(self) -> EventStream[OkxAccount]:
        return await self._subscribe(ch.account(), OkxAccount.model_validate)

    async def subscribe_positions(self) -> EventStream[OkxPosition]:
        return await self._subscribe(ch.positions(), OkxPosition.model_validate)

    async def _subscribe(
        self, arg: dict[str, Any], parse: Callable[[dict[str, Any]], T]
    ) -> EventStream[T]:
        self._ensure_connected()
        key = ch.arg_key(arg)
        if key not in self._subscribed:
            await self._send_subscribe([arg])
        stream: EventStream[T] = EventStream(on_close=self._drop)
        self._subs.append(_Sub(key=key, stream=stream, parse=parse))
        return stream

    async def _send_subscribe(self, args: list[dict[str, Any]]) -> None:
        frame, req_id = subscribe_frame(args)
        await self.request(frame, req_id, op=SUBSCRIBE)
        self._subscribed.update(ch.arg_key(arg) for arg in args)

    def _wanted_args(self) -> list[dict[str, Any]]:
        seen: dict[tuple[str, str, str], dict[str, Any]] = {}
        builders = {
            ch.arg_key(ch.orders()): ch.orders(),
            ch.arg_key(ch.fills()): ch.fills(),
            ch.arg_key(ch.account()): ch.account(),
            ch.arg_key(ch.positions()): ch.positions(),
        }
        for sub in self._subs:
            arg = builders.get(sub.key)
            if arg is not None:
                seen.setdefault(sub.key, arg)
        return list(seen.values())

    def _drop(self, stream: EventStream[Any]) -> None:
        self._subs = [s for s in self._subs if s.stream is not stream]

    def _push(self, resp: OkxResponse) -> None:
        # The subscribe arg is ``instType=ANY``; the push names the book the
        # row actually came from. Route on the channel alone.
        channel = resp.channel
        rows = resp.rows()
        if not rows:
            logger.debug("%s ignoring push %r", self.name, resp)
            return
        for sub in [s for s in self._subs if s.key[0] == channel]:
            for row in rows:
                try:
                    sub.stream.push(sub.parse(row))
                except Exception:
                    logger.exception(
                        "%s failed to parse %s row: %r",
                        self.name,
                        resp.channel,
                        row,
                    )


__all__ = ["OkxPrivateStream"]
