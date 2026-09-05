"""Bitget's private stream — ``wss://ws.bitget.com/v3/ws/private``.

Everything that happens to a UTA account, pushed: orders, fills, balances
and positions. One connection covers every book. Authentication is one
``op: login`` per connection, signed with unix-seconds + ``GET/user/verify``
(V1). It runs in :meth:`_on_open`, so a reconnect re-authenticates before
anything is replayed.

Subscriptions are **shared, not duplicated**. Private topics use
``instType: "UTA"``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from mftik.exchange.bitget import channels as ch
from mftik.exchange.bitget.models import (
    BitgetAccount,
    BitgetFill,
    BitgetOrderUpdate,
    BitgetPosition,
)
from mftik.exchange.bitget.protocol import (
    LOGIN,
    SUBSCRIBE,
    BitgetAuthError,
    BitgetResponse,
    login_frame,
    private_url,
    subscribe_frame,
)
from mftik.exchange.bitget.socket import DEFAULT_PING_INTERVAL, BitgetSocket
from mftik.exchange.stream import EventStream
from mftik.exchange.wire import WireLedger, first_seen

logger = logging.getLogger(__name__)

T = TypeVar("T")
ArgKey = tuple[str, str, str, str]


@dataclass
class _Sub:
    key: ArgKey
    stream: EventStream[Any]
    parse: Callable[[dict[str, Any]], Any]


class BitgetPrivateStream(BitgetSocket):
    """Bitget account pushes, for one UTA credential."""

    name = "bitget.private"

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
            raise BitgetAuthError(
                "api_key, api_secret and passphrase are required; Bitget's "
                "private stream authenticates before it will accept a subscribe"
            )
        self.api_key = api_key
        self._api_secret = api_secret
        self.passphrase = passphrase
        self._subs: list[_Sub] = []
        self._ledger: WireLedger[ArgKey] = WireLedger()
        self._authenticated = False

    @property
    def authenticated(self) -> bool:
        return self._authenticated

    async def _on_open(self) -> None:
        self._authenticated = False
        self._ledger.clear()
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
        self._ledger.clear()
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()

    async def subscribe_orders(self) -> EventStream[BitgetOrderUpdate]:
        return await self._subscribe(ch.orders(), BitgetOrderUpdate.model_validate)

    async def subscribe_fills(self) -> EventStream[BitgetFill]:
        return await self._subscribe(ch.fills(), BitgetFill.model_validate)

    async def subscribe_account(self) -> EventStream[BitgetAccount]:
        return await self._subscribe(ch.account(), BitgetAccount.model_validate)

    async def subscribe_positions(self) -> EventStream[BitgetPosition]:
        return await self._subscribe(ch.positions(), BitgetPosition.model_validate)

    async def _subscribe(
        self, arg: dict[str, Any], parse: Callable[[dict[str, Any]], T]
    ) -> EventStream[T]:
        self._ensure_connected()
        key = ch.arg_key(arg)
        await self._send_subscribe([arg])
        stream: EventStream[T] = EventStream(on_close=self._drop)
        self._subs.append(_Sub(key=key, stream=stream, parse=parse))
        return stream

    async def _send_subscribe(self, args: list[dict[str, Any]]) -> None:
        by_key = {ch.arg_key(item): item for item in args}

        async def send(keys: list[ArgKey]) -> None:
            wanted = [by_key[key] for key in keys]
            frame, req_id = subscribe_frame(wanted)
            await self.request(frame, req_id, op=SUBSCRIBE)

        await self._ledger.acquire([ch.arg_key(item) for item in args], send)

    def _wanted_args(self) -> list[dict[str, Any]]:
        builders = {
            ch.arg_key(ch.orders()): ch.orders(),
            ch.arg_key(ch.fills()): ch.fills(),
            ch.arg_key(ch.account()): ch.account(),
            ch.arg_key(ch.positions()): ch.positions(),
        }
        keys = first_seen(sub.key for sub in self._subs)
        return [builders[key] for key in keys if key in builders]

    def _drop(self, stream: EventStream[Any]) -> None:
        self._subs = [s for s in self._subs if s.stream is not stream]

    def _push(self, resp: BitgetResponse) -> None:
        topic = resp.topic
        rows = resp.rows()
        if not rows:
            logger.debug("%s ignoring push %r", self.name, resp)
            return
        for sub in [s for s in self._subs if s.key[0] == topic]:
            for row in rows:
                try:
                    sub.stream.push(sub.parse(row))
                except Exception:
                    logger.exception(
                        "%s failed to parse %s row: %r",
                        self.name,
                        resp.topic,
                        row,
                    )


__all__ = ["BitgetPrivateStream"]
