"""Deribit's private stream — one authenticated JSON-RPC socket.

Everything that happens to the account, pushed: orders, fills, and
per-currency portfolio summaries. One connection covers spot and linear
perps. Authentication is ``public/auth`` ``client_signature`` (V1). It
runs in :meth:`_on_open`, so a reconnect re-authenticates before
anything is replayed.

Positions are **not** streamed. ``user.changes`` would be a second
source next to fills; recon uses ``private/get_positions``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, TypeVar

from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.models import (
    DeribitFill,
    DeribitOrderUpdate,
    DeribitSummary,
)
from mftik.exchange.deribit.protocol import (
    DERIBIT_WS_URL,
    KIND_FUTURE,
    KIND_SPOT,
    DeribitAuthError,
    DeribitResponse,
    auth_params,
    rpc_frame,
)
from mftik.exchange.deribit.socket import DEFAULT_PING_INTERVAL, DeribitSocket
from mftik.exchange.stream import EventStream
from mftik.exchange.wire import WireLedger, first_seen

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class _Sub:
    channels: tuple[str, ...]
    stream: EventStream[Any]
    parse: Callable[[dict[str, Any]], Any]


class DeribitPrivateStream(DeribitSocket):
    """Deribit account pushes, for one HMAC credential."""

    name = "deribit.private"

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        url: str | None = None,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        ping_interval: float = DEFAULT_PING_INTERVAL,
        heartbeat: int = 0,
    ) -> None:
        super().__init__(
            url or DERIBIT_WS_URL,
            ack_timeout=ack_timeout,
            reconnect=reconnect,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            max_retry_backoff=max_retry_backoff,
            ping_interval=ping_interval,
            heartbeat=heartbeat,
        )
        if not api_key or not api_secret:
            raise DeribitAuthError(
                "api_key and api_secret are required; Deribit's private "
                "stream authenticates before it will accept a subscribe"
            )
        self.api_key = api_key
        self._api_secret = api_secret
        self._subs: list[_Sub] = []
        self._ledger: WireLedger[str] = WireLedger()
        self._portfolios: set[str] = set()
        self._authenticated = False

    @property
    def authenticated(self) -> bool:
        return self._authenticated

    async def _on_open(self) -> None:
        self._authenticated = False
        self._ledger.clear()
        params = auth_params(api_key=self.api_key, api_secret=self._api_secret)
        frame, req_id = rpc_frame(ch.PUBLIC_AUTH, params)
        await self.handshake(frame, req_id, op=ch.PUBLIC_AUTH)
        self._authenticated = True
        await self._enable_heartbeat()
        logger.info("%s authenticated key=%s…", self.name, self.api_key[:6])

    async def _restore(self) -> None:
        wanted = self._wanted()
        if not wanted:
            return
        await self._send_subscribe(wanted)
        logger.info("%s resubscribed %s channels", self.name, len(wanted))

    def _teardown(self) -> None:
        self._authenticated = False
        self._ledger.clear()
        self._portfolios.clear()
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()

    async def subscribe_orders(self) -> EventStream[DeribitOrderUpdate]:
        return await self._subscribe(
            (ch.user_orders(KIND_SPOT), ch.user_orders(KIND_FUTURE)),
            DeribitOrderUpdate.model_validate,
        )

    async def subscribe_fills(self) -> EventStream[DeribitFill]:
        return await self._subscribe(
            (ch.user_trades(KIND_SPOT), ch.user_trades(KIND_FUTURE)),
            DeribitFill.model_validate,
        )

    async def subscribe_account(
        self, currencies: Iterable[str] = ()
    ) -> EventStream[DeribitSummary]:
        await self.watch_portfolios(currencies)
        channels = tuple(
            ch.user_portfolio(ccy) for ccy in first_seen(self._portfolios)
        )
        if not channels:
            stream: EventStream[DeribitSummary] = EventStream(on_close=self._drop)
            self._subs.append(
                _Sub(channels=(), stream=stream, parse=DeribitSummary.model_validate)
            )
            return stream
        return await self._subscribe(channels, DeribitSummary.model_validate)

    async def watch_portfolios(self, currencies: Iterable[str]) -> None:
        """Subscribe ``user.portfolio.{ccy}`` for currencies not yet on the wire."""
        new = [
            ccy.upper()
            for ccy in first_seen(c.upper() for c in currencies if c)
            if ccy.upper() not in self._portfolios
        ]
        if not new:
            return
        self._ensure_connected()
        await self._send_subscribe([ch.user_portfolio(ccy) for ccy in new])
        self._portfolios.update(new)

    async def _subscribe(
        self, channels: tuple[str, ...], parse: Callable[[dict[str, Any]], T]
    ) -> EventStream[T]:
        self._ensure_connected()
        await self._send_subscribe(list(channels))
        stream: EventStream[T] = EventStream(on_close=self._drop)
        self._subs.append(_Sub(channels=channels, stream=stream, parse=parse))
        return stream

    async def _send_subscribe(self, channels: list[str]) -> None:
        async def send(keys: list[str]) -> None:
            frame, req_id = rpc_frame(
                ch.PRIVATE_SUBSCRIBE, {"channels": list(keys)}
            )
            await self.request(frame, req_id, op=ch.PRIVATE_SUBSCRIBE)

        await self._ledger.acquire(channels, send)

    def _wanted(self) -> list[str]:
        from_subs = [channel for sub in self._subs for channel in sub.channels]
        from_portfolios = [ch.user_portfolio(ccy) for ccy in self._portfolios]
        return first_seen([*from_subs, *from_portfolios])

    def _drop(self, stream: EventStream[Any]) -> None:
        self._subs = [s for s in self._subs if s.stream is not stream]

    def _push(self, resp: DeribitResponse) -> None:
        key = resp.channel
        rows = resp.rows()
        if not rows:
            logger.debug("%s ignoring push %r", self.name, resp)
            return
        for sub in [s for s in self._subs if key in s.channels]:
            for row in rows:
                try:
                    sub.stream.push(sub.parse(row))
                except Exception:
                    logger.exception(
                        "%s failed to parse %s row: %r",
                        self.name,
                        key,
                        row,
                    )


__all__ = ["DeribitPrivateStream"]
