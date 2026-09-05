"""The connection machinery Deribit's public and private sockets share.

Both take JSON-RPC 2.0 frames. Replies correlate on ``id``. Pushes are
``{"method": "subscription", "params": {"channel", "data"}}``. Heartbeats
are ``public/set_heartbeat`` plus a ``public/test`` reply to each
``test_request``.

**The heartbeat is not optional.** Deribit closes a connection that does
not answer ``test_request``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.protocol import (
    DeribitResponse,
    DeribitWsError,
    rpc_frame,
)
from mftik.exchange.errors import ExchangeNotConnectedError

logger = logging.getLogger(__name__)

DEFAULT_PING_INTERVAL = 15.0
DEFAULT_HEARTBEAT = 15


@dataclass
class _Pending:
    future: asyncio.Future[DeribitResponse]


@dataclass
class _Stats:
    reconnects: int = 0
    frames: int = 0
    heartbeats: int = 0
    last_frame_at: float = field(default_factory=float)


class DeribitSocket:
    """A reconnecting JSON-RPC WebSocket with id correlation."""

    name = "deribit"

    def __init__(
        self,
        url: str,
        *,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        ping_interval: float = DEFAULT_PING_INTERVAL,
        heartbeat: int = DEFAULT_HEARTBEAT,
        close_timeout: float = 2.0,
    ) -> None:
        self.url = url
        self.ack_timeout = ack_timeout
        self.reconnect = reconnect
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.max_retry_backoff = max_retry_backoff
        self.ping_interval = ping_interval
        self.heartbeat = heartbeat
        self.close_timeout = close_timeout

        self._conn: ClientConnection | None = None
        self._pending: dict[str, _Pending] = {}
        self._task: asyncio.Task[Any] | None = None
        self._watch_task: asyncio.Task[Any] | None = None
        self._connected = False
        self._closing = False
        self._pumping = False
        self._reconnect_cbs: list[Callable[[], Any]] = []
        self.stats = _Stats()

    @property
    def connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        if self._connected:
            return
        self._closing = False
        try:
            await self._open()
            await self._on_open()
            self._pumping = True
            self._task = asyncio.create_task(
                self._read_loop(), name=f"{self.name}-read"
            )
            self._connected = True
            if self.ping_interval > 0:
                self._watch_task = asyncio.create_task(
                    self._watchdog(), name=f"{self.name}-watch"
                )
        except Exception:
            await self.close()
            raise

    async def close(self) -> None:
        self._closing = True
        self._connected = False
        for task in (self._task, self._watch_task):
            if task is not None:
                task.cancel()
        tasks = [t for t in (self._task, self._watch_task) if t is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._task = None
        self._watch_task = None
        for pending in self._pending.values():
            if not pending.future.done():
                pending.future.cancel()
        self._pending.clear()
        self._teardown()
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> DeribitSocket:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    def on_reconnect(self, callback: Callable[[], Any]) -> None:
        self._reconnect_cbs.append(callback)

    def _fire_reconnect(self) -> None:
        for cb in list(self._reconnect_cbs):
            try:
                result = cb()
            except Exception:
                logger.exception("%s reconnect callback failed", self.name)
                continue
            if asyncio.iscoroutine(result):
                asyncio.create_task(result, name=f"{self.name}-reconnect-cb")

    async def _open(self) -> None:
        self._conn = await connect(
            self.url, ping_interval=None, close_timeout=self.close_timeout
        )

    def _ensure_connected(self) -> None:
        if not self._connected or self._conn is None:
            raise ExchangeNotConnectedError(
                f"{self.name} is not connected; call connect() first"
            )

    async def send(self, frame: dict[str, Any] | str) -> None:
        self._ensure_connected()
        assert self._conn is not None
        payload = frame if isinstance(frame, str) else json.dumps(frame)
        await self._conn.send(payload)

    async def request(
        self,
        frame: dict[str, Any],
        req_id: str,
        *,
        op: str = "",
        timeout: float | None = None,
    ) -> DeribitResponse:
        if self._conn is None:
            raise ExchangeNotConnectedError(
                f"{self.name} is not connected; call connect() first"
            )
        if not self._pumping:
            return await self.handshake(frame, req_id, op=op)
        self._ensure_connected()
        wait = timeout or self.ack_timeout
        loop = asyncio.get_running_loop()
        pending = _Pending(future=loop.create_future())
        self._pending[req_id] = pending
        try:
            await self.send(frame)
            resp = await asyncio.wait_for(pending.future, timeout=wait)
        except TimeoutError as exc:
            raise DeribitWsError(None, f"no reply within {wait}s", op=op) from exc
        finally:
            self._pending.pop(req_id, None)
        resp.raise_for_error()
        return resp

    async def handshake(
        self,
        frame: dict[str, Any],
        req_id: str,
        *,
        op: str = "",
    ) -> DeribitResponse:
        if self._conn is None:
            raise ExchangeNotConnectedError(f"{self.name} has no live socket")
        await self._conn.send(json.dumps(frame))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.ack_timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise DeribitWsError(
                    None,
                    f"no reply to {op or 'handshake'} within {self.ack_timeout}s",
                    op=op,
                )
            raw = await asyncio.wait_for(self._conn.recv(), timeout=remaining)
            message = self._decode(raw)
            if message is None:
                continue
            resp = DeribitResponse(message)
            if resp.is_test_request:
                await self._answer_heartbeat()
                continue
            if resp.req_id != req_id:
                logger.debug("%s dropping pre-handshake frame %r", self.name, resp)
                continue
            resp.raise_for_error()
            return resp

    async def rpc(
        self, method: str, params: dict[str, Any] | None = None
    ) -> Any:
        frame, req_id = rpc_frame(method, params)
        resp = await self.request(frame, req_id, op=method)
        return resp.result

    async def _read_loop(self) -> None:
        retries = 0
        while not self._closing:
            reason: object
            try:
                self._pumping = True
                await self._pump()
                reason = "server closed the connection"
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                reason = exc
            finally:
                self._pumping = False
            if self._closing:
                return
            if not self.reconnect:
                logger.warning("%s connection lost: %s", self.name, reason)
                self._fail()
                return
            retries += 1
            if 0 <= self.max_retries < retries:
                logger.error(
                    "%s giving up after %s reconnect attempts", self.name, retries
                )
                self._fail()
                return
            delay = min(
                self.retry_backoff * (2 ** (retries - 1)), self.max_retry_backoff
            )
            logger.warning(
                "%s reconnecting in %.1fs (attempt %s): %s",
                self.name,
                delay,
                retries,
                reason,
            )
            await asyncio.sleep(delay)
            try:
                await self._open()
                await self._on_open()
                await self._restore()
            except Exception:
                logger.exception("%s reconnect failed", self.name)
                continue
            self.stats.reconnects += 1
            self._fire_reconnect()
            retries = 0

    async def _pump(self) -> None:
        assert self._conn is not None
        async for raw in self._conn:
            message = self._decode(raw)
            if message is None:
                continue
            self._dispatch(DeribitResponse(message))

    async def _watchdog(self) -> None:
        grace = min(max(self.ping_interval, 1.0) * 3, 45.0)
        while not self._closing:
            await asyncio.sleep(max(self.ping_interval, 0.1))
            if self._closing or not self._connected:
                continue
            silent = asyncio.get_running_loop().time() - self.stats.last_frame_at
            if silent <= grace:
                continue
            conn = self._conn
            logger.warning(
                "%s answered no frame within %.0fs; dropping the socket",
                self.name,
                grace,
            )
            if conn is not None:
                with contextlib.suppress(Exception):
                    await conn.close()

    def _decode(self, raw: str | bytes) -> dict[str, Any] | None:
        self.stats.frames += 1
        self.stats.last_frame_at = asyncio.get_running_loop().time()
        text = raw.decode() if isinstance(raw, bytes) else raw
        try:
            message = json.loads(text)
        except ValueError:
            logger.warning("%s non-JSON frame: %r", self.name, text[:200])
            return None
        return message if isinstance(message, dict) else None

    def _dispatch(self, resp: DeribitResponse) -> None:
        if resp.is_test_request:
            self.stats.heartbeats += 1
            asyncio.create_task(
                self._answer_heartbeat(), name=f"{self.name}-heartbeat"
            )
            return
        if resp.is_heartbeat:
            return
        if resp.is_reply:
            pending = self._pending.get(resp.req_id or "")
            if pending is not None:
                if not pending.future.done():
                    pending.future.set_result(resp)
                return
            if not resp.success:
                logger.warning("%s unsolicited error: %s", self.name, resp.msg)
                return
            logger.debug("%s unmatched reply %r", self.name, resp)
            return
        if resp.is_push:
            self._push(resp)
            return
        logger.debug("%s ignoring frame %r", self.name, resp)

    async def _answer_heartbeat(self) -> None:
        if self._conn is None or self._closing:
            return
        frame, _ = rpc_frame(ch.PUBLIC_TEST)
        with contextlib.suppress(Exception):
            await self._conn.send(json.dumps(frame))

    def _fail(self) -> None:
        self._connected = False
        self._teardown()

    async def _enable_heartbeat(self) -> None:
        if self.heartbeat <= 0:
            return
        await self.rpc(ch.PUBLIC_SET_HEARTBEAT, {"interval": int(self.heartbeat)})

    async def _on_open(self) -> None:
        """Say whatever a fresh socket needs before it is usable."""

    async def _restore(self) -> None:
        """Replay live subscriptions onto a fresh socket."""

    def _push(self, resp: DeribitResponse) -> None:
        """Route a channel frame. Default: nothing subscribes."""

    def _teardown(self) -> None:
        """Close every stream reading this socket."""


__all__ = ["DEFAULT_HEARTBEAT", "DEFAULT_PING_INTERVAL", "DeribitSocket"]
