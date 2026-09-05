"""The connection machinery Bitget's public and private sockets share.

Both take ``{"id", "op", "args"}``. The venue's v3 ACK often omits ``id``
even when we sent one — subscribe comes back as
``{"event", "arg", "connId"}``. Correlation is therefore: the id when
present, otherwise ``(event, arg)``, the same rule handshake already
used for login. Pushes are ``{"arg", "data"}``. The heartbeat is the
literal string ``ping``; the pong is ``pong``. Neither is JSON.

**The heartbeat is not optional.** Bitget closes a connection that has
sent nothing for ~2 minutes.
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

from mftik.exchange.bitget.channels import arg_key
from mftik.exchange.bitget.protocol import (
    PONG,
    BitgetResponse,
    BitgetWsError,
)
from mftik.exchange.errors import ExchangeNotConnectedError

logger = logging.getLogger(__name__)

DEFAULT_PING_INTERVAL = 20.0


@dataclass
class _Pending:
    future: asyncio.Future[BitgetResponse]
    op: str
    args: tuple[dict[str, Any], ...] = ()


def _frame_args(frame: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    raw = frame.get("args")
    if not isinstance(raw, list):
        return ()
    return tuple(arg for arg in raw if isinstance(arg, dict))


def _ack_matches(
    resp: BitgetResponse,
    *,
    req_id: str,
    op: str,
    args: tuple[dict[str, Any], ...] = (),
) -> bool:
    """Whether ``resp`` is the ACK for this request.

    Bitget's v3 subscribe ACK omits ``id``. An id-less reply matches on
    ``event == op``, and on ``arg`` when two subscribes share the socket.
    """
    if resp.req_id:
        return resp.req_id == req_id
    if not (op and resp.event == op):
        return False
    if not resp.arg or not args:
        return True
    want = arg_key(resp.arg)
    return any(arg_key(arg) == want for arg in args)


@dataclass
class _Stats:
    reconnects: int = 0
    frames: int = 0
    pings: int = 0
    last_frame_at: float = field(default_factory=float)


class BitgetSocket:
    """A reconnecting JSON WebSocket with id correlation and a text heartbeat."""

    name = "bitget"

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
        close_timeout: float = 2.0,
    ) -> None:
        self.url = url
        self.ack_timeout = ack_timeout
        self.reconnect = reconnect
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.max_retry_backoff = max_retry_backoff
        self.ping_interval = ping_interval
        self.close_timeout = close_timeout

        self._conn: ClientConnection | None = None
        self._pending: dict[str, _Pending] = {}
        self._task: asyncio.Task[Any] | None = None
        self._ping_task: asyncio.Task[Any] | None = None
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
                self._ping_task = asyncio.create_task(
                    self._heartbeat(), name=f"{self.name}-ping"
                )
        except Exception:
            await self.close()
            raise

    async def close(self) -> None:
        self._closing = True
        self._connected = False
        for task in (self._task, self._ping_task):
            if task is not None:
                task.cancel()
        tasks = [t for t in (self._task, self._ping_task) if t is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._task = None
        self._ping_task = None
        for pending in self._pending.values():
            if not pending.future.done():
                pending.future.cancel()
        self._pending.clear()
        self._teardown()
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> BitgetSocket:
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
    ) -> BitgetResponse:
        self._ensure_connected()
        if not self._pumping:
            return await self.handshake(frame, req_id, op=op)
        wait = timeout or self.ack_timeout
        loop = asyncio.get_running_loop()
        pending = _Pending(
            future=loop.create_future(), op=op, args=_frame_args(frame)
        )
        self._pending[req_id] = pending
        try:
            await self.send(frame)
            resp = await asyncio.wait_for(pending.future, timeout=wait)
        except TimeoutError as exc:
            raise BitgetWsError(None, f"no reply within {wait}s", op=op) from exc
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
    ) -> BitgetResponse:
        if self._conn is None:
            raise ExchangeNotConnectedError(f"{self.name} has no live socket")
        await self._conn.send(json.dumps(frame))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.ack_timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise BitgetWsError(
                    None,
                    f"no reply to {op or 'handshake'} within {self.ack_timeout}s",
                    op=op,
                )
            raw = await asyncio.wait_for(self._conn.recv(), timeout=remaining)
            message = self._decode(raw)
            if message is None:
                continue
            resp = BitgetResponse(message)
            if not _ack_matches(
                resp, req_id=req_id, op=op, args=_frame_args(frame)
            ):
                logger.debug("%s dropping pre-handshake frame %r", self.name, resp)
                continue
            resp.raise_for_error()
            return resp

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
            self._dispatch(BitgetResponse(message))

    async def _heartbeat(self) -> None:
        grace = min(self.ping_interval / 2, 5.0)
        while not self._closing:
            await asyncio.sleep(max(self.ping_interval - grace, 0.1))
            if self._closing or not self._connected or self._conn is None:
                continue
            silent_since = self.stats.last_frame_at
            try:
                await self.send("ping")
                self.stats.pings += 1
            except Exception:
                logger.debug("%s heartbeat send failed", self.name, exc_info=True)
                continue
            await asyncio.sleep(grace)
            conn = self._conn
            if self._closing or conn is None:
                continue
            if self.stats.last_frame_at <= silent_since:
                logger.warning(
                    "%s answered no frame within %.0fs of a ping; "
                    "dropping the socket",
                    self.name,
                    grace,
                )
                with contextlib.suppress(Exception):
                    await conn.close()

    def _decode(self, raw: str | bytes) -> dict[str, Any] | None:
        self.stats.frames += 1
        self.stats.last_frame_at = asyncio.get_running_loop().time()
        text = raw.decode() if isinstance(raw, bytes) else raw
        if text == PONG:
            return {"event": PONG}
        try:
            message = json.loads(text)
        except ValueError:
            logger.warning("%s non-JSON frame: %r", self.name, text[:200])
            return None
        return message if isinstance(message, dict) else None

    def _dispatch(self, resp: BitgetResponse) -> None:
        if resp.is_pong:
            return
        if resp.is_reply:
            pending = self._lookup_pending(resp)
            if pending is not None:
                if not pending.future.done():
                    pending.future.set_result(resp)
                return
            if resp.error is not None:
                logger.warning("%s unsolicited error: %s", self.name, resp.error)
                return
            logger.debug("%s unmatched reply %r", self.name, resp)
            return
        if resp.is_push:
            self._push(resp)
            return
        logger.debug("%s ignoring frame %r", self.name, resp)

    def _lookup_pending(self, resp: BitgetResponse) -> _Pending | None:
        if resp.req_id:
            return self._pending.get(resp.req_id)
        found: _Pending | None = None
        for pending in self._pending.values():
            if not _ack_matches(
                resp, req_id="", op=pending.op, args=pending.args
            ):
                continue
            if found is not None:
                return None
            found = pending
        return found

    def _fail(self) -> None:
        self._connected = False
        self._teardown()

    async def _on_open(self) -> None:
        """Say whatever a fresh socket needs before it is usable."""

    async def _restore(self) -> None:
        """Replay live subscriptions onto a fresh socket."""

    def _push(self, resp: BitgetResponse) -> None:
        """Route a channel frame. Default: nothing subscribes."""

    def _teardown(self) -> None:
        """Close every stream reading this socket."""


__all__ = ["DEFAULT_PING_INTERVAL", "BitgetSocket"]
