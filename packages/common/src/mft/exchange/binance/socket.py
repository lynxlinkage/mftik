"""The connection machinery every Binance socket shares.

Binance splits what Gate serves on one URL across several: order entry and
account reads live on a **WebSocket API** (``ws-api.binance.com``,
``ws-fapi.binance.com``), while market pushes live on **market streams** hosts
(``stream.binance.com``, and on futures a ``fstream.binance.com`` endpoint per
traffic class). They are separate connections with separate lifetimes and
separate reasons to drop.

What they are *not* is separate protocols. All of them take ``{"id", "method",
"params"}``, all answer on the ``id`` they were given, and all drop the
connection for reasons the owner has to recover from rather than notice. So the
reconnect loop, the request/reply correlation and the stream fan-out live here
once, and the clients above differ only in what they say on a fresh socket
(:meth:`BinanceSocket._on_open`), what they replay onto it
(:meth:`BinanceSocket._restore`), and how they route a push
(:meth:`BinanceSocket._push`).

**Correlation is by ``id`` alone.** Every reply carries the id it answers and
every push carries none, so there is no arrival-order heuristic here and no
ambiguity when several requests are in flight on one socket — which matters,
because that is the normal state of the order path.

There is no application-level ping loop. Binance sends protocol-level pings and
expects pongs, which ``websockets`` answers on its own, and it drops a
connection that stops responding — so a ping loop here would duplicate a
keepalive that already exists and hide the disconnect it was meant to catch.
The 24-hour disconnect Binance applies to every socket is a reconnect, not an
error, and the loop below treats it as one.
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

from mft.exchange.binance.protocol import BinanceResponse, BinanceWsError
from mft.exchange.errors import ExchangeNotConnectedError

logger = logging.getLogger(__name__)


@dataclass
class _Pending:
    """A request waiting on its reply, keyed by the ``id`` we sent."""

    future: asyncio.Future[BinanceResponse]
    method: str


@dataclass
class _Stats:
    reconnects: int = 0
    frames: int = 0
    last_frame_at: float = field(default=0.0)


class BinanceSocket:
    """A reconnecting JSON WebSocket with id-correlated request/reply."""

    name = "binance"

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
        self.url = url
        self.ack_timeout = ack_timeout
        self.reconnect = reconnect
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.max_retry_backoff = max_retry_backoff
        #: Protocol-level ping interval handed to ``websockets``. Zero disables
        #: it, which only a test against a stub server should want.
        self.keepalive = keepalive

        self._conn: ClientConnection | None = None
        self._pending: dict[str, _Pending] = {}
        self._task: asyncio.Task[Any] | None = None
        self._connected = False
        self._closing = False
        #: Whether the read loop is currently draining the socket. False while
        #: :meth:`_on_open` and :meth:`_restore` run, which is what lets
        #: :meth:`request` know nobody is there to hand it a reply.
        self._pumping = False
        self._reconnect_cbs: list[Callable[[], Any]] = []
        self.stats = _Stats()
        #: Serializes :meth:`connect`. Futures opens a market socket lazily on
        #: first subscribe, and two concurrent subscribe_* calls would otherwise
        #: each open a connection and start a second ``recv`` alongside the
        #: first — websockets refuses that with a reconnect storm.
        self._connect_lock = asyncio.Lock()

    # --- lifecycle ---------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        async with self._connect_lock:
            if self._connected:
                return
            self._closing = False
            try:
                await self._open()
                # Before the pump starts, so a handshake reply can be read
                # inline off the same socket — the reconnect path reuses this
                # for exactly that reason.
                await self._on_open()
                # Set before the task exists, not inside it: the loop owns
                # ``recv`` from here on, and a caller that reached ``request``
                # in the gap would otherwise read the socket alongside it.
                self._pumping = True
                self._task = asyncio.create_task(
                    self._read_loop(), name=f"{self.name}-read"
                )
                self._connected = True
            except Exception:
                await self.close()
                raise

    async def close(self) -> None:
        self._closing = True
        self._connected = False
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        for pending in self._pending.values():
            if not pending.future.done():
                pending.future.cancel()
        self._pending.clear()
        self._teardown()
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> BinanceSocket:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    def on_reconnect(self, callback: Callable[[], Any]) -> None:
        """Register a callback fired once the socket is back and restored."""
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
            self.url,
            ping_interval=self.keepalive or None,
        )

    def _ensure_connected(self) -> None:
        if not self._connected or self._conn is None:
            raise ExchangeNotConnectedError(
                f"{self.name} is not connected; call connect() first"
            )

    # --- request / reply ---------------------------------------------------

    async def send(self, frame: dict[str, Any]) -> None:
        """Send a pre-built frame. Escape hatch for methods not typed here."""
        self._ensure_connected()
        assert self._conn is not None
        await self._conn.send(json.dumps(frame))

    async def request(
        self,
        frame: dict[str, Any],
        req_id: str,
        *,
        method: str = "",
        timeout: float | None = None,
    ) -> BinanceResponse:
        """Send a frame and await the reply carrying the same ``id``.

        Raises whatever error the reply carried, so a caller gets the venue's
        own code rather than a bare ``None`` result to interpret.

        On a socket whose pump is not running — which is the state
        :meth:`_restore` re-subscribes from — the reply is read inline instead.
        Without that, a restore that awaited an ack would wait for a read loop
        that has not started, block the reconnect for a full timeout, and then
        fail it; the subscription would have gone out and the socket would sit
        unread for that whole window.
        """
        self._ensure_connected()
        if not self._pumping:
            return await self.handshake(frame, req_id, method=method)
        wait = timeout or self.ack_timeout
        loop = asyncio.get_running_loop()
        pending = _Pending(future=loop.create_future(), method=method)
        self._pending[req_id] = pending
        try:
            await self.send(frame)
            resp = await asyncio.wait_for(pending.future, timeout=wait)
        except TimeoutError as exc:
            raise BinanceWsError(
                None, f"no reply within {wait}s", method=method
            ) from exc
        finally:
            self._pending.pop(req_id, None)
        resp.raise_for_error()
        return resp

    async def handshake(
        self,
        frame: dict[str, Any],
        req_id: str,
        *,
        method: str = "",
    ) -> BinanceResponse:
        """Send a frame and read its reply inline, with the pump stopped.

        Used only from :meth:`_on_open`, where there is no read loop yet to
        hand the reply over. Frames that arrive before the one we want are
        dropped: nothing has subscribed yet, so nothing is owed them.
        """
        if self._conn is None:
            raise ExchangeNotConnectedError(f"{self.name} has no live socket")
        await self._conn.send(json.dumps(frame))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.ack_timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise BinanceWsError(
                    None,
                    f"no reply to {method or 'handshake'} within "
                    f"{self.ack_timeout}s",
                    method=method,
                )
            raw = await asyncio.wait_for(self._conn.recv(), timeout=remaining)
            message = self._decode(raw)
            if message is None:
                continue
            resp = BinanceResponse(message)
            if resp.id != req_id:
                logger.debug(
                    "%s dropping pre-handshake frame %r", self.name, resp
                )
                continue
            resp.raise_for_error()
            return resp

    # --- loops -------------------------------------------------------------

    async def _read_loop(self) -> None:
        retries = 0
        while not self._closing:
            reason: object
            try:
                # Re-asserted every lap: the reconnect block below hands
                # ``recv`` back to ``_on_open`` / ``_restore`` and this takes
                # it again. There is no await between here and ``_pump``, so
                # nothing can observe the socket as unowned.
                self._pumping = True
                await self._pump()
                # Binance closes every socket after 24 hours, and a clean close
                # ends the iteration without raising. That is a disconnect, not
                # a reason to stop reading.
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
            # Whatever happened while the socket was down went unreported. Tell
            # the owner so it can rebuild state rather than trusting a view
            # that stopped being updated at some unknown point.
            self._fire_reconnect()
            retries = 0

    async def _pump(self) -> None:
        assert self._conn is not None
        conn = self._conn
        async for raw in conn:
            message = self._decode(raw)
            if message is None:
                continue
            self._dispatch(BinanceResponse(message))

    def _decode(self, raw: str | bytes) -> dict[str, Any] | None:
        self.stats.frames += 1
        self.stats.last_frame_at = asyncio.get_running_loop().time()
        try:
            message = json.loads(raw)
        except ValueError:
            logger.warning("%s non-JSON frame: %r", self.name, raw[:200])
            return None
        return message if isinstance(message, dict) else None

    def _dispatch(self, resp: BinanceResponse) -> None:
        if resp.is_reply:
            pending = self._pending.get(resp.id or "")
            if pending is None:
                logger.debug("%s unmatched reply %r", self.name, resp)
                return
            if not pending.future.done():
                pending.future.set_result(resp)
            return
        if resp.error is not None:
            # An error with no id answers no request — a malformed subscribe,
            # or a limit tripped by something already sent.
            logger.warning("%s unsolicited error: %s", self.name, resp.error)
            return
        self._push(resp)

    def _fail(self) -> None:
        """Give up: mark disconnected and close everything reading this socket."""
        self._connected = False
        self._teardown()

    # --- subclass hooks ----------------------------------------------------

    async def _on_open(self) -> None:
        """Say whatever a fresh socket needs before it is usable.

        Runs with the pump stopped, on both the first connect and every
        reconnect, so :meth:`handshake` is available here and nowhere else.
        """

    async def _restore(self) -> None:
        """Replay live subscriptions onto a fresh socket."""

    def _push(self, resp: BinanceResponse) -> None:
        """Route an unsolicited frame. Default: nothing subscribes."""

    def _teardown(self) -> None:
        """Close every stream reading this socket. Called on close and on give-up."""


__all__ = ["BinanceSocket"]
