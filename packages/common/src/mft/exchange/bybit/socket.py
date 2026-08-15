"""The connection machinery Bybit's three sockets share.

Bybit runs order entry, account pushes and market pushes on separate
connections (see :mod:`.protocol`), but not on separate protocols: all three
take ``{"op", "args", "req_id"}``, answer on the id they were given, push
``{"topic", "data"}`` frames that answer nothing, and expect an application
heartbeat. So the reconnect loop, the request/reply correlation, the heartbeat
and the push fan-out live here once, and the three clients differ only in what
they say on a fresh socket (:meth:`BybitSocket._on_open`), what they replay
onto it (:meth:`BybitSocket._restore`), and how they route a push
(:meth:`BybitSocket._push`).

**The heartbeat is not optional.** Bybit closes a connection that has sent
nothing for long enough, whatever the WebSocket layer is doing, so
:meth:`BybitSocket._heartbeat` sends ``{"op": "ping"}`` on a timer. It is also
this adapter's liveness check, and deliberately not a request/reply one: the
public sockets echo ``op: ping`` with our ``req_id`` while the private ones
answer ``op: pong`` without it, so a ping that waited for *its own* reply would
time out forever on half the venue. What the loop watches instead is whether
**any** frame has arrived recently — a pong proves the peer is there whichever
way it is spelled — and a socket that has gone quiet through a heartbeat is
closed so the read loop reconnects it.

That is the failure this matters for: a TCP connection that is dead in one
direction goes on accepting sends. Without the watchdog, an account socket
could sit "connected" and silent indefinitely while orders filled unreported.
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

from mft.exchange.bybit.protocol import (
    BybitResponse,
    BybitWsError,
    ping_frame,
)
from mft.exchange.errors import ExchangeNotConnectedError

logger = logging.getLogger(__name__)

#: How often the application heartbeat goes out. Bybit's own guidance is 20
#: seconds, and its idle timeout is comfortably longer.
DEFAULT_PING_INTERVAL = 20.0


@dataclass
class _Pending:
    """A request waiting on its reply, keyed by the ``req_id`` we sent."""

    future: asyncio.Future[BybitResponse]
    op: str


@dataclass
class _Stats:
    reconnects: int = 0
    frames: int = 0
    pings: int = 0
    last_frame_at: float = field(default=0.0)


class BybitSocket:
    """A reconnecting JSON WebSocket with req_id correlation and a heartbeat."""

    name = "bybit"

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
        #: Seconds between application heartbeats. Zero disables both the
        #: heartbeat and the silence watchdog, which only a test against a stub
        #: server should want.
        self.ping_interval = ping_interval
        #: How long the closing handshake may take before the socket is
        #: dropped. ``websockets`` defaults this to 10 seconds, which is paid
        #: by whoever is tearing the connection down and buys nothing here:
        #: :meth:`close` has already cancelled every pending request, and the
        #: venue learns we are gone from the TCP close regardless. Orders
        #: already at the venue are unaffected by how this socket ends.
        self.close_timeout = close_timeout

        self._conn: ClientConnection | None = None
        self._pending: dict[str, _Pending] = {}
        self._task: asyncio.Task[Any] | None = None
        self._ping_task: asyncio.Task[Any] | None = None
        self._connected = False
        self._closing = False
        #: Whether the read loop is currently draining the socket. False while
        #: :meth:`_on_open` and :meth:`_restore` run, which is what lets
        #: :meth:`request` know nobody is there to hand it a reply.
        self._pumping = False
        self._reconnect_cbs: list[Callable[[], Any]] = []
        self.stats = _Stats()

    # --- lifecycle ---------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        if self._connected:
            return
        self._closing = False
        try:
            await self._open()
            # Before the pump starts, so the auth reply can be read inline off
            # the same socket — the reconnect path reuses this for exactly that
            # reason.
            await self._on_open()
            # Set before the task exists, not inside it: the loop owns ``recv``
            # from here on, and a caller that reached ``request`` in the gap
            # would otherwise read the socket alongside it.
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

    async def __aenter__(self) -> BybitSocket:
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
        # No protocol-level ping: Bybit's keepalive is the application ``ping``
        # op, and layering the WebSocket one on top would mean a connection
        # could be dropped by ``websockets`` for a pong Bybit never promised.
        self._conn = await connect(
            self.url, ping_interval=None, close_timeout=self.close_timeout
        )

    def _ensure_connected(self) -> None:
        if not self._connected or self._conn is None:
            raise ExchangeNotConnectedError(
                f"{self.name} is not connected; call connect() first"
            )

    # --- request / reply ---------------------------------------------------

    async def send(self, frame: dict[str, Any]) -> None:
        """Send a pre-built frame. Escape hatch for ops not typed here."""
        self._ensure_connected()
        assert self._conn is not None
        await self._conn.send(json.dumps(frame))

    async def request(
        self,
        frame: dict[str, Any],
        req_id: str,
        *,
        op: str = "",
        timeout: float | None = None,
    ) -> BybitResponse:
        """Send a frame and await the reply carrying the same ``req_id``.

        Raises whatever the reply refused with, so a caller gets Bybit's own
        ``retCode`` rather than a bare unsuccessful result to interpret.

        On a socket whose pump is not running — which is the state
        :meth:`_restore` re-subscribes from — the reply is read inline instead.
        Without that, a restore that awaited an ack would wait for a read loop
        that has not started, block the reconnect for a full timeout, and then
        fail it; the subscription would have gone out and the socket would sit
        unread for that whole window.
        """
        self._ensure_connected()
        if not self._pumping:
            return await self.handshake(frame, req_id, op=op)
        wait = timeout or self.ack_timeout
        loop = asyncio.get_running_loop()
        pending = _Pending(future=loop.create_future(), op=op)
        self._pending[req_id] = pending
        try:
            await self.send(frame)
            resp = await asyncio.wait_for(pending.future, timeout=wait)
        except TimeoutError as exc:
            raise BybitWsError(None, f"no reply within {wait}s", op=op) from exc
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
    ) -> BybitResponse:
        """Send a frame and read its reply inline, with the pump stopped.

        Used from :meth:`_on_open`, where there is no read loop yet to hand the
        reply over. Frames that arrive before the one we want are dropped:
        nothing has subscribed yet, so nothing is owed them.
        """
        if self._conn is None:
            raise ExchangeNotConnectedError(f"{self.name} has no live socket")
        await self._conn.send(json.dumps(frame))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.ack_timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise BybitWsError(
                    None,
                    f"no reply to {op or 'handshake'} within {self.ack_timeout}s",
                    op=op,
                )
            raw = await asyncio.wait_for(self._conn.recv(), timeout=remaining)
            message = self._decode(raw)
            if message is None:
                continue
            resp = BybitResponse(message)
            # Bybit echoes the id on an op reply, but not always on the frame
            # that refuses one — an auth rejected before it was parsed comes
            # back with the op and no id. Matching the op as well means such a
            # refusal is raised here rather than waited out.
            if resp.req_id != req_id and not (op and resp.op == op):
                logger.debug("%s dropping pre-handshake frame %r", self.name, resp)
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
                # ``recv`` back to ``_on_open`` / ``_restore`` and this takes it
                # again. There is no await between here and ``_pump``, so
                # nothing can observe the socket as unowned.
                self._pumping = True
                await self._pump()
                # A clean close ends the iteration without raising. That is a
                # disconnect, not a reason to stop reading.
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
            # the owner so it can rebuild state rather than trusting a view that
            # stopped being updated at some unknown point.
            self._fire_reconnect()
            retries = 0

    async def _pump(self) -> None:
        assert self._conn is not None
        conn = self._conn
        async for raw in conn:
            message = self._decode(raw)
            if message is None:
                continue
            self._dispatch(BybitResponse(message))

    async def _heartbeat(self) -> None:
        """Send ``{"op": "ping"}`` on a timer and watch for silence in return.

        The reply is not awaited — see the module docstring for why a pong
        cannot be correlated on this venue. What is checked is the arrival time
        of the last frame of *any* kind: if a whole interval has passed with
        nothing coming back after a ping went out, the connection is not
        carrying data in both directions and is closed so the read loop
        reconnects it.
        """
        # How long the pong is given before silence counts as a dead socket.
        # Taken out of the interval rather than added to it, so the heartbeat
        # still goes out once per ``ping_interval`` however long the grace is.
        grace = min(self.ping_interval / 2, 5.0)
        while not self._closing:
            await asyncio.sleep(max(self.ping_interval - grace, 0.1))
            if self._closing or not self._connected or self._conn is None:
                continue
            silent_since = self.stats.last_frame_at
            try:
                frame, _ = ping_frame()
                await self.send(frame)
                self.stats.pings += 1
            except Exception:
                # A send that fails means the read loop is already on its way
                # to noticing; nothing to add here.
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
        try:
            message = json.loads(raw)
        except ValueError:
            logger.warning("%s non-JSON frame: %r", self.name, raw[:200])
            return None
        return message if isinstance(message, dict) else None

    def _dispatch(self, resp: BybitResponse) -> None:
        if resp.is_reply:
            pending = self._pending.get(resp.req_id or "")
            if pending is not None:
                if not pending.future.done():
                    pending.future.set_result(resp)
                return
            if resp.is_pong:
                # Expected and uncorrelatable: the heartbeat does not wait for
                # it, and its only job was to update ``last_frame_at``, which
                # decoding it already did.
                return
            if resp.error is not None:
                # A refusal answering no request we are still waiting on — a
                # malformed subscribe, or a limit tripped by something already
                # sent.
                logger.warning("%s unsolicited error: %s", self.name, resp.error)
                return
            logger.debug("%s unmatched reply %r", self.name, resp)
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

    def _push(self, resp: BybitResponse) -> None:
        """Route a topic frame. Default: nothing subscribes."""

    def _teardown(self) -> None:
        """Close every stream reading this socket. On close and on give-up."""


__all__ = ["DEFAULT_PING_INTERVAL", "BybitSocket"]
