"""Gate spot WebSocket v4 client — one socket for public and private channels.

Gate serves everything from ``wss://api.gateio.ws/ws/v4/`` with one envelope,
so this adapter is one class rather than the usual public/private split.
Credentials are optional: supply them and private channel subscribe frames
are signed, plus ``spot.login`` runs so trading API calls can proceed. Omit
them and the public channels work unchanged.

This is an adapter, not a normalized venue interface. Pairs stay in Gate's
``BTC_USDT`` spelling, streams are typed with Gate's own message models, and
channels Gate has no shared analogue for (candlesticks, book ticker, depth
diffs) are exposed as-is.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, TypeVar

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from mftik.exchange.errors import ExchangeError, ExchangeNotConnectedError
from mftik.exchange.gate.spot import channels as ch
from mftik.exchange.gate.spot.models import (
    GateBalance,
    GateBookTicker,
    GateCandlestick,
    GateOrderAck,
    GateOrderBook,
    GateOrderBookUpdate,
    GateOrderUpdate,
    GateTicker,
    GateTrade,
    GateUserTrade,
    to_text,
)
from mftik.exchange.gate.spot.protocol import (
    GateResponse,
    GateWsError,
    login_frame,
    ping_frame,
    request_frame,
    session_api_frame,
)
from mftik.exchange.models import OrderType, Side
from mftik.exchange.stream import EventStream
from mftik.exchange.wire import WireLedger

logger = logging.getLogger(__name__)

T = TypeVar("T")

GATE_SPOT_WS_URL = "wss://api.gateio.ws/ws/v4/"


@dataclass
class _Sub:
    """One live subscribe call: its frame (for replay) and its output stream."""

    channel: str
    payload: list[str]
    stream: EventStream[Any]
    parse: Callable[[dict[str, Any]], Any]
    private: bool = False


@dataclass
class _Pending:
    """Awaiting the reply to a subscribe/unsubscribe frame or a trading call."""

    future: asyncio.Future[GateResponse]
    channel: str
    event: str
    req_id: str | None = None


@dataclass
class _Stats:
    reconnects: int = 0
    frames: int = 0
    last_pong_at: float = field(default=0.0)


class GateSpotWebSocket:
    """Gate spot v4 WebSocket connectivity.

    Market data, account pushes and order entry all share the one socket::

        async with GateSpotWebSocket(api_key=k, api_secret=s) as ws:
            trades = await ws.subscribe_trades("BTC_USDT", "ETH_USDT")
            orders = await ws.subscribe_orders()      # !all, needs auth
            ack = await ws.place_order(
                currency_pair="BTC_USDT", side="buy",
                amount="0.001", price="60000", text="my-id",
            )
            await ws.cancel_order(ack.id, currency_pair="BTC_USDT")
            async for update in orders:
                ...

    One ``subscribe_*`` call yields one stream carrying **everything** that
    channel pushes — Gate multiplexes per channel, not per pair, so list all
    the pairs you want in a single call and read ``currency_pair`` off each
    message rather than opening a stream per pair.

    Trading calls are request/reply and return once Gate answers; the matching
    lifecycle push still arrives on ``spot.orders`` independently.
    """

    name = "gate.spot"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        url: str = GATE_SPOT_WS_URL,
        channel_id: str = "",
        ping_interval: float = 5.0,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        close_timeout: float = 2.0,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.url = url
        #: Optional broker/partner tag sent as ``X-Gate-Channel-Id``.
        self.channel_id = channel_id
        self.ping_interval = ping_interval
        self.ack_timeout = ack_timeout
        self.reconnect = reconnect
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.max_retry_backoff = max_retry_backoff
        #: How long the closing handshake may take before the socket is
        #: dropped. ``websockets`` defaults this to 10 seconds, paid by
        #: whoever is tearing the connection down. Nothing here needs it:
        #: pending requests are cancelled before the handshake begins, and
        #: orders already at the venue do not depend on how this socket ends.
        self.close_timeout = close_timeout

        self._conn: ClientConnection | None = None
        self._subs: list[_Sub] = []
        self._ledger: WireLedger[tuple[str, tuple[str, ...]]] = WireLedger()
        self._pending: list[_Pending] = []
        self._tasks: list[asyncio.Task[Any]] = []
        self._connected = False
        self._closing = False
        self._reconnect_cbs: list = []
        self._logged_in = False
        self.stats = _Stats()

    # --- lifecycle ---------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def authenticated(self) -> bool:
        """Whether credentials are configured (private channels + trading)."""
        return bool(self.api_key and self.api_secret)

    @property
    def logged_in(self) -> bool:
        """Whether ``spot.login`` has succeeded on the current socket."""
        return self._logged_in

    async def connect(self) -> None:
        if self._connected:
            return
        self._closing = False
        try:
            await self._open()
            # Login before the read loop starts so reconnect can reuse the
            # same inline recv path (no concurrent pump yet).
            if self.authenticated:
                await self._authenticate_socket()
            self._tasks = [
                asyncio.create_task(self._read_loop(), name="gate-spot-read"),
            ]
            if self.ping_interval > 0:
                self._tasks.append(
                    asyncio.create_task(self._ping_loop(), name="gate-spot-ping")
                )
            self._connected = True
        except Exception:
            await self.close()
            raise

    async def close(self) -> None:
        self._closing = True
        self._connected = False
        self._logged_in = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        for pending in self._pending:
            if not pending.future.done():
                pending.future.cancel()
        self._pending.clear()
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()
        self._ledger.clear()
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> GateSpotWebSocket:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    def on_reconnect(self, callback) -> None:
        """Register a callback fired after the socket is back and resubscribed."""
        self._reconnect_cbs.append(callback)

    def _fire_reconnect(self) -> None:
        for cb in list(self._reconnect_cbs):
            try:
                result = cb()
            except Exception:
                logger.exception("gate.spot reconnect callback failed")
                continue
            if asyncio.iscoroutine(result):
                asyncio.create_task(result, name="gate-spot-reconnect-cb")

    async def _open(self) -> None:
        self._conn = await connect(self.url, close_timeout=self.close_timeout)

    async def _authenticate_socket(self) -> None:
        """Run ``spot.login`` on the current socket (no concurrent reader).

        Used from :meth:`connect` and the reconnect path, both of which call
        this while the pump is stopped so the login reply can be read inline.
        """
        if self._conn is None or not self.api_key or not self.api_secret:
            raise ExchangeError("spot.login requires a live socket and credentials")
        frame, req_id = login_frame(api_key=self.api_key, api_secret=self.api_secret)
        await self._conn.send(json.dumps(frame))
        deadline = asyncio.get_running_loop().time() + self.ack_timeout
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise GateWsError(
                    None,
                    f"no reply to {ch.LOGIN} within {self.ack_timeout}s",
                    channel=ch.LOGIN,
                )
            raw = await asyncio.wait_for(self._conn.recv(), timeout=remaining)
            self.stats.frames += 1
            try:
                message = json.loads(raw)
            except ValueError:
                logger.warning("gate.spot non-JSON frame during login: %r", raw[:200])
                continue
            if not isinstance(message, dict):
                continue
            resp = GateResponse(message)
            if resp.channel == ch.PONG:
                self.stats.last_pong_at = asyncio.get_running_loop().time()
                continue
            if resp.ack:
                continue
            if resp.is_api and (
                resp.req_id == req_id or resp.channel == ch.LOGIN
            ):
                resp.raise_for_error()
                self._logged_in = True
                logger.info("gate.spot logged in")
                return
            logger.warning(
                "gate.spot dropping pre-login frame channel=%s event=%s",
                resp.channel,
                resp.event,
            )

    def _ensure_connected(self) -> None:
        if not self._connected or self._conn is None:
            raise ExchangeNotConnectedError(
                f"{self.name} is not connected; call connect() first"
            )

    def _ensure_auth(self, channel: str) -> None:
        if not self.authenticated:
            kind = "trading call" if channel in ch.TRADING else "private channel"
            raise ExchangeError(
                f"{channel} is a {kind}; api_key and api_secret are required"
            )

    # --- raw plumbing ------------------------------------------------------

    async def send(self, frame: dict[str, Any]) -> None:
        """Send a pre-built frame. Escape hatch for channels not typed here."""
        self._ensure_connected()
        assert self._conn is not None
        await self._conn.send(json.dumps(frame))

    async def request(
        self,
        channel: str,
        event: str,
        payload: list[str] | None,
        *,
        private: bool = False,
    ) -> GateResponse:
        """Send a subscribe/unsubscribe frame and await its ack."""
        self._ensure_connected()
        if private:
            self._ensure_auth(channel)
        frame = request_frame(
            channel,
            event,
            payload,
            api_key=self.api_key if private else None,
            api_secret=self.api_secret if private else None,
        )
        loop = asyncio.get_running_loop()
        pending = _Pending(future=loop.create_future(), channel=channel, event=event)
        self._pending.append(pending)
        try:
            await self.send(frame)
            resp = await asyncio.wait_for(pending.future, timeout=self.ack_timeout)
        except TimeoutError as exc:
            raise GateWsError(
                None, f"no ack within {self.ack_timeout}s", channel=channel
            ) from exc
        finally:
            if pending in self._pending:
                self._pending.remove(pending)
        resp.raise_for_error()
        return resp

    async def subscribe_raw(
        self,
        channel: str,
        payload: list[str] | None = None,
        *,
        private: bool = False,
    ) -> EventStream[dict[str, Any]]:
        """Subscribe and stream the channel's raw ``result`` rows."""
        return await self._subscribe(channel, payload, lambda row: row, private=private)

    async def unsubscribe(
        self, channel: str, payload: list[str] | None = None, *, private: bool = False
    ) -> None:
        """Unsubscribe a channel and close every stream reading it."""
        await self.request(channel, ch.UNSUBSCRIBE, payload, private=private)
        self._ledger.discard(key for key in self._ledger.held() if key[0] == channel)
        for sub in [s for s in self._subs if s.channel == channel]:
            # close() fires on_close → _drop_stream, which does the removal.
            sub.stream.close()

    async def ping(self) -> None:
        """Send one application-level ping (Gate answers on ``spot.pong``)."""
        await self.send(ping_frame(ch.PING))

    async def _subscribe(
        self,
        channel: str,
        payload: list[str] | None,
        parse: Callable[[dict[str, Any]], T],
        *,
        private: bool = False,
    ) -> EventStream[T]:
        key = (channel, tuple(payload or ()))

        async def send(_keys: list[tuple[str, tuple[str, ...]]]) -> None:
            await self.request(channel, ch.SUBSCRIBE, payload, private=private)

        await self._ledger.acquire([key], send)
        stream: EventStream[T] = EventStream(on_close=self._drop_stream)
        self._subs.append(
            _Sub(
                channel=channel,
                payload=list(payload or []),
                stream=stream,
                parse=parse,
                private=private,
            )
        )
        return stream

    def _drop_stream(self, stream: EventStream[Any]) -> None:
        self._subs = [s for s in self._subs if s.stream is not stream]

    # --- public channels ---------------------------------------------------

    async def subscribe_tickers(self, *pairs: str) -> EventStream[GateTicker]:
        """``spot.tickers`` — 24h rolling stats."""
        return await self._subscribe(
            ch.TICKERS, ch.tickers(*pairs), GateTicker.model_validate
        )

    async def subscribe_trades(self, *pairs: str) -> EventStream[GateTrade]:
        """``spot.trades`` — public tape."""
        return await self._subscribe(
            ch.TRADES, ch.trades(*pairs), GateTrade.model_validate
        )

    async def subscribe_candlesticks(
        self, interval: str, pair: str
    ) -> EventStream[GateCandlestick]:
        """``spot.candlesticks`` — one interval/pair per subscribe call."""
        return await self._subscribe(
            ch.CANDLESTICKS,
            ch.candlesticks(interval, pair),
            GateCandlestick.model_validate,
        )

    async def subscribe_order_book(
        self, pair: str, *, level: str = "20", interval: str = "1000ms"
    ) -> EventStream[GateOrderBook]:
        """``spot.order_book`` — capped-depth snapshots on a timer."""
        return await self._subscribe(
            ch.ORDER_BOOK,
            ch.order_book(pair, level=level, interval=interval),
            GateOrderBook.model_validate,
        )

    async def subscribe_order_book_update(
        self, pair: str, *, interval: str = "100ms"
    ) -> EventStream[GateOrderBookUpdate]:
        """``spot.order_book_update`` — depth diffs; apply in ``U``/``u`` order."""
        return await self._subscribe(
            ch.ORDER_BOOK_UPDATE,
            ch.order_book_update(pair, interval=interval),
            GateOrderBookUpdate.model_validate,
        )

    async def subscribe_book_ticker(self, *pairs: str) -> EventStream[GateBookTicker]:
        """``spot.book_ticker`` — best bid/ask on every change."""
        return await self._subscribe(
            ch.BOOK_TICKER, ch.book_ticker(*pairs), GateBookTicker.model_validate
        )

    # --- private channels --------------------------------------------------

    async def subscribe_orders(self, *pairs: str) -> EventStream[GateOrderUpdate]:
        """``spot.orders`` — order lifecycle. Defaults to ``!all``."""
        return await self._subscribe(
            ch.ORDERS,
            ch.orders(*pairs),
            GateOrderUpdate.model_validate,
            private=True,
        )

    async def subscribe_user_trades(self, *pairs: str) -> EventStream[GateUserTrade]:
        """``spot.usertrades`` — own executions. Defaults to ``!all``."""
        return await self._subscribe(
            ch.USER_TRADES,
            ch.user_trades(*pairs),
            GateUserTrade.model_validate,
            private=True,
        )

    async def subscribe_balances(self) -> EventStream[GateBalance]:
        """``spot.balances`` — spot account balance changes."""
        return await self._subscribe(
            ch.BALANCES,
            ch.balances(),
            GateBalance.model_validate,
            private=True,
        )

    # --- trading calls -----------------------------------------------------

    async def api_request(
        self,
        channel: str,
        req_param: Any,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Make a session-authenticated trading call and return ``data.result``.

        Requires a prior ``spot.login`` (done automatically by :meth:`connect`
        when credentials are set). The escape hatch for calls not wrapped below
        (``spot.order_amend``, ``spot.order_status``, ``spot.order_cancel_cp``):
        pass the channel and its ``req_param`` object straight through.
        """
        self._ensure_connected()
        self._ensure_auth(channel)
        if not self._logged_in:
            raise ExchangeError(
                f"{channel} requires spot.login; reconnect or call connect() "
                "with api_key/api_secret"
            )
        frame, req_id = session_api_frame(
            channel,
            req_param,
            channel_id=self.channel_id,
        )
        loop = asyncio.get_running_loop()
        pending = _Pending(
            future=loop.create_future(),
            channel=channel,
            event=ch.API,
            req_id=req_id,
        )
        self._pending.append(pending)
        try:
            await self.send(frame)
            resp = await asyncio.wait_for(
                pending.future, timeout=timeout or self.ack_timeout
            )
        except TimeoutError as exc:
            raise GateWsError(
                None,
                f"no reply to {channel} within {timeout or self.ack_timeout}s",
                channel=channel,
            ) from exc
        finally:
            if pending in self._pending:
                self._pending.remove(pending)
        resp.raise_for_error()
        return resp.result

    async def place_order(
        self,
        *,
        currency_pair: str,
        side: Side | str,
        amount: Decimal | str,
        price: Decimal | str | None = None,
        type: OrderType | str = OrderType.LIMIT,
        account: str = "spot",
        time_in_force: str | None = None,
        text: str | None = None,
        **extra: Any,
    ) -> GateOrderAck:
        """``spot.order_place``.

        ``text`` is Gate's client order id and is wrapped to ``t-<text>`` for
        you. For a market order Gate reads ``amount`` as quote currency on a
        buy and base currency on a sell.
        """
        param: dict[str, Any] = {
            "currency_pair": currency_pair,
            "type": str(type),
            "account": account,
            "side": str(side),
            "amount": str(amount),
        }
        if price is not None:
            param["price"] = str(price)
        if time_in_force:
            param["time_in_force"] = time_in_force
        if text:
            param["text"] = to_text(text)
        param.update(extra)
        result = await self.api_request(ch.ORDER_PLACE, param)
        return GateOrderAck.model_validate(result)

    async def cancel_order(
        self,
        order_id: str,
        *,
        currency_pair: str,
        account: str | None = None,
    ) -> GateOrderAck:
        """``spot.order_cancel`` — cancel one order.

        ``order_id`` is Gate's numeric id, or a client order id in ``t-`` form
        (pass it through :func:`~mftik.exchange.gate.spot.to_text` first, or hand
        it in already prefixed).
        """
        param: dict[str, Any] = {
            "order_id": order_id,
            "currency_pair": currency_pair,
        }
        if account:
            param["account"] = account
        result = await self.api_request(ch.ORDER_CANCEL, param)
        return GateOrderAck.model_validate(result)

    async def cancel_orders(
        self, orders: list[dict[str, Any]]
    ) -> list[GateOrderAck]:
        """``spot.order_cancel_ids`` — batch cancel.

        Each entry is ``{"id": ..., "currency_pair": ...}``. Legs fail
        independently, so check ``succeeded`` on every element of the result
        rather than assuming the call either worked or raised.
        """
        result = await self.api_request(ch.ORDER_CANCEL_IDS, orders)
        rows = result if isinstance(result, list) else [result]
        return [GateOrderAck.model_validate(row) for row in rows]

    # --- loops -------------------------------------------------------------

    async def _ping_loop(self) -> None:
        while not self._closing:
            await asyncio.sleep(self.ping_interval)
            if self._closing:
                return
            try:
                await self.ping()
            except (ConnectionClosed, ExchangeNotConnectedError):
                # Socket is down; the read loop owns reconnection. Skip this
                # tick and try again once it has a fresh one.
                continue
            except Exception:
                logger.exception("gate.spot ping failed")

    async def _read_loop(self) -> None:
        retries = 0
        while not self._closing:
            reason: object
            try:
                await self._pump()
                # A clean server-side close ends the iteration without raising,
                # so this is a disconnect too — not a reason to stop reading.
                reason = "server closed the connection"
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                reason = exc
            if self._closing:
                return
            if not self.reconnect:
                logger.warning("gate.spot connection lost: %s", reason)
                self._fail_streams()
                return
            retries += 1
            if 0 <= self.max_retries < retries:
                logger.error(
                    "gate.spot giving up after %s reconnect attempts", retries
                )
                self._fail_streams()
                return
            delay = min(
                self.retry_backoff * (2 ** (retries - 1)), self.max_retry_backoff
            )
            logger.warning(
                "gate.spot reconnecting in %.1fs (attempt %s): %s",
                delay,
                retries,
                reason,
            )
            await asyncio.sleep(delay)
            try:
                await self._open()
                self._logged_in = False
                if self.authenticated:
                    await self._authenticate_socket()
                await self._resubscribe()
            except Exception:
                logger.exception("gate.spot reconnect failed")
                continue
            self.stats.reconnects += 1
            # Whatever happened while the socket was down was not reported.
            # Tell the owner so it can rebuild state instead of trusting a
            # book that stopped being updated at some unknown point.
            self._fire_reconnect()
            retries = 0

    async def _pump(self) -> None:
        assert self._conn is not None
        conn = self._conn
        async for raw in conn:
            self.stats.frames += 1
            try:
                message = json.loads(raw)
            except ValueError:
                logger.warning("gate.spot non-JSON frame: %r", raw[:200])
                continue
            if not isinstance(message, dict):
                continue
            self._dispatch(GateResponse(message))

    def _dispatch(self, resp: GateResponse) -> None:
        if resp.channel == ch.PONG:
            self.stats.last_pong_at = asyncio.get_running_loop().time()
            return

        if resp.ack:
            # Bare acknowledgement ahead of the real reply — nothing to do.
            return

        if resp.is_api or resp.event in (ch.SUBSCRIBE, ch.UNSUBSCRIBE):
            self._resolve_ack(resp)
            return

        if resp.error is not None:
            logger.warning("gate.spot error on %s: %s", resp.channel, resp.error)
            return

        for sub in [s for s in self._subs if s.channel == resp.channel]:
            for row in resp.rows():
                try:
                    sub.stream.push(sub.parse(row))
                except Exception:
                    logger.exception(
                        "gate.spot failed to parse %s row: %r", resp.channel, row
                    )

    def _resolve_ack(self, resp: GateResponse) -> None:
        # Trading calls echo req_id, so correlate on it and fall back to
        # channel+event (which is all a subscribe ack gives us).
        if resp.req_id:
            for pending in self._pending:
                if pending.req_id == resp.req_id:
                    if not pending.future.done():
                        pending.future.set_result(resp)
                    return
        # A subscribe ack carries nothing to correlate on but channel+event, so
        # concurrent subscribes to one channel are told apart by arrival order:
        # the Nth ack answers the Nth waiter still outstanding. Skipping the
        # ones already resolved matters — a caller that has been given its ack
        # but has not resumed to remove itself is still in this list, and
        # stopping there would strand every later waiter until its timeout.
        for pending in self._pending:
            if pending.channel != resp.channel or pending.event != resp.event:
                continue
            if pending.future.done():
                continue
            pending.future.set_result(resp)
            return
        logger.debug("gate.spot unmatched reply %s/%s", resp.channel, resp.event)

    async def _resubscribe(self) -> None:
        """Replay every live subscription onto a fresh socket."""
        seen: dict[tuple[str, tuple[str, ...]], _Sub] = {}
        for sub in self._subs:
            seen.setdefault((sub.channel, tuple(sub.payload)), sub)
        self._ledger.clear()
        if not seen:
            return

        async def send(keys: list[tuple[str, tuple[str, ...]]]) -> None:
            for key in keys:
                sub = seen[key]
                frame = request_frame(
                    sub.channel,
                    ch.SUBSCRIBE,
                    sub.payload,
                    api_key=self.api_key if sub.private else None,
                    api_secret=self.api_secret if sub.private else None,
                )
                await self.send(frame)

        await self._ledger.acquire(list(seen), send)
        logger.info("gate.spot resubscribed %s channels", len(seen))

    def _fail_streams(self) -> None:
        self._connected = False
        self._logged_in = False
        self._ledger.clear()
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()
