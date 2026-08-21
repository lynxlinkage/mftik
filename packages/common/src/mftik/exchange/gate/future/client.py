"""Gate USDT-perpetual WebSocket v4 — one socket for public, private, trading.

Same envelope as spot, different host (``fx-ws`` / ``usdt``) and channel
prefix. Credentials trigger ``futures.login``; the uid on that reply is what
private channel payloads need. Public connections send no keys.
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
from mftik.exchange.gate.future import channels as ch
from mftik.exchange.gate.future.models import (
    GateFuturesBalance,
    GateFuturesBookTicker,
    GateFuturesCandlestick,
    GateFuturesLiquidation,
    GateFuturesOrder,
    GateFuturesOrderBook,
    GateFuturesPosition,
    GateFuturesTicker,
    GateFuturesTrade,
    GateFuturesUserTrade,
    format_size,
    to_text,
)
from mftik.exchange.gate.future.protocol import (
    GATE_FUTURES_WS_URL,
    SIZE_DECIMAL_HEADER,
    GateResponse,
    GateWsError,
    login_frame,
    ping_frame,
    request_frame,
    session_api_frame,
)
from mftik.exchange.stream import EventStream

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class _Sub:
    channel: str
    payload: list[str]
    stream: EventStream[Any]
    parse: Callable[[dict[str, Any]], Any]
    private: bool = False


@dataclass
class _Pending:
    future: asyncio.Future[GateResponse]
    channel: str
    event: str
    req_id: str | None = None


@dataclass
class _Stats:
    reconnects: int = 0
    frames: int = 0
    last_pong_at: float = field(default=0.0)


class GateFuturesWebSocket:
    """Gate futures v4 WebSocket connectivity.

    Market data, account pushes and order entry share one socket::

        async with GateFuturesWebSocket(api_key=k, api_secret=s) as ws:
            trades = await ws.subscribe_trades("BTC_USDT")
            orders = await ws.subscribe_orders()
            ack = await ws.place_order(
                contract="BTC_USDT", size="10", price="60000",
            )
    """

    name = "gate.futures"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        url: str = GATE_FUTURES_WS_URL,
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
        self.channel_id = channel_id
        self.ping_interval = ping_interval
        self.ack_timeout = ack_timeout
        self.reconnect = reconnect
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.max_retry_backoff = max_retry_backoff
        self.close_timeout = close_timeout

        self._conn: ClientConnection | None = None
        self._subs: list[_Sub] = []
        self._pending: list[_Pending] = []
        self._tasks: list[asyncio.Task[Any]] = []
        self._connected = False
        self._closing = False
        self._reconnect_cbs: list = []
        self._logged_in = False
        self.uid: str = ""
        self.stats = _Stats()

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def authenticated(self) -> bool:
        return bool(self.api_key and self.api_secret)

    @property
    def logged_in(self) -> bool:
        return self._logged_in

    async def connect(self) -> None:
        if self._connected:
            return
        self._closing = False
        try:
            await self._open()
            if self.authenticated:
                await self._authenticate_socket()
            self._tasks = [
                asyncio.create_task(self._read_loop(), name="gate-futures-read"),
            ]
            if self.ping_interval > 0:
                self._tasks.append(
                    asyncio.create_task(
                        self._ping_loop(), name="gate-futures-ping"
                    )
                )
            self._connected = True
        except Exception:
            await self.close()
            raise

    async def close(self) -> None:
        self._closing = True
        self._connected = False
        self._logged_in = False
        self.uid = ""
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
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> GateFuturesWebSocket:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    def on_reconnect(self, callback) -> None:
        self._reconnect_cbs.append(callback)

    def _fire_reconnect(self) -> None:
        for cb in list(self._reconnect_cbs):
            try:
                result = cb()
            except Exception:
                logger.exception("gate.futures reconnect callback failed")
                continue
            if asyncio.iscoroutine(result):
                asyncio.create_task(result, name="gate-futures-reconnect-cb")

    async def _open(self) -> None:
        self._conn = await connect(
            self.url,
            additional_headers=SIZE_DECIMAL_HEADER,
            close_timeout=self.close_timeout,
        )

    async def _authenticate_socket(self) -> None:
        if self._conn is None or not self.api_key or not self.api_secret:
            raise ExchangeError(
                "futures.login requires a live socket and credentials"
            )
        frame, req_id = login_frame(
            api_key=self.api_key,
            api_secret=self.api_secret,
            channel=ch.LOGIN,
        )
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
                logger.warning(
                    "gate.futures non-JSON frame during login: %r", raw[:200]
                )
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
                uid = ""
                if isinstance(resp.result, dict):
                    uid = str(resp.result.get("uid") or "")
                if not uid:
                    raise ExchangeError("futures.login returned no uid")
                self.uid = uid
                self._logged_in = True
                logger.info("gate.futures logged in uid=%s", uid)
                return
            logger.warning(
                "gate.futures dropping pre-login frame channel=%s event=%s",
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

    def _require_uid(self) -> str:
        if not self.uid:
            raise ExchangeError(
                "private futures channels need the uid from futures.login"
            )
        return self.uid

    async def send(self, frame: dict[str, Any]) -> None:
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

    async def unsubscribe(
        self, channel: str, payload: list[str] | None = None, *, private: bool = False
    ) -> None:
        await self.request(channel, ch.UNSUBSCRIBE, payload, private=private)
        for sub in [s for s in self._subs if s.channel == channel]:
            sub.stream.close()

    async def ping(self) -> None:
        await self.send(ping_frame(ch.PING))

    async def _subscribe(
        self,
        channel: str,
        payload: list[str] | None,
        parse: Callable[[dict[str, Any]], T],
        *,
        private: bool = False,
    ) -> EventStream[T]:
        await self.request(channel, ch.SUBSCRIBE, payload, private=private)
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

    async def subscribe_tickers(self, *contracts: str) -> EventStream[GateFuturesTicker]:
        return await self._subscribe(
            ch.TICKERS, ch.tickers(*contracts), GateFuturesTicker.model_validate
        )

    async def subscribe_trades(self, *contracts: str) -> EventStream[GateFuturesTrade]:
        return await self._subscribe(
            ch.TRADES, ch.trades(*contracts), GateFuturesTrade.model_validate
        )

    async def subscribe_candlesticks(
        self, interval: str, contract: str
    ) -> EventStream[GateFuturesCandlestick]:
        return await self._subscribe(
            ch.CANDLESTICKS,
            ch.candlesticks(interval, contract),
            GateFuturesCandlestick.model_validate,
        )

    async def subscribe_order_book(
        self, contract: str, *, level: str = "20", interval: str = "1000ms"
    ) -> EventStream[GateFuturesOrderBook]:
        return await self._subscribe(
            ch.ORDER_BOOK,
            ch.order_book(contract, level=level, interval=interval),
            GateFuturesOrderBook.model_validate,
        )

    async def subscribe_book_ticker(
        self, *contracts: str
    ) -> EventStream[GateFuturesBookTicker]:
        return await self._subscribe(
            ch.BOOK_TICKER,
            ch.book_ticker(*contracts),
            GateFuturesBookTicker.model_validate,
        )

    async def subscribe_liquidations(
        self, *contracts: str
    ) -> EventStream[GateFuturesLiquidation]:
        return await self._subscribe(
            ch.PUBLIC_LIQUIDATES,
            ch.public_liquidates(*contracts),
            GateFuturesLiquidation.model_validate,
        )

    async def subscribe_orders(
        self, *contracts: str
    ) -> EventStream[GateFuturesOrder]:
        return await self._subscribe(
            ch.ORDERS,
            ch.orders(self._require_uid(), *contracts),
            GateFuturesOrder.model_validate,
            private=True,
        )

    async def subscribe_user_trades(
        self, *contracts: str
    ) -> EventStream[GateFuturesUserTrade]:
        return await self._subscribe(
            ch.USER_TRADES,
            ch.user_trades(self._require_uid(), *contracts),
            GateFuturesUserTrade.model_validate,
            private=True,
        )

    async def subscribe_positions(
        self, *contracts: str
    ) -> EventStream[GateFuturesPosition]:
        return await self._subscribe(
            ch.POSITIONS,
            ch.positions(self._require_uid(), *contracts),
            GateFuturesPosition.model_validate,
            private=True,
        )

    async def subscribe_balances(self) -> EventStream[GateFuturesBalance]:
        return await self._subscribe(
            ch.BALANCES,
            ch.balances(self._require_uid()),
            GateFuturesBalance.model_validate,
            private=True,
        )

    async def api_request(
        self,
        channel: str,
        req_param: Any,
        *,
        timeout: float | None = None,
    ) -> Any:
        self._ensure_connected()
        self._ensure_auth(channel)
        if not self._logged_in:
            raise ExchangeError(
                f"{channel} requires futures.login; reconnect or call connect() "
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
        contract: str,
        size: Decimal | str,
        price: Decimal | str | None = None,
        tif: str | None = None,
        text: str | None = None,
        reduce_only: bool = False,
        **extra: Any,
    ) -> GateFuturesOrder:
        """``futures.order_place``. ``size`` is signed contracts on the wire."""
        param: dict[str, Any] = {
            "contract": contract,
            "size": format_size(Decimal(str(size))),
        }
        if price is not None:
            param["price"] = str(price)
        if tif:
            param["tif"] = tif
        if text:
            param["text"] = to_text(text)
        if reduce_only:
            param["reduce_only"] = True
        param.update(extra)
        result = await self.api_request(ch.ORDER_PLACE, param)
        return GateFuturesOrder.model_validate(result)

    async def cancel_order(
        self,
        order_id: str,
        *,
        contract: str | None = None,
    ) -> GateFuturesOrder:
        param: dict[str, Any] = {"order_id": order_id}
        if contract:
            param["contract"] = contract
        result = await self.api_request(ch.ORDER_CANCEL, param)
        return GateFuturesOrder.model_validate(result)

    async def fetch_order(
        self, order_id: str, *, contract: str | None = None
    ) -> GateFuturesOrder:
        param: dict[str, Any] = {"order_id": order_id}
        if contract:
            param["contract"] = contract
        result = await self.api_request(ch.ORDER_STATUS, param)
        return GateFuturesOrder.model_validate(result)

    async def list_orders(
        self, *, contract: str | None = None, status: str = "open"
    ) -> list[GateFuturesOrder]:
        param: dict[str, Any] = {"status": status}
        if contract:
            param["contract"] = contract
        result = await self.api_request(ch.ORDER_LIST, param)
        rows = result
        if isinstance(result, dict):
            rows = result.get("orders") or result.get("result") or []
        return [GateFuturesOrder.model_validate(row) for row in rows or []]

    async def _ping_loop(self) -> None:
        while not self._closing:
            await asyncio.sleep(self.ping_interval)
            if self._closing:
                return
            try:
                await self.ping()
            except (ConnectionClosed, ExchangeNotConnectedError):
                continue
            except Exception:
                logger.exception("gate.futures ping failed")

    async def _read_loop(self) -> None:
        retries = 0
        while not self._closing:
            reason: object
            try:
                await self._pump()
                reason = "server closed the connection"
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                reason = exc
            if self._closing:
                return
            if not self.reconnect:
                logger.warning("gate.futures connection lost: %s", reason)
                self._fail_streams()
                return
            retries += 1
            if 0 <= self.max_retries < retries:
                logger.error(
                    "gate.futures giving up after %s reconnect attempts", retries
                )
                self._fail_streams()
                return
            delay = min(
                self.retry_backoff * (2 ** (retries - 1)), self.max_retry_backoff
            )
            logger.warning(
                "gate.futures reconnecting in %.1fs (attempt %s): %s",
                delay,
                retries,
                reason,
            )
            await asyncio.sleep(delay)
            try:
                await self._open()
                self._logged_in = False
                self.uid = ""
                if self.authenticated:
                    await self._authenticate_socket()
                await self._resubscribe()
            except Exception:
                logger.exception("gate.futures reconnect failed")
                continue
            self.stats.reconnects += 1
            self._fire_reconnect()
            retries = 0

    async def _pump(self) -> None:
        assert self._conn is not None
        async for raw in self._conn:
            self.stats.frames += 1
            try:
                message = json.loads(raw)
            except ValueError:
                logger.warning("gate.futures non-JSON frame: %r", raw[:200])
                continue
            if not isinstance(message, dict):
                continue
            self._dispatch(GateResponse(message))

    def _dispatch(self, resp: GateResponse) -> None:
        if resp.channel == ch.PONG:
            self.stats.last_pong_at = asyncio.get_running_loop().time()
            return
        if resp.ack:
            return
        if resp.is_api or resp.event in (ch.SUBSCRIBE, ch.UNSUBSCRIBE):
            self._resolve_ack(resp)
            return
        if resp.error is not None:
            logger.warning("gate.futures error on %s: %s", resp.channel, resp.error)
            return
        for sub in [s for s in self._subs if s.channel == resp.channel]:
            for row in resp.rows():
                try:
                    sub.stream.push(sub.parse(row))
                except Exception:
                    logger.exception(
                        "gate.futures failed to parse %s row: %r",
                        resp.channel,
                        row,
                    )

    def _resolve_ack(self, resp: GateResponse) -> None:
        if resp.req_id:
            for pending in self._pending:
                if pending.req_id == resp.req_id:
                    if not pending.future.done():
                        pending.future.set_result(resp)
                    return
        for pending in self._pending:
            if pending.channel != resp.channel or pending.event != resp.event:
                continue
            if pending.future.done():
                continue
            pending.future.set_result(resp)
            return
        logger.debug("gate.futures unmatched reply %s/%s", resp.channel, resp.event)

    async def _resubscribe(self) -> None:
        for sub in list(self._subs):
            frame = request_frame(
                sub.channel,
                ch.SUBSCRIBE,
                sub.payload,
                api_key=self.api_key if sub.private else None,
                api_secret=self.api_secret if sub.private else None,
            )
            await self.send(frame)
        logger.info("gate.futures resubscribed %s channels", len(self._subs))

    def _fail_streams(self) -> None:
        self._connected = False
        self._logged_in = False
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()


__all__ = ["GATE_FUTURES_WS_URL", "GateFuturesWebSocket"]
