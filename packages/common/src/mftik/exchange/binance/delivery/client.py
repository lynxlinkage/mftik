"""Binance COIN-M WebSocket API — order entry and account reads.

Everything ``ws-dapi.binance.com`` answers on request::

    async with BinanceDeliveryWsApi(api_key=k, api_secret=pem) as api:
        ack = await api.place_order(
            symbol="BTCUSD_PERP", side="buy", type="limit",
            quantity="1", price="60000", time_in_force="GTC",
            client_order_id="my-id",
        )
        await api.cancel_order("BTCUSD_PERP", order_id=ack.order_id)
        positions = await api.fetch_positions()

**The logon is the whole authentication story**, as on the other Binance
planes: Binance signs it with the Ed25519 key and then authenticates the
*connection*. A lost socket is a lost authentication, so :meth:`_on_open`
re-runs the logon on every reconnect before anything else.

**Nothing is pushed to this socket.** Account events are a listen key
handed out here (:meth:`start_user_stream`) and read on ``dstream``
(:class:`~mftik.exchange.binance.delivery.user.BinanceDeliveryUserStream`).
This class has no ``_push``.

``quantity`` is a **contract count**. dapi ``contractSize`` is USD per
contract; multiplying by it invents a dollar notional, not BTC.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from mftik.exchange.binance.delivery import methods as m
from mftik.exchange.binance.delivery.models import (
    BinanceDeliveryBalance,
    BinanceDeliveryOrderAck,
    BinanceDeliveryPosition,
)
from mftik.exchange.binance.delivery.protocol import (
    BINANCE_DELIVERY_WS_API_URL,
    load_private_key,
    logon_frame,
    now_ms,
    request_frame,
    signed_frame,
)
from mftik.exchange.binance.socket import BinanceSocket
from mftik.exchange.errors import ExchangeError
from mftik.exchange.models import OrderType, Side

logger = logging.getLogger(__name__)


class BinanceDeliveryWsApi(BinanceSocket):
    """Binance COIN-M WebSocket API connectivity."""

    name = "binance.delivery"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        url: str = BINANCE_DELIVERY_WS_API_URL,
        recv_window: int | None = None,
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
        self.api_key = api_key
        self.recv_window = recv_window
        self._key = load_private_key(api_secret) if api_secret else None
        self._logged_on = False

    @property
    def authenticated(self) -> bool:
        return bool(self.api_key and self._key)

    @property
    def logged_on(self) -> bool:
        """Whether ``session.logon`` has succeeded on the current socket."""
        return self._logged_on

    async def _on_open(self) -> None:
        self._logged_on = False
        if not self.authenticated:
            return
        assert self.api_key and self._key
        frame, req_id = logon_frame(
            api_key=self.api_key,
            private_key=self._key,
            recv_window=self.recv_window,
        )
        await self.handshake(frame, req_id, method=m.SESSION_LOGON)
        self._logged_on = True
        logger.info("%s session logged on key=%s…", self.name, self.api_key[:6])

    def _teardown(self) -> None:
        self._logged_on = False

    async def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Make one WebSocket API call and return its ``result``.

        Signed methods still carry a ``timestamp`` after logon — Binance
        answers ``-1102`` without one. Listen-key methods are key-only: a
        timestamp is ``-1101``.
        """
        self._ensure_connected()
        signed = method in m.SIGNED
        key_only = method in m.API_KEY_ONLY
        if (signed or key_only) and not self.authenticated:
            kind = "trading call" if method in m.TRADING else "authenticated call"
            raise ExchangeError(
                f"{method} is a {kind}; api_key and api_secret are required"
            )

        if signed and not self._logged_on:
            assert self.api_key and self._key
            frame, req_id = signed_frame(
                method,
                params,
                api_key=self.api_key,
                private_key=self._key,
                recv_window=self.recv_window,
            )
        else:
            frame, req_id = request_frame(method, self._stamped(method, params))
        resp = await self.request(frame, req_id, method=method, timeout=timeout)
        return resp.result

    def _stamped(
        self, method: str, params: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        if method in m.API_KEY_ONLY:
            if self._logged_on or not self.api_key:
                return params
            body = dict(params or {})
            body["apiKey"] = self.api_key
            return body
        if method not in m.SIGNED:
            return params
        body = dict(params or {})
        body["timestamp"] = now_ms()
        if self.recv_window is not None:
            body["recvWindow"] = self.recv_window
        return body

    async def place_order(
        self,
        *,
        symbol: str,
        side: Side | str,
        type: OrderType | str = OrderType.LIMIT,
        quantity: Decimal | str | None = None,
        price: Decimal | str | None = None,
        time_in_force: str | None = None,
        client_order_id: str | None = None,
        position_side: str | None = None,
        reduce_only: bool | None = None,
        **extra: Any,
    ) -> BinanceDeliveryOrderAck:
        """``order.place``. ``quantity`` is contracts, not base."""
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": str(side).upper(),
            "type": str(type).upper(),
        }
        if quantity is not None:
            params["quantity"] = quantity
        if price is not None:
            params["price"] = price
        if time_in_force:
            params["timeInForce"] = time_in_force.upper()
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        if position_side:
            params["positionSide"] = position_side.upper()
        if reduce_only is not None:
            params["reduceOnly"] = reduce_only
        params.update(extra)
        result = await self.call(m.ORDER_PLACE, params)
        return BinanceDeliveryOrderAck.model_validate(result)

    async def cancel_order(
        self,
        symbol: str,
        *,
        order_id: str | int | None = None,
        client_order_id: str | None = None,
        **extra: Any,
    ) -> BinanceDeliveryOrderAck:
        """``order.cancel`` — by venue id or by the id we gave it."""
        params = _by_id(symbol, order_id, client_order_id, what="cancel")
        params.update(extra)
        result = await self.call(m.ORDER_CANCEL, params)
        return BinanceDeliveryOrderAck.model_validate(result)

    async def query_order(
        self,
        symbol: str,
        *,
        order_id: str | int | None = None,
        client_order_id: str | None = None,
    ) -> BinanceDeliveryOrderAck:
        """``order.status`` — what became of one order."""
        result = await self.call(
            m.ORDER_STATUS, _by_id(symbol, order_id, client_order_id, what="query")
        )
        return BinanceDeliveryOrderAck.model_validate(result)

    async def fetch_balances(self) -> list[BinanceDeliveryBalance]:
        """``account.balance`` — every asset in the coin-margined wallet."""
        result = await self.call(m.ACCOUNT_BALANCE)
        return [BinanceDeliveryBalance.model_validate(row) for row in result or []]

    async def fetch_positions(self) -> list[BinanceDeliveryPosition]:
        """``account.position`` — the contracts this account holds.

        Flat rows are kept. dapi's unversioned method can answer for every
        listed contract; a zero is how the OMS learns to drop a stale one.
        """
        result = await self.call(m.ACCOUNT_POSITION)
        return [BinanceDeliveryPosition.model_validate(row) for row in result or []]

    async def start_user_stream(self) -> str:
        """``userDataStream.start`` — a listen key for the account feed.

        Events arrive on a socket of their own. Handing the key back rather
        than opening that socket here keeps this class request/reply only.
        """
        result = await self.call(m.USER_DATA_STREAM_START)
        key = str((result or {}).get("listenKey") or "")
        if not key:
            raise ExchangeError(
                f"{m.USER_DATA_STREAM_START} answered without a listenKey: {result!r}"
            )
        return key

    async def ping_user_stream(self) -> None:
        """``userDataStream.ping`` — another 60 minutes for the current key."""
        await self.call(m.USER_DATA_STREAM_PING)

    async def stop_user_stream(self) -> None:
        """``userDataStream.stop`` — close the account feed now."""
        await self.call(m.USER_DATA_STREAM_STOP)


def _by_id(
    symbol: str,
    order_id: str | int | None,
    client_order_id: str | None,
    *,
    what: str,
) -> dict[str, Any]:
    if order_id is None and not client_order_id:
        raise ExchangeError(
            f"Binance delivery {what} needs orderId or origClientOrderId, "
            f"got neither"
        )
    params: dict[str, Any] = {"symbol": symbol}
    if order_id is not None:
        params["orderId"] = int(order_id)
    if client_order_id:
        params["origClientOrderId"] = client_order_id
    return params


__all__ = ["BinanceDeliveryWsApi"]
