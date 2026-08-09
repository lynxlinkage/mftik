"""Bybit's order-entry socket — ``wss://stream.bybit.com/v5/trade``.

Request/reply only. It subscribes to nothing and pushes nothing: an order sent
here is acknowledged with two ids and then reported on the private stream
(:mod:`.account`), which is why the two sockets are held together by the
connector and why neither is much use without the other.

It exists because it is faster than REST for the same calls — one authenticated
connection, no TLS handshake and no signature per order — and it takes the same
arguments, so :mod:`.rest` remains a working fallback for a caller that would
rather not hold a second socket open.

**The frame carries a header, not a signature.** ``op: auth`` authenticates the
connection once (:meth:`_on_open`), and every later frame carries only the
``X-BAPI-TIMESTAMP`` and ``X-BAPI-RECV-WINDOW`` a REST call would put in
headers. Those are Bybit's staleness check rather than part of any signature,
and a frame without them is refused however the connection was authenticated —
which is the same shape as Binance's post-logon calls still needing a clock.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from mft.exchange.bybit import channels as ch
from mft.exchange.bybit.models import BybitOrderAck
from mft.exchange.bybit.protocol import (
    AUTH,
    BYBIT_WS_TRADE_URL,
    DEFAULT_RECV_WINDOW_MS,
    BybitAuthError,
    BybitResponse,
    BybitWsError,
    auth_frame,
    trade_frame,
)
from mft.exchange.bybit.socket import DEFAULT_PING_INTERVAL, BybitSocket

logger = logging.getLogger(__name__)


class BybitTradeSocket(BybitSocket):
    """Bybit order entry over WebSocket.

    ::

        async with BybitTradeSocket(api_key=k, api_secret=s) as trade:
            ack = await trade.place_order(
                category="spot", symbol="BTCUSDT", side="buy",
                order_type="limit", qty=Decimal("0.001"),
                price=Decimal("60000"), order_link_id="my-id",
            )
            await trade.cancel_order(
                category="spot", symbol="BTCUSDT", order_link_id="my-id",
            )

    Every call names its ``category``: this one connection trades all of
    Bybit's books, and the category is what says which one an order is for.
    """

    name = "bybit.trade"

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        url: str = BYBIT_WS_TRADE_URL,
        recv_window: int = DEFAULT_RECV_WINDOW_MS,
        auth_window_ms: int | None = None,
        referer: str | None = None,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        ping_interval: float = DEFAULT_PING_INTERVAL,
    ) -> None:
        super().__init__(
            url,
            ack_timeout=ack_timeout,
            reconnect=reconnect,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            max_retry_backoff=max_retry_backoff,
            ping_interval=ping_interval,
        )
        if not api_key or not api_secret:
            raise BybitAuthError(
                "api_key and api_secret are required; Bybit's trade socket "
                "authenticates before it will accept an order"
            )
        self.api_key = api_key
        self._api_secret = api_secret
        #: How far a frame may lag Bybit's clock, in ms. Part of every frame's
        #: header; widening it widens the window in which a replayed order is
        #: still accepted, so it is opt-in.
        self.recv_window = recv_window
        self.auth_window_ms = auth_window_ms
        #: Broker referral tag, if the credential is under one.
        self.referer = referer
        self._authenticated = False

    # --- lifecycle ---------------------------------------------------------

    @property
    def authenticated(self) -> bool:
        """Whether ``op: auth`` has succeeded on the current socket."""
        return self._authenticated

    async def _on_open(self) -> None:
        self._authenticated = False
        kwargs: dict[str, Any] = {}
        if self.auth_window_ms is not None:
            kwargs["window_ms"] = self.auth_window_ms
        frame, req_id = auth_frame(
            api_key=self.api_key, api_secret=self._api_secret, **kwargs
        )
        await self.handshake(frame, req_id, op=AUTH)
        self._authenticated = True
        logger.info("%s authenticated key=%s…", self.name, self.api_key[:6])

    def _teardown(self) -> None:
        self._authenticated = False

    def _push(self, resp: BybitResponse) -> None:
        # Nothing subscribes here, and Bybit pushes nothing to this socket. A
        # frame that arrives anyway is worth a line rather than a silence.
        logger.debug("%s ignoring unexpected push %r", self.name, resp)

    # --- calls -------------------------------------------------------------

    async def call(
        self,
        op: str,
        args: dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> Any:
        """Send one op and return its ``data``.

        The escape hatch for ops not wrapped below — the batch forms, mostly.
        Refused here rather than at the venue if the connection has not
        authenticated: an order that goes out on an unauthenticated socket is
        an order whose fate we would have to ask about.
        """
        self._ensure_connected()
        if not self._authenticated:
            raise BybitWsError(
                None,
                "trade socket is not authenticated; the connection dropped and "
                "has not finished reconnecting",
                op=op,
            )
        frame, req_id = trade_frame(
            op,
            [args],
            recv_window=self.recv_window,
            referer=self.referer,
        )
        resp = await self.request(frame, req_id, op=op, timeout=timeout)
        return resp.data

    async def place_order(
        self,
        *,
        category: str,
        symbol: str,
        side: str,
        order_type: str,
        qty: Decimal | str,
        price: Decimal | str | None = None,
        time_in_force: str | None = None,
        order_link_id: str | None = None,
        market_unit: str | None = None,
        **extra: Any,
    ) -> BybitOrderAck:
        """``order.create``.

        ``side`` and ``order_type`` are title-cased for Bybit, which spells
        them ``Buy``/``Sell`` and ``Limit``/``Market`` and refuses any other
        casing — so either our enums or the venue's own spelling can be passed.

        ``market_unit`` is the one Bybit-specific thing a caller must not
        forget on spot: a spot **market buy** sizes in the *quote* currency by
        default, so ``qty=0.5`` on BTCUSDT means half a dollar unless
        ``marketUnit="baseCoin"`` says otherwise. The connector sets it; it is
        exposed here because a caller using this class directly needs the same
        control.
        """
        args: dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": _title(side),
            "orderType": _title(order_type),
            "qty": qty,
        }
        if price is not None:
            args["price"] = price
        if time_in_force:
            args["timeInForce"] = time_in_force
        if order_link_id:
            args["orderLinkId"] = order_link_id
        if market_unit:
            args["marketUnit"] = market_unit
        args.update(extra)
        return BybitOrderAck.model_validate(
            await self.call(ch.ORDER_CREATE, args) or {}
        )

    async def cancel_order(
        self,
        *,
        category: str,
        symbol: str,
        order_id: str | None = None,
        order_link_id: str | None = None,
        **extra: Any,
    ) -> BybitOrderAck:
        """``order.cancel`` — by Bybit's id or by the id we gave it.

        Bybit needs the symbol either way: an order id alone does not identify
        an order to it.
        """
        args = _by_id(category, symbol, order_id, order_link_id, what="cancel")
        args.update(extra)
        return BybitOrderAck.model_validate(
            await self.call(ch.ORDER_CANCEL, args) or {}
        )

    async def amend_order(
        self,
        *,
        category: str,
        symbol: str,
        order_id: str | None = None,
        order_link_id: str | None = None,
        qty: Decimal | str | None = None,
        price: Decimal | str | None = None,
        **extra: Any,
    ) -> BybitOrderAck:
        """``order.amend`` — repricing or resizing in place.

        Kept because Bybit really does amend rather than cancel-replace: the
        order keeps its id and its queue position where the change allows it,
        which a cancel and a new order cannot.
        """
        args = _by_id(category, symbol, order_id, order_link_id, what="amend")
        if qty is not None:
            args["qty"] = qty
        if price is not None:
            args["price"] = price
        args.update(extra)
        return BybitOrderAck.model_validate(
            await self.call(ch.ORDER_AMEND, args) or {}
        )


def _title(value: Any) -> str:
    """``buy`` → ``Buy``. Bybit's enums are title-cased and it means it."""
    return str(value).strip().title()


def _by_id(
    category: str,
    symbol: str,
    order_id: str | None,
    order_link_id: str | None,
    *,
    what: str,
) -> dict[str, Any]:
    """Build the ``category`` + ``symbol`` + one-id args both calls take."""
    if not order_id and not order_link_id:
        raise BybitWsError(
            None,
            f"Bybit {what} needs orderId or orderLinkId, got neither",
            op=what,
        )
    args: dict[str, Any] = {"category": category, "symbol": symbol}
    if order_id:
        args["orderId"] = order_id
    if order_link_id:
        args["orderLinkId"] = order_link_id
    return args


__all__ = ["BybitTradeSocket"]
