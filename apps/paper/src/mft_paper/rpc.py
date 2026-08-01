"""Paper-engine request-reply handlers."""

from __future__ import annotations

import logging

from mft.broker import IncomingRequest
from mft.exchange import (
    InsufficientBalanceError,
    OrderError,
    PaperAuthError,
    PaperExchange,
)
from mft.exchange.models import Balance, Order, OrderBook, Ticker
from mft.protocol import (
    PAPER_AUTH,
    PAPER_CANCEL_BY_CLIENT_ORDER_ID,
    PAPER_CANCEL_ORDER,
    PAPER_ERROR,
    PAPER_FETCH_BALANCES,
    PAPER_FETCH_INSTRUMENTS,
    PAPER_FETCH_OPEN_ORDERS,
    PAPER_FETCH_ORDER,
    PAPER_FETCH_ORDER_BOOK,
    PAPER_FETCH_TICKER,
    PAPER_PLACE_ORDER,
    Envelope,
    PaperCancelRequest,
    PaperCredentials,
    PaperFetchBalancesRequest,
    PaperFetchOpenOrdersRequest,
    PaperFetchOrderBookRequest,
    PaperFetchOrderRequest,
    PaperFetchTickerRequest,
    PaperPlaceOrderRequest,
    RpcError,
    UntypedEnvelope,
)

logger = logging.getLogger(__name__)


async def dispatch(req: IncomingRequest, *, exchange: PaperExchange) -> None:
    try:
        if req.envelope.type == PAPER_AUTH:
            await _auth(req, exchange)
        elif req.envelope.type == PAPER_PLACE_ORDER:
            await _place(req, exchange)
        elif req.envelope.type == PAPER_CANCEL_ORDER:
            await _cancel(req, exchange, by_client=False)
        elif req.envelope.type == PAPER_CANCEL_BY_CLIENT_ORDER_ID:
            await _cancel(req, exchange, by_client=True)
        elif req.envelope.type == PAPER_FETCH_ORDER:
            await _fetch_order(req, exchange)
        elif req.envelope.type == PAPER_FETCH_OPEN_ORDERS:
            await _fetch_open(req, exchange)
        elif req.envelope.type == PAPER_FETCH_BALANCES:
            await _fetch_balances(req, exchange)
        elif req.envelope.type == PAPER_FETCH_INSTRUMENTS:
            await _fetch_instruments(req, exchange)
        elif req.envelope.type == PAPER_FETCH_TICKER:
            await _fetch_ticker(req, exchange)
        elif req.envelope.type == PAPER_FETCH_ORDER_BOOK:
            await _fetch_order_book(req, exchange)
        else:
            await _error(req, "unknown_type", f"unknown type: {req.envelope.type}")
    except Exception:
        logger.exception("paper rpc failed type=%s", req.envelope.type)
        await _error(req, "internal", "paper engine internal error")


def _authenticate(exchange: PaperExchange, creds: PaperCredentials) -> None:
    """Validate credentials; auto-register unknown keys with empty BTC book."""
    from decimal import Decimal

    try:
        exchange.authenticate(
            creds.api_key,
            creds.api_secret,
            passphrase=creds.passphrase,
        )
    except PaperAuthError as exc:
        if "unknown paper api_key" not in str(exc):
            raise
        exchange.register_api(
            creds.api_key,
            creds.api_secret,
            passphrase=creds.passphrase,
            balances={"BTC": Decimal("0"), "USDT": Decimal("100000")},
        )
        exchange.authenticate(
            creds.api_key,
            creds.api_secret,
            passphrase=creds.passphrase,
        )


async def _auth(req: IncomingRequest, exchange: PaperExchange) -> None:
    try:
        creds = PaperCredentials.model_validate(req.envelope.payload)
        _authenticate(exchange, creds)
    except PaperAuthError as exc:
        await _error(req, "auth", str(exc))
        return
    await req.reply(
        UntypedEnvelope.wrap(
            {"ok": True, "api_key": creds.api_key},
            type=PAPER_AUTH,
            source="paper",
        )
    )


async def _place(req: IncomingRequest, exchange: PaperExchange) -> None:
    from mft.exchange.models import PlaceOrderRequest

    try:
        body = PaperPlaceOrderRequest.model_validate(req.envelope.payload)
        _authenticate(exchange, body.credentials)
        order = await exchange.place_order(
            body.credentials.api_key,
            PlaceOrderRequest(
                symbol=body.symbol,
                side=body.side,
                type=body.type,
                qty=body.qty,
                price=body.price,
                client_order_id=body.client_order_id,
            ),
        )
    except PaperAuthError as exc:
        await _error(req, "auth", str(exc))
        return
    except InsufficientBalanceError as exc:
        await _error(req, "insufficient_balance", str(exc))
        return
    except OrderError as exc:
        await _error(req, "order", str(exc))
        return
    await req.reply(_model_reply(order, PAPER_PLACE_ORDER))


async def _cancel(
    req: IncomingRequest, exchange: PaperExchange, *, by_client: bool
) -> None:
    try:
        body = PaperCancelRequest.model_validate(req.envelope.payload)
        _authenticate(exchange, body.credentials)
        if by_client:
            if not body.client_order_id:
                raise OrderError("client_order_id required")
            order = await exchange.cancel_by_client_order_id(
                body.credentials.api_key, body.client_order_id
            )
        else:
            if not body.order_id:
                raise OrderError("order_id required")
            order = await exchange.cancel_order(
                body.credentials.api_key, body.order_id
            )
    except PaperAuthError as exc:
        await _error(req, "auth", str(exc))
        return
    except OrderError as exc:
        await _error(req, "order", str(exc))
        return
    type_ = (
        PAPER_CANCEL_BY_CLIENT_ORDER_ID if by_client else PAPER_CANCEL_ORDER
    )
    await req.reply(_model_reply(order, type_))


async def _fetch_order(req: IncomingRequest, exchange: PaperExchange) -> None:
    try:
        body = PaperFetchOrderRequest.model_validate(req.envelope.payload)
        _authenticate(exchange, body.credentials)
        order = exchange.get_order(body.order_id)
    except PaperAuthError as exc:
        await _error(req, "auth", str(exc))
        return
    except OrderError as exc:
        await _error(req, "order", str(exc))
        return
    await req.reply(_model_reply(order, PAPER_FETCH_ORDER))


async def _fetch_open(req: IncomingRequest, exchange: PaperExchange) -> None:
    try:
        body = PaperFetchOpenOrdersRequest.model_validate(req.envelope.payload)
        _authenticate(exchange, body.credentials)
        orders = exchange.list_open_orders(
            body.credentials.api_key, body.symbol
        )
    except PaperAuthError as exc:
        await _error(req, "auth", str(exc))
        return
    await req.reply(
        UntypedEnvelope.wrap(
            {"orders": [o.model_dump(mode="json") for o in orders]},
            type=PAPER_FETCH_OPEN_ORDERS,
            source="paper",
        )
    )


async def _fetch_balances(req: IncomingRequest, exchange: PaperExchange) -> None:
    try:
        body = PaperFetchBalancesRequest.model_validate(req.envelope.payload)
        _authenticate(exchange, body.credentials)
        balances = exchange.list_balances(body.credentials.api_key)
    except PaperAuthError as exc:
        await _error(req, "auth", str(exc))
        return
    await req.reply(
        UntypedEnvelope.wrap(
            {"balances": [b.model_dump(mode="json") for b in balances]},
            type=PAPER_FETCH_BALANCES,
            source="paper",
        )
    )


async def _fetch_instruments(req: IncomingRequest, exchange: PaperExchange) -> None:
    instruments = exchange.list_instruments()
    await req.reply(
        UntypedEnvelope.wrap(
            {"instruments": [i.model_dump(mode="json") for i in instruments]},
            type=PAPER_FETCH_INSTRUMENTS,
            source="paper",
        )
    )


async def _fetch_ticker(req: IncomingRequest, exchange: PaperExchange) -> None:
    try:
        body = PaperFetchTickerRequest.model_validate(req.envelope.payload)
        ticker = exchange.get_ticker(body.symbol)
    except Exception as exc:
        await _error(req, "order", str(exc))
        return
    await req.reply(_model_reply(ticker, PAPER_FETCH_TICKER))


async def _fetch_order_book(req: IncomingRequest, exchange: PaperExchange) -> None:
    try:
        body = PaperFetchOrderBookRequest.model_validate(req.envelope.payload)
        book = exchange.get_order_book(body.symbol, depth=body.depth)
    except Exception as exc:
        await _error(req, "order", str(exc))
        return
    await req.reply(_model_reply(book, PAPER_FETCH_ORDER_BOOK))


def _model_reply(
    model: Order | Balance | OrderBook | Ticker, type_: str
) -> UntypedEnvelope:
    return UntypedEnvelope.wrap(
        model.model_dump(mode="json"),
        type=type_,
        source="paper",
    )


async def _error(req: IncomingRequest, code: str, message: str) -> None:
    await req.reply(
        Envelope[RpcError].wrap(
            RpcError(code=code, message=message),
            type=PAPER_ERROR,
            source="paper",
            session_id=req.envelope.session_id,
        )
    )
