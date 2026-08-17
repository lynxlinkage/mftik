"""Bybit v5 framing, HMAC signing, and the one response envelope.

Bybit is a **unified** venue: one credential and one API version cover spot,
linear perps, inverse perps and options, and the market an instrument trades
on is a ``category`` parameter rather than a different endpoint with different
keys. That is why there is no ``bybit/spot`` package here as there is for Gate
and Binance — see :mod:`mftik.exchange.tickers` for why the platform names that
axis on the instrument instead.

What *is* split is the sockets, and along an axis that has nothing to do with
the account:

* :data:`BYBIT_WS_PRIVATE_URL` — account pushes. Orders, executions, wallet
  and positions, for every category at once.
* :data:`BYBIT_WS_TRADE_URL` — order entry. Request/reply only; it pushes
  nothing, and an order placed here is reported on the private stream.
* :func:`public_url` — market pushes, one socket **per category**. This is the
  one place the category reaches a URL: ``.../v5/public/spot`` and
  ``.../v5/public/linear`` are different connections carrying the same topic
  names for different instruments.

All four speak one envelope::

    {"op": ..., "args": [...], "req_id": ...}          out
    {"op": ..., "success": true, "req_id": ...}        reply
    {"topic": ..., "type": "snapshot", "data": [...]}  push

with the trade socket differing only in spelling — ``reqId`` for the id and
``retCode``/``retMsg`` where the others say ``success``/``ret_msg``. Both are
normalized onto :class:`BybitResponse`, so one read loop routes all of them.

**Signing is HMAC-SHA256, over two different strings.** A socket authenticates
once per connection with ``GET/realtime<expires>``; a REST call signs
``<timestamp><api_key><recv_window><query-or-body>`` per request. The socket
form is the one the order path uses, so — as on Binance, and unlike Gate — no
crypto happens per order.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from decimal import Decimal
from typing import Any

from mftik.exchange.errors import ExchangeError
from mftik.exchange.tickers import Category

# --- endpoints -------------------------------------------------------------

#: Account pushes: order, execution, wallet, position — all categories.
BYBIT_WS_PRIVATE_URL = "wss://stream.bybit.com/v5/private"
#: Order entry. Request/reply only.
BYBIT_WS_TRADE_URL = "wss://stream.bybit.com/v5/trade"
#: Market pushes. ``{}`` is the product category — see :func:`public_url`.
BYBIT_WS_PUBLIC_URL = "wss://stream.bybit.com/v5/public/{product}"
#: Everything request/reply that is not order entry.
BYBIT_REST_URL = "https://api.bybit.com"

#: Testnet, for smoke-testing a credential without risking one.
BYBIT_WS_PRIVATE_TESTNET_URL = "wss://stream-testnet.bybit.com/v5/private"
BYBIT_WS_TRADE_TESTNET_URL = "wss://stream-testnet.bybit.com/v5/trade"
BYBIT_WS_PUBLIC_TESTNET_URL = "wss://stream-testnet.bybit.com/v5/public/{product}"
BYBIT_REST_TESTNET_URL = "https://api-testnet.bybit.com"

# --- product categories ----------------------------------------------------

#: Bybit's own spelling of the four books. Every REST call and every public
#: socket names one; the private socket names none, because it carries all of
#: them.
SPOT = "spot"
LINEAR = "linear"
INVERSE = "inverse"
OPTION = "option"

PRODUCTS = (SPOT, LINEAR, INVERSE, OPTION)

#: Our :class:`~mftik.exchange.tickers.Category` → Bybit's ``category``.
#:
#: ``PERP`` maps to ``linear`` and not to ``inverse``, which is a real choice
#: rather than an oversight. Bybit splits perpetuals by what collateralises
#: them — ``BTCUSDT`` is margined in USDT (linear), ``BTCUSD`` in BTC
#: (inverse) — and our ``Perp`` says nothing about that. Every inverse contract
#: is quoted in USD while its linear twin is quoted in USDT, so the two are
#: different symbols and the platform can carry both; what it cannot do is tell
#: them apart from the category alone. Linear is the default because it is what
#: a USDT-quoted symbol means, and the inverse book stays reachable one layer
#: down: :func:`public_url` and every REST call take the product string
#: directly.
_PRODUCT_BY_CATEGORY: dict[Category, str] = {
    Category.SPOT: SPOT,
    Category.PERP: LINEAR,
    Category.FUTURE: LINEAR,
    Category.OPTION: OPTION,
}


def product_of(category: Category | str) -> str:
    """Bybit's ``category`` for one of ours. Raises on one it does not trade."""
    if isinstance(category, str) and category in PRODUCTS:
        return category
    resolved = category if isinstance(category, Category) else Category(category)
    found = _PRODUCT_BY_CATEGORY.get(resolved)
    if found is None:
        raise ExchangeError(f"Bybit has no product for category {category!r}")
    return found


def public_url(product: str, *, testnet: bool = False) -> str:
    """The market-push socket for one category.

    One connection per category, by Bybit's design: the topic names are the
    same on every one of them, so ``publicTrade.BTCUSDT`` means the spot tape
    or the perp tape depending only on which socket it was subscribed on.
    """
    if product not in PRODUCTS:
        raise ExchangeError(
            f"unknown Bybit product {product!r}; known: {', '.join(PRODUCTS)}"
        )
    template = BYBIT_WS_PUBLIC_TESTNET_URL if testnet else BYBIT_WS_PUBLIC_URL
    return template.format(product=product)


# --- errors ----------------------------------------------------------------

#: What a successful ``retCode`` is.
RET_OK = 0

#: "There is no such order" — as opposed to a call that failed. Bybit answers a
#: different code per book for the same fact: ``110001`` on the derivatives
#: side, ``170213`` on spot.
NOT_FOUND_CODES = frozenset({110001, 170213})

#: The credential itself is the problem: unknown key, bad signature, expired
#: timestamp, or a key without the permission for the call.
AUTH_CODES = frozenset({10002, 10003, 10004, 10005, 10010, 33004})


class BybitError(ExchangeError):
    """Bybit refused a call, on either transport.

    ``code`` is kept unformatted as well as folded into the string form: TD and
    MD normalize on it and should not have to parse it back out of
    ``str(exc)``. Bybit's codes are positive integers and mean the same thing
    over the socket and over REST — ``110007`` is insufficient balance whether
    the order went out on ``order.create`` or ``POST /v5/order/create`` — which
    is why one error type covers both and the transports only add what is
    theirs.
    """

    def __init__(self, code: int | None, message: str, *, op: str = "") -> None:
        self.code = code
        self.op = op
        prefix = f"{op}: " if op else ""
        super().__init__(f"{prefix}[{code}] {message}")

    @property
    def not_found(self) -> bool:
        """Whether this says the order does not exist, rather than failing."""
        return self.code in NOT_FOUND_CODES


class BybitWsError(BybitError):
    """A WebSocket op came back unsuccessful."""


class BybitRestError(BybitError):
    """A REST call failed, in the body or in the status line.

    ``status`` says something ``code`` does not: whether we were rate limited
    (429) or refused on the merits (a 200 carrying a non-zero ``retCode``,
    which is Bybit's normal way of rejecting).
    """

    def __init__(
        self,
        code: int | None,
        message: str,
        *,
        status: int | None = None,
        op: str = "",
    ) -> None:
        self.status = status
        super().__init__(code, message, op=op)


class BybitAuthError(ExchangeError):
    """The credential is missing or unusable before anything was sent.

    Distinct from a :class:`BybitWsError` carrying ``10004``: this one never
    reached the venue, so it is a configuration mistake and should read as one.
    """


# --- signing ---------------------------------------------------------------

#: How long a socket auth stays valid, in milliseconds. Bybit compares
#: ``expires`` against its own clock, so this doubles as the tolerance for a
#: local clock that runs slow.
DEFAULT_AUTH_WINDOW_MS = 5_000

#: How far a REST request may lag Bybit's clock before it is refused, in
#: milliseconds. Bybit's own default; sent explicitly because it is part of the
#: signed string and both sides must agree on the value.
DEFAULT_RECV_WINDOW_MS = 5_000


def now_ms() -> int:
    """Wall clock in milliseconds — the unit every Bybit timestamp uses."""
    return int(time.time() * 1000)


def sign_ws(api_secret: str, expires: int) -> str:
    """HMAC-SHA256 hex over ``GET/realtime<expires>``.

    The literal ``GET/realtime`` is Bybit's, not a path we could vary: the
    server rebuilds this exact string from the ``expires`` it received, so the
    only input is the deadline.
    """
    return hmac.new(
        api_secret.encode("utf-8"),
        f"GET/realtime{expires}".encode(),
        hashlib.sha256,
    ).hexdigest()


def sign_rest(
    api_secret: str,
    *,
    api_key: str,
    timestamp: int,
    recv_window: int,
    payload: str = "",
) -> str:
    """HMAC-SHA256 hex over ``<timestamp><api_key><recv_window><payload>``.

    ``payload`` is the query string for a GET and the **raw JSON body** for a
    POST — the exact bytes that go on the wire, because Bybit re-reads what it
    received to verify. :meth:`~mftik.exchange.bybit.rest.BybitRest._post`
    guarantees that by signing and sending one ``json.dumps`` output.
    """
    message = f"{timestamp}{api_key}{recv_window}{payload}"
    return hmac.new(
        api_secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def rest_headers(
    *,
    api_key: str,
    api_secret: str,
    payload: str = "",
    recv_window: int = DEFAULT_RECV_WINDOW_MS,
    timestamp: int | None = None,
) -> dict[str, str]:
    """The four ``X-BAPI-*`` headers a signed REST call carries."""
    ts = now_ms() if timestamp is None else timestamp
    return {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": str(ts),
        "X-BAPI-RECV-WINDOW": str(recv_window),
        "X-BAPI-SIGN": sign_rest(
            api_secret,
            api_key=api_key,
            timestamp=ts,
            recv_window=recv_window,
            payload=payload,
        ),
    }


# --- wire values -----------------------------------------------------------


def decimal_text(value: Decimal) -> str:
    """A number as Bybit wants it written.

    Two things go wrong if a ``Decimal`` is handed over as it comes:

    ``1E-8`` — plenty of real tick sizes are small enough for ``str`` to reach
    for exponent notation, which Bybit does not parse as a price. ``f``
    formatting keeps it positional.

    ``0.00780000`` — trailing zeros are the ``Decimal``'s *scale*, not
    formatting, and arithmetic propagates them: a size floored against a
    ``qtyStep`` of ``0.00010000`` carries eight decimals whatever the number
    actually is. Bybit checks written precision against the instrument's step
    and refuses ``0.00780000`` where it takes ``0.0078``, though they are the
    same quantity.
    """
    stripped = value.normalize()
    # ``normalize`` renders a whole number exponentially (10 → 1E+1), which is
    # the other spelling Bybit will not read.
    if stripped.as_tuple().exponent > 0:
        stripped = stripped.quantize(Decimal(1))
    return f"{stripped:f}"


def wire(value: Any) -> Any:
    """One argument value, as it goes into the JSON frame.

    Only ``Decimal`` is rewritten, and how it is written matters — see
    :func:`decimal_text`. Everything else is left as the JSON type it already
    is, which is a deliberate line rather than an omission: Bybit's *money*
    fields are strings (``qty``, ``price``, ``triggerPrice``), but its flags
    and enums are not. ``positionIdx`` and ``isLeverage`` are integers and
    ``reduceOnly`` is a boolean, and a blanket "stringify every number" would
    turn a hedge-mode or margin order into a ``10001``.

    ``None`` values are dropped from a mapping: an optional argument left unset
    should not arrive as a null Bybit has to interpret.
    """
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, (list, tuple)):
        return [wire(item) for item in value]
    if isinstance(value, dict):
        return {k: wire(v) for k, v in value.items() if v is not None}
    return value


def query_string(params: dict[str, Any] | None) -> str:
    """A GET's query, in the order it will be sent — and therefore signed.

    Insertion order, not sorted: the signature covers the literal query string,
    so what matters is that this function builds the string that is actually
    put on the wire. ``None`` values are dropped, because an omitted optional
    parameter should not arrive as the four characters ``None``.
    """
    return "&".join(
        f"{key}={_query_value(value)}"
        for key, value in (params or {}).items()
        if value is not None
    )


def _query_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return decimal_text(value)
    return str(value)


def json_body(args: dict[str, Any]) -> str:
    """A POST body, serialized once so the signature covers what is sent."""
    return json.dumps(wire(args), separators=(",", ":"))


# --- frames ----------------------------------------------------------------

AUTH = "auth"
SUBSCRIBE = "subscribe"
UNSUBSCRIBE = "unsubscribe"
PING = "ping"
PONG = "pong"


def auth_frame(
    *,
    api_key: str,
    api_secret: str,
    window_ms: int = DEFAULT_AUTH_WINDOW_MS,
    req_id: str | None = None,
    expires: int | None = None,
) -> tuple[dict[str, Any], str]:
    """An ``op: auth`` frame. Returns ``(frame, req_id)``.

    ``expires`` is a deadline, not a timestamp: Bybit refuses the auth once its
    own clock passes it, so the window is really a tolerance for clock skew
    between us and the venue. This is the only signature the socket path
    computes — everything after it rides the authenticated connection.
    """
    deadline = now_ms() + window_ms if expires is None else expires
    req_id = req_id or uuid.uuid4().hex
    return (
        {
            "req_id": req_id,
            "op": AUTH,
            "args": [api_key, deadline, sign_ws(api_secret, deadline)],
        },
        req_id,
    )


def subscribe_frame(
    topics: list[str],
    *,
    op: str = SUBSCRIBE,
    req_id: str | None = None,
) -> tuple[dict[str, Any], str]:
    """A ``subscribe`` / ``unsubscribe`` frame. Returns ``(frame, req_id)``."""
    req_id = req_id or uuid.uuid4().hex
    return {"req_id": req_id, "op": op, "args": list(topics)}, req_id


def ping_frame(req_id: str | None = None) -> tuple[dict[str, Any], str]:
    """The application-level heartbeat every Bybit socket expects.

    Not optional and not a substitute for a protocol ping: Bybit closes a
    connection that has been quiet for too long regardless of WebSocket-level
    keepalive, so this is what keeps a socket carrying an idle account alive.
    """
    req_id = req_id or uuid.uuid4().hex
    return {"req_id": req_id, "op": PING}, req_id


def trade_frame(
    op: str,
    args: list[dict[str, Any]],
    *,
    recv_window: int = DEFAULT_RECV_WINDOW_MS,
    req_id: str | None = None,
    referer: str | None = None,
    timestamp: int | None = None,
) -> tuple[dict[str, Any], str]:
    """An order-entry frame for the trade socket. Returns ``(frame, req_id)``.

    The trade socket carries the same ``X-BAPI-*`` values a REST call would put
    in headers — minus the credential, which the connection's ``auth`` already
    established — in a ``header`` block on each frame. The timestamp is not
    part of any signature here; it is the venue's staleness check, and a frame
    without one is refused however the connection was authenticated.

    ``args`` is a list because Bybit's batch ops take several orders on one
    frame; the single-order ops take a list of exactly one.
    """
    req_id = req_id or uuid.uuid4().hex
    header: dict[str, str] = {
        "X-BAPI-TIMESTAMP": str(now_ms() if timestamp is None else timestamp),
        "X-BAPI-RECV-WINDOW": str(recv_window),
    }
    if referer:
        header["Referer"] = referer
    return (
        {
            "reqId": req_id,
            "header": header,
            "op": op,
            "args": [wire(arg) for arg in args],
        },
        req_id,
    )


# --- responses -------------------------------------------------------------


class BybitResponse:
    """One decoded frame, whichever socket and whichever shape it arrived in.

    Four shapes are normalized onto the same attributes so one read loop can
    route all of them:

    * an op reply — ``{"op", "success", "ret_msg", "req_id", "conn_id"}``, from
      auth, subscribe and ping;
    * a trade reply — ``{"op", "reqId", "retCode", "retMsg", "data"}``, which
      says the same things in Bybit's REST vocabulary;
    * a push — ``{"topic", "type", "ts", "data"}``, from a public or private
      subscription;
    * a pong, which is an op reply whose only content is that it arrived.

    **A frame is a push if and only if it carries a ``topic``.** Everything
    else answers something we sent, and is correlated by ``req_id`` alone — so
    there is no arrival-order heuristic here and no ambiguity when several
    orders are in flight on one socket, which is the normal state of the order
    path.
    """

    __slots__ = (
        "raw",
        "req_id",
        "op",
        "topic",
        "type",
        "success",
        "ret_code",
        "ret_msg",
        "data",
        "ts",
        "conn_id",
    )

    def __init__(self, message: dict[str, Any]) -> None:
        self.raw = message
        # ``req_id`` on the stream sockets, ``reqId`` on the trade socket. Same
        # correlation, two spellings, and the empty string Bybit echoes when we
        # sent none is not an id.
        raw_id = message.get("req_id") or message.get("reqId")
        self.req_id: str | None = str(raw_id) if raw_id else None
        self.op: str = str(message.get("op") or "")
        self.topic: str = str(message.get("topic") or "")
        #: ``snapshot`` or ``delta`` on a push; empty on a reply.
        self.type: str = str(message.get("type") or "")
        self.ret_msg: str = str(message.get("ret_msg") or message.get("retMsg") or "")
        self.conn_id: str = str(message.get("conn_id") or message.get("connId") or "")
        self.ts: int = int(message.get("ts") or 0)
        self.data: Any = message.get("data")

        ret_code = message.get("retCode")
        self.ret_code: int | None = None if ret_code is None else int(ret_code)
        success = message.get("success")
        if success is None:
            # A trade reply says it with ``retCode``; a push says nothing and
            # is not a verdict on anything we sent.
            success = True if self.ret_code is None else self.ret_code == RET_OK
        self.success: bool = bool(success)

    @property
    def is_push(self) -> bool:
        """Whether this frame is an unsolicited market or account update."""
        return bool(self.topic)

    @property
    def is_reply(self) -> bool:
        """Whether this frame answers something we sent."""
        return not self.topic

    @property
    def is_pong(self) -> bool:
        """Whether this is the heartbeat coming back.

        Bybit spells it two ways — the public sockets echo ``op: ping`` while
        the private ones answer ``op: pong`` — and only one of them carries the
        ``req_id`` we sent. Both are recognised here so the read loop can drop
        them without either logging an unmatched reply or waiting on one.
        """
        return self.op in (PING, PONG) or self.ret_msg == PONG

    @property
    def error(self) -> BybitWsError | None:
        """The refusal this frame carried, if it refused anything."""
        if self.success:
            return None
        return BybitWsError(self.ret_code, self.ret_msg or "request failed", op=self.op)

    def raise_for_error(self) -> None:
        error = self.error
        if error is not None:
            raise error

    def rows(self) -> list[dict[str, Any]]:
        """``data`` as a list of dicts, whichever shape the topic used.

        Bybit pushes account topics as a list and the order book as a single
        object, and the trade socket answers with one object. Flattening that
        here keeps every caller from re-deciding it.
        """
        if isinstance(self.data, list):
            return [row for row in self.data if isinstance(row, dict)]
        if isinstance(self.data, dict):
            return [self.data]
        return []

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        what = self.topic or self.op or f"req_id={self.req_id}"
        return (
            f"BybitResponse({what}, success={self.success}, "
            f"ret_code={self.ret_code}, ret_msg={self.ret_msg!r})"
        )


__all__ = [
    "AUTH",
    "AUTH_CODES",
    "BYBIT_REST_TESTNET_URL",
    "BYBIT_REST_URL",
    "BYBIT_WS_PRIVATE_TESTNET_URL",
    "BYBIT_WS_PRIVATE_URL",
    "BYBIT_WS_PUBLIC_TESTNET_URL",
    "BYBIT_WS_PUBLIC_URL",
    "BYBIT_WS_TRADE_TESTNET_URL",
    "BYBIT_WS_TRADE_URL",
    "DEFAULT_AUTH_WINDOW_MS",
    "DEFAULT_RECV_WINDOW_MS",
    "INVERSE",
    "LINEAR",
    "NOT_FOUND_CODES",
    "OPTION",
    "PING",
    "PONG",
    "PRODUCTS",
    "RET_OK",
    "SPOT",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "BybitAuthError",
    "BybitError",
    "BybitResponse",
    "BybitRestError",
    "BybitWsError",
    "auth_frame",
    "decimal_text",
    "json_body",
    "now_ms",
    "ping_frame",
    "product_of",
    "public_url",
    "query_string",
    "rest_headers",
    "sign_rest",
    "sign_ws",
    "subscribe_frame",
    "trade_frame",
    "wire",
]
