"""Bitget UTA v3 framing, HMAC signing, and the one response envelope.

Bitget is a **unified** venue: one credential (key, secret, passphrase) and
one API version cover spot and both linear perpetual books. The market an
instrument trades on is a ``category`` parameter rather than a different
host with different keys. Classic v2 (``/api/v2/...``, mix hosts) is not
modelled.

What *is* split is the wire product on the two linear books. Identity is
one :class:`~mftik.exchange.tickers.Category.PERP`; routing is not.
:func:`product_of` therefore takes a ticker (or a quote / settlement),
never a category alone — ``USDT-FUTURES`` and ``USDC-FUTURES`` are
different sockets and different REST ``category`` values.

REST signs ``<timestamp><METHOD><path[?sortedQuery]><body>`` with
milliseconds and HMAC-SHA256 then Base64. A private socket authenticates
once with ``<unix-seconds>GET/user/verify``. Mixing the two units is a
login fail. The ping is the literal string ``ping``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import uuid
from decimal import Decimal
from typing import Any

from mftik.exchange.errors import ExchangeError
from mftik.exchange.tickers import Category, UniversalTicker

# --- endpoints -------------------------------------------------------------

BITGET_REST_URL = "https://api.bitget.com"
BITGET_WS_PUBLIC_URL = "wss://ws.bitget.com/v3/ws/public"
BITGET_WS_PRIVATE_URL = "wss://ws.bitget.com/v3/ws/private"

#: Demo trading. Different host, a header, and a different key. Not shipped.
BITGET_WS_PUBLIC_DEMO_URL = "wss://wspap.bitget.com/v3/ws/public"
BITGET_WS_PRIVATE_DEMO_URL = "wss://wspap.bitget.com/v3/ws/private"
DEMO_HEADER = "paptrading"

# --- product types ---------------------------------------------------------

SPOT = "SPOT"
USDT_FUTURES = "USDT-FUTURES"
USDC_FUTURES = "USDC-FUTURES"
COIN_FUTURES = "COIN-FUTURES"
MARGIN = "MARGIN"

PRODUCTS = (SPOT, USDT_FUTURES, USDC_FUTURES)

#: Public-socket ``instType``. V4: USDC is a third socket, not a rider on
#: ``usdt-futures``.
INST_SPOT = "spot"
INST_USDT = "usdt-futures"
INST_USDC = "usdc-futures"

_INST_BY_PRODUCT: dict[str, str] = {
    SPOT: INST_SPOT,
    USDT_FUTURES: INST_USDT,
    USDC_FUTURES: INST_USDC,
}

_PRODUCT_BY_INST: dict[str, str] = {v: k for k, v in _INST_BY_PRODUCT.items()}

_USDT = "USDT"
_USDC = "USDC"


def _as_ticker(value: UniversalTicker | Category | str) -> UniversalTicker | None:
    if isinstance(value, UniversalTicker):
        return value
    return None


def _settle_of(
    ticker: UniversalTicker | None,
    *,
    quote: str | None,
    settlement: str | None,
) -> str:
    for candidate in (settlement, quote):
        if candidate:
            return candidate.upper()
    if ticker is None:
        return ""
    symbol = ticker.symbol.upper()
    if symbol.endswith(_USDT):
        return _USDT
    if symbol.endswith(_USDC):
        return _USDC
    return ""


def product_of(
    value: UniversalTicker | Category | str,
    *,
    quote: str | None = None,
    settlement: str | None = None,
) -> str:
    """Bitget's REST ``category`` for one of ours.

    Outbound takes a ticker (or category + quote / settlement), not
    :class:`~mftik.exchange.tickers.Category` alone. A ``Perp`` whose settle
    coin is ``USDC`` is ``USDC-FUTURES``; ``USDT`` (and only USDT) is
    ``USDT-FUTURES``. A perp whose quote is neither raises.
    """
    if isinstance(value, str) and value in (*PRODUCTS, COIN_FUTURES, MARGIN):
        if value not in PRODUCTS:
            raise ExchangeError(f"Bitget does not trade product {value!r}")
        return value
    ticker = _as_ticker(value)
    if ticker is not None:
        category = ticker.category
    elif isinstance(value, Category):
        category = value
    else:
        try:
            category = Category(value) if not isinstance(value, Category) else value
        except ValueError:
            # Lowercase public-socket instType, or a raw category word.
            folded = (value or "").strip()
            found = _PRODUCT_BY_INST.get(folded.casefold())
            if found is not None:
                return found
            raise ExchangeError(f"Bitget has no product for {value!r}") from None
    if category is Category.SPOT:
        return SPOT
    if category is Category.PERP:
        settle = _settle_of(ticker, quote=quote, settlement=settlement)
        if settle == _USDC:
            return USDC_FUTURES
        if settle == _USDT:
            return USDT_FUTURES
        raise ExchangeError(
            f"Bitget Perp {ticker or category} has no linear settle "
            f"(need USDT or USDC, got {settle or 'nothing'})"
        )
    raise ExchangeError(f"Bitget has no product for category {category!r}")


def category_of(value: str | None, default: Category = Category.SPOT) -> Category:
    """Inbound wire product → our category. Both linear books are ``Perp``."""
    raw = (value or "").strip()
    if not raw:
        return default
    folded = raw.casefold()
    if raw == SPOT or folded == INST_SPOT:
        return Category.SPOT
    if raw in {USDT_FUTURES, USDC_FUTURES} or folded in {INST_USDT, INST_USDC}:
        return Category.PERP
    return default


def inst_type_of(product: str) -> str:
    """REST ``category`` → public-socket ``instType`` (V4)."""
    found = _INST_BY_PRODUCT.get(product)
    if found is None:
        raise ExchangeError(
            f"Bitget has no public instType for {product!r}; "
            f"known: {', '.join(PRODUCTS)}"
        )
    return found


def public_url(*, demo: bool = False) -> str:
    return BITGET_WS_PUBLIC_DEMO_URL if demo else BITGET_WS_PUBLIC_URL


def private_url(*, demo: bool = False) -> str:
    return BITGET_WS_PRIVATE_DEMO_URL if demo else BITGET_WS_PRIVATE_URL


# --- errors ----------------------------------------------------------------

RET_OK = "00000"
WS_RET_OK = "0"

#: "There is no such order" — as opposed to a call that failed.
NOT_FOUND_CODES = frozenset({43001, 43012, 22001})

#: The credential itself is the problem, including a Classic key on v3.
AUTH_CODES = frozenset({40006, 40009, 40014, 40018, 40085})


class BitgetError(ExchangeError):
    """Bitget refused a call, on either transport.

    ``code`` is kept unformatted as well as folded into the string form: TD
    and MD normalize on it and should not have to parse it back out of
    ``str(exc)``.
    """

    def __init__(self, code: int | None, message: str, *, op: str = "") -> None:
        self.code = code
        self.op = op
        prefix = f"{op}: " if op else ""
        super().__init__(f"{prefix}[{code}] {message}")

    @property
    def not_found(self) -> bool:
        return self.code in NOT_FOUND_CODES


class BitgetWsError(BitgetError):
    """A WebSocket op came back unsuccessful."""


class BitgetRestError(BitgetError):
    """A REST call failed, in the body or in the status line."""

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


class BitgetAuthError(ExchangeError):
    """The credential is missing or unusable before anything was sent."""


# --- signing ---------------------------------------------------------------

LOGIN_PATH = "/user/verify"


def now_ms() -> int:
    return int(time.time() * 1000)


def now_s() -> int:
    """Unix seconds — the unit a private-socket login signs (V1)."""
    return int(time.time())


def _hmac_b64(secret: str, message: str) -> str:
    digest = hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).digest()
    return base64.b64encode(digest).decode("ascii")


def sign_rest(
    api_secret: str,
    *,
    timestamp: str,
    method: str,
    request_path: str,
    body: str = "",
) -> str:
    """Base64 HMAC-SHA256 over ``<timestamp><METHOD><path[?query]><body>``.

    ``request_path`` is the literal path-plus-query that goes on the wire.
    Query keys must already be sorted — see :func:`query_string`.
    """
    return _hmac_b64(
        api_secret, f"{timestamp}{method.upper()}{request_path}{body}"
    )


def sign_ws(api_secret: str, timestamp: str) -> str:
    """Base64 HMAC-SHA256 over ``<unix-seconds>GET/user/verify``.

    **V1:** the published login example and the Java HMAC sample use
    unix-seconds. REST is milliseconds. We send seconds and never
    milliseconds.
    """
    return _hmac_b64(api_secret, f"{timestamp}GET{LOGIN_PATH}")


def rest_headers(
    *,
    api_key: str,
    api_secret: str,
    passphrase: str,
    method: str,
    request_path: str,
    body: str = "",
    timestamp: str | None = None,
    demo: bool = False,
) -> dict[str, str]:
    """The four ``ACCESS-*`` headers a signed REST call carries."""
    ts = str(now_ms()) if timestamp is None else timestamp
    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": sign_rest(
            api_secret,
            timestamp=ts,
            method=method,
            request_path=request_path,
            body=body,
        ),
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "locale": "en-US",
    }
    if demo:
        headers[DEMO_HEADER] = "1"
    return headers


# --- wire values -----------------------------------------------------------


def decimal_text(value: Decimal) -> str:
    stripped = value.normalize()
    if stripped.as_tuple().exponent > 0:
        stripped = stripped.quantize(Decimal(1))
    return f"{stripped:f}"


def wire(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, (list, tuple)):
        return [wire(item) for item in value]
    if isinstance(value, dict):
        return {k: wire(v) for k, v in value.items() if v is not None}
    return value


def query_string(params: dict[str, Any] | None) -> str:
    """A GET's query, keys **sorted alphabetically** (V10).

    The signature covers this exact string, so httpx must not reserialise a
    different one. Callers put this string on the path they send.
    """
    items = [
        (key, _query_value(value))
        for key, value in (params or {}).items()
        if value is not None
    ]
    items.sort(key=lambda pair: pair[0])
    return "&".join(f"{key}={value}" for key, value in items)


def _query_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return decimal_text(value)
    return str(value)


def json_body(args: dict[str, Any]) -> str:
    return json.dumps(wire(args), separators=(",", ":"))


# --- frames ----------------------------------------------------------------

LOGIN = "login"
SUBSCRIBE = "subscribe"
UNSUBSCRIBE = "unsubscribe"
PING = "ping"
PONG = "pong"
ERROR = "error"


def login_frame(
    *,
    api_key: str,
    api_secret: str,
    passphrase: str,
    timestamp: str | None = None,
    req_id: str | None = None,
) -> tuple[dict[str, Any], str]:
    """An ``op: login`` frame. Timestamp is unix-seconds (V1)."""
    ts = str(now_s()) if timestamp is None else timestamp
    req_id = req_id or uuid.uuid4().hex
    return (
        {
            "id": req_id,
            "op": LOGIN,
            "args": [
                {
                    "apiKey": api_key,
                    "passphrase": passphrase,
                    "timestamp": ts,
                    "sign": sign_ws(api_secret, ts),
                }
            ],
        },
        req_id,
    )


def subscribe_frame(
    args: list[dict[str, Any]],
    *,
    op: str = SUBSCRIBE,
    req_id: str | None = None,
) -> tuple[dict[str, Any], str]:
    req_id = req_id or uuid.uuid4().hex
    return {"id": req_id, "op": op, "args": list(args)}, req_id


# --- responses -------------------------------------------------------------


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class BitgetResponse:
    """One decoded frame, whichever socket and whichever shape it arrived in.

    Three shapes are normalized onto the same attributes:

    * an op reply — ``{"id", "event", "code", "msg", "arg"}``;
    * a push — ``{"arg", "data", "action?"}``;
    * a pong, which is the literal string ``pong``.

    **A frame is a push if and only if it carries ``arg`` and ``data`` and
    no ``event``.**
    """

    __slots__ = (
        "raw",
        "req_id",
        "event",
        "op",
        "arg",
        "action",
        "success",
        "code",
        "msg",
        "data",
        "ts",
        "conn_id",
    )

    def __init__(self, message: dict[str, Any]) -> None:
        self.raw = message
        raw_id = message.get("id")
        self.req_id: str | None = str(raw_id) if raw_id else None
        self.event: str = str(message.get("event") or "")
        self.op: str = str(message.get("op") or self.event)
        arg = message.get("arg")
        self.arg: dict[str, Any] = arg if isinstance(arg, dict) else {}
        self.action: str = str(message.get("action") or "")
        self.msg: str = str(message.get("msg") or "")
        self.conn_id: str = str(message.get("connId") or "")
        self.data: Any = message.get("data")
        self.ts: int = int(message.get("ts") or 0)
        raw_code = message.get("code")
        self.code = _as_int(raw_code)
        if self.event == ERROR:
            self.success = False
        elif raw_code in (None, ""):
            self.success = True
        else:
            self.success = str(raw_code) in {RET_OK, WS_RET_OK, "0"}

    @property
    def topic(self) -> str:
        return str(self.arg.get("topic") or self.arg.get("channel") or "")

    @property
    def inst_type(self) -> str:
        return str(self.arg.get("instType") or "")

    @property
    def symbol(self) -> str:
        return str(self.arg.get("symbol") or self.arg.get("instId") or "")

    @property
    def is_push(self) -> bool:
        return bool(self.arg) and "data" in self.raw and not self.event

    @property
    def is_reply(self) -> bool:
        return bool(self.event) and not self.is_push

    @property
    def is_pong(self) -> bool:
        return self.event == PONG or self.op == PONG

    @property
    def error(self) -> BitgetWsError | None:
        if self.success:
            return None
        return BitgetWsError(self.code, self.msg or "request failed", op=self.op)

    def raise_for_error(self) -> None:
        error = self.error
        if error is not None:
            raise error

    def rows(self) -> list[dict[str, Any]]:
        if isinstance(self.data, list):
            return [row for row in self.data if isinstance(row, dict)]
        if isinstance(self.data, dict):
            return [self.data]
        return []

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        what = self.topic or self.event or self.op or f"id={self.req_id}"
        return (
            f"BitgetResponse({what}, success={self.success}, "
            f"code={self.code}, msg={self.msg!r})"
        )


__all__ = [
    "AUTH_CODES",
    "BITGET_REST_URL",
    "BITGET_WS_PRIVATE_DEMO_URL",
    "BITGET_WS_PRIVATE_URL",
    "BITGET_WS_PUBLIC_DEMO_URL",
    "BITGET_WS_PUBLIC_URL",
    "COIN_FUTURES",
    "DEMO_HEADER",
    "ERROR",
    "INST_SPOT",
    "INST_USDC",
    "INST_USDT",
    "LOGIN",
    "LOGIN_PATH",
    "MARGIN",
    "NOT_FOUND_CODES",
    "PING",
    "PONG",
    "PRODUCTS",
    "RET_OK",
    "SPOT",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "USDC_FUTURES",
    "USDT_FUTURES",
    "WS_RET_OK",
    "BitgetAuthError",
    "BitgetError",
    "BitgetResponse",
    "BitgetRestError",
    "BitgetWsError",
    "category_of",
    "decimal_text",
    "inst_type_of",
    "json_body",
    "login_frame",
    "now_ms",
    "now_s",
    "private_url",
    "product_of",
    "public_url",
    "query_string",
    "rest_headers",
    "sign_rest",
    "sign_ws",
    "subscribe_frame",
    "wire",
]
