"""OKX v5 framing, HMAC signing, and the one response envelope.

OKX is a **unified** venue: one credential (key, secret, passphrase) and one
API version cover spot and USDT-margined swaps, and the market an instrument
trades on is an ``instType`` parameter rather than a different host with
different keys. That is why there is no ``okx/spot`` package here as there is
for Gate and Binance — see :mod:`mftik.exchange.tickers` for why the platform
names that axis on the instrument instead.

What *is* split is the sockets, and along an axis that has nothing to do with
the account:

* :data:`OKX_WS_PRIVATE_URL` — account pushes. Orders, fills, balances and
  positions, for every ``instType`` at once.
* :data:`OKX_WS_PUBLIC_URL` — market pushes. Books, trades, tickers,
  liquidations. One connection carries every instrument; the ``instId`` is
  on the subscribe arg.
* :data:`OKX_WS_BUSINESS_URL` — candles. OKX moved them off the public
  socket; a client that never asks for klines never opens this one.
* REST — order entry and the reads no socket serves. Unlike Bybit there is
  no separate trade socket; placing an order is a signed HTTP call.

REST and the private socket both sign HMAC-SHA256 then Base64, over two
different strings. A REST call signs ``<timestamp><METHOD><path[?query]><body>``
with an ISO-8601 timestamp; a socket authenticates once with
``<unix-seconds>GET/users/self/verify``. The passphrase is a header on every
signed REST call and an argument of the login frame — it is not optional.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from mftik.exchange.errors import ExchangeError
from mftik.exchange.tickers import Category

# --- endpoints -------------------------------------------------------------

OKX_REST_URL = "https://www.okx.com"
OKX_WS_PUBLIC_URL = "wss://ws.okx.com:8443/ws/v5/public"
OKX_WS_PRIVATE_URL = "wss://ws.okx.com:8443/ws/v5/private"
OKX_WS_BUSINESS_URL = "wss://ws.okx.com:8443/ws/v5/business"

#: Demo trading. Same REST host, a header, and a different socket.
OKX_WS_PUBLIC_DEMO_URL = "wss://wspap.okx.com:8443/ws/v5/public"
OKX_WS_PRIVATE_DEMO_URL = "wss://wspap.okx.com:8443/ws/v5/private"
OKX_WS_BUSINESS_DEMO_URL = "wss://wspap.okx.com:8443/ws/v5/business"
DEMO_HEADER = "x-simulated-trading"

# --- product types ---------------------------------------------------------

SPOT = "SPOT"
SWAP = "SWAP"
FUTURES = "FUTURES"
OPTION = "OPTION"
MARGIN = "MARGIN"

PRODUCTS = (SPOT, MARGIN, SWAP, FUTURES, OPTION)

#: Our :class:`~mftik.exchange.tickers.Category` → OKX's ``instType``.
#:
#: ``PERP`` maps to ``SWAP`` and not to ``FUTURES``. SWAP is the perpetual;
#: FUTURES is a dated expiry and a different instrument, the same distinction
#: Bybit makes between ``LinearPerpetual`` and a quarterly. Inverse coin-m
#: swaps are still ``SWAP`` but a different ``instId`` (``BTC-USD-SWAP``);
#: they stay reachable by passing the product string directly.
_PRODUCT_BY_CATEGORY: dict[Category, str] = {
    Category.SPOT: SPOT,
    Category.PERP: SWAP,
    Category.FUTURE: FUTURES,
    Category.OPTION: OPTION,
}


def product_of(category: Category | str) -> str:
    """OKX's ``instType`` for one of ours. Raises on one it does not trade."""
    if isinstance(category, str) and category in PRODUCTS:
        return category
    resolved = category if isinstance(category, Category) else Category(category)
    found = _PRODUCT_BY_CATEGORY.get(resolved)
    if found is None:
        raise ExchangeError(f"OKX has no product for category {category!r}")
    return found


def public_url(*, demo: bool = False) -> str:
    return OKX_WS_PUBLIC_DEMO_URL if demo else OKX_WS_PUBLIC_URL


def private_url(*, demo: bool = False) -> str:
    return OKX_WS_PRIVATE_DEMO_URL if demo else OKX_WS_PRIVATE_URL


def business_url(*, demo: bool = False) -> str:
    return OKX_WS_BUSINESS_DEMO_URL if demo else OKX_WS_BUSINESS_URL


# --- errors ----------------------------------------------------------------

RET_OK = 0

#: "There is no such order" — as opposed to a call that failed.
NOT_FOUND_CODES = frozenset({51400, 51401, 51402, 51603})

#: The credential itself is the problem.
AUTH_CODES = frozenset(
    {50101, 50102, 50103, 50104, 50105, 50111, 50112, 50113, 50114, 50115, 50119}
)


class OkxError(ExchangeError):
    """OKX refused a call, on either transport.

    ``code`` is kept unformatted as well as folded into the string form: TD and
    MD normalize on it and should not have to parse it back out of
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


class OkxWsError(OkxError):
    """A WebSocket op came back unsuccessful."""


class OkxRestError(OkxError):
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


class OkxAuthError(ExchangeError):
    """The credential is missing or unusable before anything was sent."""


# --- signing ---------------------------------------------------------------

LOGIN_PATH = "/users/self/verify"


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_timestamp(when: float | None = None) -> str:
    """ISO-8601 UTC with milliseconds — what a REST signature covers.

    ``2020-12-08T09:08:57.715Z``. The socket login uses unix seconds instead;
    mixing the two is a ``50113``.
    """
    dt = datetime.fromtimestamp(when if when is not None else time.time(), tz=UTC)
    millis = dt.microsecond // 1000
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{millis:03d}Z"


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

    ``request_path`` is the literal path-plus-query that goes on the wire,
    because OKX rebuilds this exact string from what it received.
    """
    return _hmac_b64(
        api_secret, f"{timestamp}{method.upper()}{request_path}{body}"
    )


def sign_ws(api_secret: str, timestamp: str) -> str:
    """Base64 HMAC-SHA256 over ``<unix-seconds>GET/users/self/verify``."""
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
    """The four ``OK-ACCESS-*`` headers a signed REST call carries."""
    ts = iso_timestamp() if timestamp is None else timestamp
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": sign_rest(
            api_secret,
            timestamp=ts,
            method=method,
            request_path=request_path,
            body=body,
        ),
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }
    if demo:
        headers[DEMO_HEADER] = "1"
    return headers


# --- wire values -----------------------------------------------------------


def decimal_text(value: Decimal) -> str:
    """A number as OKX wants it written.

    Scientific notation and trailing zeros from a Decimal's scale are both
    refused against the instrument's tick, the same trap as Bybit.
    """
    stripped = value.normalize()
    if stripped.as_tuple().exponent > 0:
        stripped = stripped.quantize(Decimal(1))
    return f"{stripped:f}"


def wire(value: Any) -> Any:
    """One argument value, as it goes into the JSON body.

    Only ``Decimal`` is rewritten. Flags stay booleans and enums stay the
    strings OKX published; a blanket stringify would turn ``reduceOnly`` into
    a parameter error. ``None`` is dropped from a mapping.
    """
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, (list, tuple)):
        return [wire(item) for item in value]
    if isinstance(value, dict):
        return {k: wire(v) for k, v in value.items() if v is not None}
    return value


def query_string(params: dict[str, Any] | None) -> str:
    """A GET's query, in the order it will be sent — and therefore signed."""
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
    """An ``op: login`` frame. Returns ``(frame, req_id)``."""
    ts = str(int(time.time())) if timestamp is None else timestamp
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
    """A ``subscribe`` / ``unsubscribe`` frame. Returns ``(frame, req_id)``."""
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


class OkxResponse:
    """One decoded frame, whichever socket and whichever shape it arrived in.

    Three shapes are normalized onto the same attributes so one read loop can
    route all of them:

    * an op reply — ``{"id", "event", "code", "msg", "arg"}``, from login,
      subscribe and unsubscribe;
    * a push — ``{"arg", "data", "action?"}``, from a public or private
      subscription;
    * a pong, which is the literal string ``pong`` and is not JSON at all.

    **A frame is a push if and only if it carries ``arg`` and ``data`` and no
    ``event``.** Everything else answers something we sent, and is correlated
    by ``id`` — or, for a login that forgot to echo it, by ``event``.
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
        self.code = _as_int(message.get("code"))
        if self.event == ERROR:
            self.success = False
        elif self.code is None:
            self.success = True
        else:
            self.success = self.code == RET_OK

    @property
    def channel(self) -> str:
        return str(self.arg.get("channel") or "")

    @property
    def inst_id(self) -> str:
        return str(self.arg.get("instId") or "")

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
    def error(self) -> OkxWsError | None:
        if self.success:
            return None
        return OkxWsError(self.code, self.msg or "request failed", op=self.op)

    def raise_for_error(self) -> None:
        error = self.error
        if error is not None:
            raise error

    def rows(self) -> list[dict[str, Any]]:
        """``data`` as a list of dicts, whichever shape the channel used."""
        if isinstance(self.data, list):
            return [row for row in self.data if isinstance(row, dict)]
        if isinstance(self.data, dict):
            return [self.data]
        return []

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        what = self.channel or self.event or self.op or f"id={self.req_id}"
        return (
            f"OkxResponse({what}, success={self.success}, "
            f"code={self.code}, msg={self.msg!r})"
        )


__all__ = [
    "AUTH_CODES",
    "DEMO_HEADER",
    "ERROR",
    "FUTURES",
    "LOGIN",
    "LOGIN_PATH",
    "MARGIN",
    "NOT_FOUND_CODES",
    "OKX_REST_URL",
    "OKX_WS_BUSINESS_DEMO_URL",
    "OKX_WS_BUSINESS_URL",
    "OKX_WS_PRIVATE_DEMO_URL",
    "OKX_WS_PRIVATE_URL",
    "OKX_WS_PUBLIC_DEMO_URL",
    "OKX_WS_PUBLIC_URL",
    "OPTION",
    "PING",
    "PONG",
    "PRODUCTS",
    "RET_OK",
    "SPOT",
    "SUBSCRIBE",
    "SWAP",
    "UNSUBSCRIBE",
    "OkxAuthError",
    "OkxError",
    "OkxResponse",
    "OkxRestError",
    "OkxWsError",
    "business_url",
    "decimal_text",
    "iso_timestamp",
    "json_body",
    "login_frame",
    "now_ms",
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
