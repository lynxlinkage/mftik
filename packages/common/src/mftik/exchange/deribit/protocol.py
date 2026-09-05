"""Deribit JSON-RPC 2.0 framing, HMAC signing, and the one response envelope.

Deribit is a **unified** venue: one HMAC credential (Client ID / Client
Secret, no passphrase) covers spot and the linear perpetual books. The
market an instrument trades on is its ``kind`` / ``instrument_type``,
not a different host. Inverse, dated futures and options are not
modelled.

HTTP and WebSocket speak the same methods. A private socket authenticates
once with ``public/auth`` ``grant_type=client_signature`` (V1) and then
keeps a session. Mixing the WS string-to-sign with the HTTP
``deri-hmac-sha256`` formula is a login fail.

The heartbeat is ``public/set_heartbeat`` plus a ``public/test`` reply to
each ``test_request``.
"""

from __future__ import annotations

import hashlib
import hmac
import itertools
import time
from collections.abc import Iterator
from typing import Any

from mftik.exchange.errors import ExchangeError
from mftik.exchange.tickers import Category, UniversalTicker

# --- endpoints -------------------------------------------------------------

DERIBIT_REST_URL = "https://www.deribit.com/api/v2"
DERIBIT_WS_URL = "wss://www.deribit.com/ws/api/v2"

#: Testnet. Different host and a different key. Not shipped.
DERIBIT_TEST_REST_URL = "https://test.deribit.com/api/v2"
DERIBIT_TEST_WS_URL = "wss://test.deribit.com/ws/api/v2"

# --- kinds -----------------------------------------------------------------

KIND_SPOT = "spot"
KIND_FUTURE = "future"
KIND_OPTION = "option"

LINEAR = "linear"
REVERSED = "reversed"
PERPETUAL = "perpetual"

MARGIN_MODELS = frozenset(
    {"segregated_sm", "segregated_pm", "cross_sm", "cross_pm"}
)

# --- errors ----------------------------------------------------------------

NOT_FOUND_CODES = frozenset({10009, 10003, 11044})
AUTH_CODES = frozenset({10000, 10003, 10010, 13668})
RATE_LIMIT_CODES = frozenset({10028})
CBE_UNSUPPORTED = 11060


class DeribitError(ExchangeError):
    """Deribit refused a call, on either transport.

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


class DeribitWsError(DeribitError):
    """A WebSocket RPC came back unsuccessful."""


class DeribitRestError(DeribitError):
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


class DeribitAuthError(ExchangeError):
    """The credential is missing or unusable before anything was sent."""


# --- identity --------------------------------------------------------------


def kind_of(category: Category | UniversalTicker) -> str:
    """Our category → Deribit's ``kind`` on listing and private filters."""
    resolved = category.category if isinstance(category, UniversalTicker) else category
    if resolved is Category.SPOT:
        return KIND_SPOT
    if resolved is Category.PERP:
        return KIND_FUTURE
    raise ExchangeError(f"Deribit has no kind for category {resolved!r}")


def category_of(
    kind: str | None,
    *,
    instrument_type: str = "",
    future_type: str = "",
    settlement_period: str = "",
    default: Category = Category.SPOT,
) -> Category:
    """Inbound wire kind → our category.

    Linear perpetuals fold into ``Perp``. Inverse and dated futures are
    not a category this adapter serves; callers that need to drop them
    use :func:`is_linear_perp`.
    """
    folded = (kind or "").strip().casefold()
    if folded == KIND_SPOT:
        return Category.SPOT
    if folded == KIND_FUTURE and is_linear_perp(
        instrument_type=instrument_type,
        future_type=future_type,
        settlement_period=settlement_period,
    ):
        return Category.PERP
    return default


def is_linear_perp(
    *,
    instrument_type: str = "",
    future_type: str = "",
    settlement_period: str = "",
    kind: str = "",
) -> bool:
    """Whether a ``kind=future`` row is a linear perpetual (V3)."""
    if kind and kind.strip().casefold() not in {"", KIND_FUTURE}:
        return False
    if (settlement_period or "").strip().casefold() != PERPETUAL:
        return False
    itype = (instrument_type or future_type or "").strip().casefold()
    return itype == LINEAR


def is_cbe_routed(row: dict[str, Any]) -> bool:
    """Coinbase-routed spot. The fields are **omitted** when false (V12)."""
    return bool(row.get("is_cbe_routed") or row.get("is_csr"))


def is_linear_perp_name(instrument_name: str) -> bool:
    """Wire name of a linear perpetual: ``BTC_USDC-PERPETUAL``, not inverse."""
    name = instrument_name or ""
    return "_" in name and name.endswith("-PERPETUAL")


def category_from_instrument(instrument_name: str) -> Category:
    """Inbound ``instrument_name`` → category for the books this adapter serves."""
    if is_linear_perp_name(instrument_name):
        return Category.PERP
    return Category.SPOT


# --- signing ---------------------------------------------------------------


def now_ms() -> int:
    return int(time.time() * 1000)


def sign_ws(
    api_secret: str,
    timestamp: int | str,
    nonce: str,
    data: str = "",
) -> str:
    """Hex HMAC-SHA256 over ``timestamp\\nnonce\\ndata`` (V1).

    ``data`` omitted is the empty string; the trailing newline after
    nonce is still required.
    """
    message = f"{timestamp}\n{nonce}\n{data}"
    return hmac.new(
        api_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def sign_rest(
    api_secret: str,
    *,
    timestamp: int | str,
    nonce: str,
    method: str,
    uri: str,
    body: str = "",
) -> str:
    """Hex HMAC-SHA256 over the HTTP REST string-to-sign (V1).

    ``METHOD\\nURI\\nBODY\\n`` is nested inside
    ``timestamp\\nnonce\\n<request>``. The three newlines in the request
    half are mandatory even when the body is empty.
    """
    request_data = f"{method.upper()}\n{uri}\n{body}\n"
    message = f"{timestamp}\n{nonce}\n{request_data}"
    return hmac.new(
        api_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def rest_headers(
    *,
    api_key: str,
    api_secret: str,
    method: str,
    uri: str,
    body: str = "",
    timestamp: int | None = None,
    nonce: str | None = None,
) -> dict[str, str]:
    """``Authorization: deri-hmac-sha256 id=…,ts=…,nonce=…,sig=…``."""
    ts = now_ms() if timestamp is None else timestamp
    used_nonce = nonce if nonce is not None else f"{ts:x}"
    signature = sign_rest(
        api_secret,
        timestamp=ts,
        nonce=used_nonce,
        method=method,
        uri=uri,
        body=body,
    )
    return {
        "Authorization": (
            f"deri-hmac-sha256 id={api_key},ts={ts},"
            f"nonce={used_nonce},sig={signature}"
        ),
        "Content-Type": "application/json",
    }


def auth_params(
    *,
    api_key: str,
    api_secret: str,
    timestamp: int | None = None,
    nonce: str | None = None,
    data: str = "",
    scope: str = "session:mftik",
) -> dict[str, Any]:
    """``public/auth`` ``client_signature`` params (V1)."""
    ts = now_ms() if timestamp is None else timestamp
    used_nonce = nonce if nonce is not None else f"{ts:x}"
    params: dict[str, Any] = {
        "grant_type": "client_signature",
        "client_id": api_key,
        "timestamp": ts,
        "nonce": used_nonce,
        "data": data,
        "signature": sign_ws(api_secret, ts, used_nonce, data),
    }
    if scope:
        params["scope"] = scope
    return params


# --- frames ----------------------------------------------------------------

JSONRPC = "2.0"
SUBSCRIPTION = "subscription"
HEARTBEAT = "heartbeat"
TEST_REQUEST = "test_request"

_ids: Iterator[int] = itertools.count(1)


def next_id() -> int:
    return next(_ids)


def rpc_frame(
    method: str,
    params: dict[str, Any] | None = None,
    *,
    req_id: int | None = None,
) -> tuple[dict[str, Any], str]:
    ident = next_id() if req_id is None else req_id
    frame: dict[str, Any] = {
        "jsonrpc": JSONRPC,
        "id": ident,
        "method": method,
    }
    if params:
        frame["params"] = params
    return frame, str(ident)


# --- responses -------------------------------------------------------------


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class DeribitResponse:
    """One decoded JSON-RPC frame.

    Three shapes land on the same attributes:

    * an RPC reply — ``{"id", "result"}`` or ``{"id", "error"}``;
    * a subscription push — ``{"method": "subscription", "params":
      {"channel", "data"}}`` and no ``id``;
    * a heartbeat — ``{"method": "heartbeat", "params": {"type"}}``.
    """

    __slots__ = (
        "raw",
        "req_id",
        "method",
        "result",
        "error",
        "code",
        "msg",
        "channel",
        "data",
        "params",
    )

    def __init__(self, message: dict[str, Any]) -> None:
        self.raw = message
        raw_id = message.get("id")
        self.req_id: str | None = str(raw_id) if raw_id is not None else None
        self.method: str = str(message.get("method") or "")
        self.result: Any = message.get("result")
        error = message.get("error")
        self.error: dict[str, Any] = error if isinstance(error, dict) else {}
        self.code = _as_int(self.error.get("code"))
        self.msg: str = str(self.error.get("message") or "")
        params = message.get("params")
        self.params: dict[str, Any] = params if isinstance(params, dict) else {}
        self.channel: str = str(self.params.get("channel") or "")
        self.data: Any = self.params.get("data")

    @property
    def success(self) -> bool:
        return not self.error

    @property
    def is_push(self) -> bool:
        return self.method == SUBSCRIPTION and bool(self.channel)

    @property
    def is_heartbeat(self) -> bool:
        return self.method == HEARTBEAT

    @property
    def is_test_request(self) -> bool:
        return self.is_heartbeat and str(self.params.get("type") or "") == TEST_REQUEST

    @property
    def is_reply(self) -> bool:
        return self.req_id is not None and not self.is_push

    def raise_for_error(self) -> None:
        if self.success:
            return
        raise DeribitWsError(
            self.code, self.msg or "request failed", op=self.method
        )

    def rows(self) -> list[dict[str, Any]]:
        if isinstance(self.data, list):
            return [row for row in self.data if isinstance(row, dict)]
        if isinstance(self.data, dict):
            return [self.data]
        return []

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        what = self.channel or self.method or f"id={self.req_id}"
        return (
            f"DeribitResponse({what}, success={self.success}, "
            f"code={self.code}, msg={self.msg!r})"
        )


__all__ = [
    "AUTH_CODES",
    "CBE_UNSUPPORTED",
    "DERIBIT_REST_URL",
    "DERIBIT_TEST_REST_URL",
    "DERIBIT_TEST_WS_URL",
    "DERIBIT_WS_URL",
    "HEARTBEAT",
    "JSONRPC",
    "KIND_FUTURE",
    "KIND_OPTION",
    "KIND_SPOT",
    "LINEAR",
    "MARGIN_MODELS",
    "NOT_FOUND_CODES",
    "PERPETUAL",
    "RATE_LIMIT_CODES",
    "REVERSED",
    "SUBSCRIPTION",
    "TEST_REQUEST",
    "DeribitAuthError",
    "DeribitError",
    "DeribitResponse",
    "DeribitRestError",
    "DeribitWsError",
    "auth_params",
    "category_from_instrument",
    "category_of",
    "is_cbe_routed",
    "is_linear_perp",
    "is_linear_perp_name",
    "kind_of",
    "next_id",
    "now_ms",
    "rest_headers",
    "rpc_frame",
    "sign_rest",
    "sign_ws",
]
