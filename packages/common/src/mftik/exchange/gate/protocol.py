"""Gate WebSocket v4 request framing, signing, and response envelopes.

Shared by the spot and futures planes. The envelope, HMAC-SHA512 and the two
request styles are the same; only the channel names and hosts differ.

**Subscriptions** (``event: subscribe``) use::

    {"time": <unix s>, "channel": ..., "event": ..., "payload": [...]}

signed — for private channels — with HMAC-SHA512 over
``channel=<channel>&event=<event>&time=<time>`` in an ``auth`` block.

**Trading calls** (``event: api``) need a prior login on the socket
(``spot.login`` / ``futures.login``). Login is signed with an empty
``req_param`` string; after that, order entry frames carry only ``req_id`` /
``req_param`` (session auth). One connection still carries public data,
private pushes, and order entry.

REST signing is the same HMAC-SHA512 scheme on both planes:
``METHOD\\nPATH\\nQUERY\\nSHA512(body)\\nTIMESTAMP``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from typing import Any

from mftik.exchange.errors import ExchangeError


class GateWsError(ExchangeError):
    """An ``error`` block came back on a Gate WebSocket frame.

    ``code`` is kept unformatted as well as folded into the string form: TD
    normalizes on it, and should not have to parse it back out of
    ``str(exc)``.
    """

    def __init__(self, code: int | None, message: str, *, channel: str = "") -> None:
        self.code = code
        self.channel = channel
        prefix = f"{channel}: " if channel else ""
        super().__init__(f"{prefix}[{code}] {message}")


class GateApiError(ExchangeError):
    """A trading call came back with ``data.errs``.

    Gate reports these as a ``label`` (machine-readable, e.g.
    ``BALANCE_NOT_ENOUGH``, ``ORDER_NOT_FOUND``) plus a human message. The
    label is kept unformatted for the same reason as :class:`GateWsError`.
    """

    def __init__(self, label: str, message: str, *, channel: str = "") -> None:
        self.label = label
        self.channel = channel
        prefix = f"{channel}: " if channel else ""
        super().__init__(f"{prefix}{label}: {message}")


class GateRestError(ExchangeError):
    """Non-2xx or error-bodied response from Gate's REST API."""

    def __init__(self, status: int, label: str, message: str) -> None:
        self.status = status
        self.label = label
        super().__init__(f"[{status}] {label}: {message}")


def sign(api_secret: str, channel: str, event: str, ts: int) -> str:
    """HMAC-SHA512 hex digest of ``channel=..&event=..&time=..``."""
    message = f"channel={channel}&event={event}&time={ts}"
    return hmac.new(
        api_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha512,
    ).hexdigest()


def request_frame(
    channel: str,
    event: str,
    payload: list[str] | None = None,
    *,
    api_key: str | None = None,
    api_secret: str | None = None,
    ts: int | None = None,
) -> dict[str, Any]:
    """Build a subscribe/unsubscribe frame, signed when credentials are given."""
    ts = int(time.time()) if ts is None else ts
    frame: dict[str, Any] = {"time": ts, "channel": channel, "event": event}
    if payload is not None:
        frame["payload"] = payload
    if api_key and api_secret:
        frame["auth"] = {
            "method": "api_key",
            "KEY": api_key,
            "SIGN": sign(api_secret, channel, event, ts),
        }
    return frame


def ping_frame(channel: str = "spot.ping", ts: int | None = None) -> dict[str, Any]:
    """Application-level ping. Gate answers on the matching ``*.pong``."""
    return {"time": int(time.time()) if ts is None else ts, "channel": channel}


def api_sign(api_secret: str, channel: str, param_json: str, ts: int) -> str:
    """HMAC-SHA512 over ``api\\n<channel>\\n<req_param JSON>\\n<time>``.

    ``param_json`` must be the exact serialization that goes on the wire —
    Gate re-serializes what it receives to verify, so separators and key order
    have to line up. :func:`api_frame` guarantees that by signing and sending
    the same ``json.dumps`` output.
    """
    message = f"api\n{channel}\n{param_json}\n{ts}"
    return hmac.new(
        api_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha512,
    ).hexdigest()


def login_frame(
    *,
    api_key: str,
    api_secret: str,
    req_id: str | None = None,
    ts: int | None = None,
    channel: str = "spot.login",
) -> tuple[dict[str, Any], str]:
    """Build a login frame. Returns ``(frame, req_id)``.

    Gate signs login with an empty ``req_param`` string and omits ``req_param``
    from the payload entirely. ``channel`` is ``spot.login`` or
    ``futures.login``.
    """
    ts = int(time.time()) if ts is None else ts
    req_id = req_id or uuid.uuid4().hex
    payload: dict[str, Any] = {
        "api_key": api_key,
        "timestamp": str(ts),
        "signature": api_sign(api_secret, channel, "", ts),
        "req_id": req_id,
    }
    return (
        {"time": ts, "channel": channel, "event": "api", "payload": payload},
        req_id,
    )


def api_frame(
    channel: str,
    req_param: Any,
    *,
    api_key: str,
    api_secret: str,
    req_id: str | None = None,
    channel_id: str = "",
    ts: int | None = None,
) -> tuple[dict[str, Any], str]:
    """Build a signed trading-call frame. Returns ``(frame, req_id)``.

    Prefer :func:`session_api_frame` after login; this form is kept for
    callers that need a fully signed ``event: api`` payload.
    """
    ts = int(time.time()) if ts is None else ts
    req_id = req_id or uuid.uuid4().hex
    param_json = json.dumps(req_param)
    payload: dict[str, Any] = {
        "api_key": api_key,
        "timestamp": str(ts),
        "signature": api_sign(api_secret, channel, param_json, ts),
        "req_id": req_id,
        "req_param": req_param,
    }
    if channel_id:
        payload["req_header"] = {"X-Gate-Channel-Id": channel_id}
    return (
        {"time": ts, "channel": channel, "event": "api", "payload": payload},
        req_id,
    )


def session_api_frame(
    channel: str,
    req_param: Any,
    *,
    req_id: str | None = None,
    channel_id: str = "",
    ts: int | None = None,
) -> tuple[dict[str, Any], str]:
    """Build a trading-call frame for a socket that has already logged in.

    After login Gate authenticates the connection; subsequent ``event: api``
    calls carry only ``req_id`` / ``req_param`` (no per-call signature).
    """
    ts = int(time.time()) if ts is None else ts
    req_id = req_id or uuid.uuid4().hex
    payload: dict[str, Any] = {
        "req_id": req_id,
        "req_param": req_param,
    }
    if channel_id:
        payload["req_header"] = {"X-Gate-Channel-Id": channel_id}
    return (
        {"time": ts, "channel": channel, "event": "api", "payload": payload},
        req_id,
    )


def sign_rest(
    api_secret: str,
    method: str,
    path: str,
    query: str = "",
    body: str = "",
    ts: float | None = None,
) -> tuple[str, str]:
    """Return ``(signature, timestamp)`` for a REST call.

    Signs ``METHOD\\nPATH\\nQUERY\\nSHA512(body)\\nTIMESTAMP``. The timestamp is
    a float rendered as-is, matching Gate's own SDK — an int works too, but the
    same value must appear in the header and in the signed string.
    """
    ts = time.time() if ts is None else ts
    hashed_payload = hashlib.sha512(body.encode("utf-8")).hexdigest()
    message = f"{method}\n{path}\n{query}\n{hashed_payload}\n{ts}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha512,
    ).hexdigest()
    return signature, str(ts)


class GateResponse:
    """One decoded frame from the server, either style.

    Subscription frames carry ``channel``/``event``/``result`` at the top
    level. Trading-call replies nest theirs: the channel and event live under
    ``header``, and the payload under ``data.result`` / ``data.errs``. Both are
    normalized onto the same attributes here so the read loop has one shape to
    route on.
    """

    __slots__ = (
        "channel",
        "event",
        "time",
        "time_ms",
        "result",
        "error",
        "raw",
        "is_api",
        "req_id",
        "ack",
        "status",
        "api_error",
    )

    def __init__(self, message: dict[str, Any]) -> None:
        self.raw = message
        header = message.get("header") or {}
        self.is_api = bool(header) or message.get("event") == "api"
        self.channel: str = message.get("channel") or header.get("channel", "")
        self.event: str = message.get("event") or header.get("event", "")
        self.time: int | None = message.get("time")
        self.time_ms: int | None = message.get("time_ms")
        self.status: str = str(header.get("status", ""))
        self.req_id: str | None = message.get("request_id") or message.get("req_id")
        # Gate sends a preliminary ``ack: true`` frame before the real trading
        # reply. That frame often still carries ``data`` (an echo of the
        # request's ``req_param``), so presence of ``data`` must not clear the
        # ack flag — otherwise we treat the echo as the order and see an empty
        # identity field.
        self.ack: bool = bool(message.get("ack"))

        data = message.get("data") or {}
        self.result: Any = (
            data.get("result") if self.is_api else message.get("result")
        )

        err = message.get("error")
        self.error: GateWsError | None = None
        if err:
            self.error = GateWsError(
                err.get("code"), str(err.get("message", "")), channel=self.channel
            )

        errs = data.get("errs")
        self.api_error: GateApiError | None = None
        if errs:
            self.api_error = GateApiError(
                str(errs.get("label", "error")),
                str(errs.get("message", "")),
                channel=self.channel,
            )

    @property
    def ok(self) -> bool:
        return self.error is None and self.api_error is None

    def raise_for_error(self) -> None:
        if self.error is not None:
            raise self.error
        if self.api_error is not None:
            raise self.api_error

    def rows(self) -> list[dict[str, Any]]:
        """``result`` as a list of dicts, whichever shape the channel used."""
        if self.result is None:
            return []
        if isinstance(self.result, list):
            return [r for r in self.result if isinstance(r, dict)]
        if isinstance(self.result, dict):
            return [self.result]
        return []

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"GateResponse(channel={self.channel!r}, event={self.event!r}, "
            f"error={self.error!r})"
        )


__all__ = [
    "GateApiError",
    "GateResponse",
    "GateRestError",
    "GateWsError",
    "api_frame",
    "api_sign",
    "login_frame",
    "ping_frame",
    "request_frame",
    "session_api_frame",
    "sign",
    "sign_rest",
]
