"""Binance WebSocket framing, Ed25519 signing, and response envelopes.

Every Binance socket this platform speaks to uses the same envelope —
``{"id", "method", "params"}`` out, ``{"id", "status", "result" | "error"}``
back — which is why one :class:`BinanceResponse` and one request/reply
correlation path cover the spot WebSocket API, the futures WebSocket API and
both venues' market streams alike. What differs is only *what* they answer, not
how, so this module holds no URL and knows no market: each product package
(:mod:`mft.exchange.binance.spot`, :mod:`mft.exchange.binance.future`) states
its own endpoints and its own method names on top of this.

**Signing is Ed25519, not HMAC.** Binance ties the session logon that trading
runs through to Ed25519 keys specifically, so this adapter takes an Ed25519
private key where Gate takes an HMAC secret, and the venue registry records
that difference as :data:`mft.exchange.venues.ED25519`.

The signed payload is the request's own ``params`` — every key except
``signature``, sorted by name, joined ``k=v`` with ``&``, signed as UTF-8, and
base64'd. Sorting is Binance's rule and not an implementation detail: the
server rebuilds the same string from what it receives, so a different order is
a different message and fails verification.

The one thing that makes this bearable compared to a REST-and-socket venue:
after ``session.logon`` the connection itself is authenticated, and every later
call carries no key and no signature at all. So the signing above happens
exactly once per connection, at logon, and the order path does no crypto.
"""

from __future__ import annotations

import base64
import binascii
import json
import time
import uuid
from decimal import Decimal
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    load_der_private_key,
    load_pem_private_key,
)

from mft.exchange.errors import ExchangeError

#: The one method name this module needs to know, because :func:`logon_frame`
#: builds it. Every other name lives in a product's ``methods`` module; this one
#: is spelled identically on both of them and importing either from here would
#: close a cycle.
SESSION_LOGON = "session.logon"

#: Raw Ed25519 private keys are 32 bytes; a PKCS#8 DER wrapper adds 16.
_SEED_LEN = 32


class BinanceWsError(ExchangeError):
    """Binance answered a request with an ``error`` block.

    ``code`` is kept unformatted as well as folded into the string form: TD and
    MD normalize on it, and should not have to parse it back out of
    ``str(exc)``. Binance's codes are negative integers (``-2010`` insufficient
    balance, ``-2011`` unknown order, ``-1121`` invalid symbol) and are the
    only machine-readable thing on the wire — there is no ``label`` here as
    there is on Gate.

    ``status`` is the HTTP-shaped status the frame carried (400, 418, 429, 5xx)
    and says something the code does not: whether we were rate limited or
    banned rather than refused on the merits.
    """

    def __init__(
        self,
        code: int | None,
        message: str,
        *,
        status: int | None = None,
        method: str = "",
    ) -> None:
        self.code = code
        self.status = status
        self.method = method
        prefix = f"{method}: " if method else ""
        super().__init__(f"{prefix}[{code}] {message}")


class BinanceAuthError(ExchangeError):
    """The credential could not be loaded or used at all.

    Distinct from a :class:`BinanceWsError` carrying an auth code: this one
    never reached the venue. A key that is not Ed25519, or is not a key,
    is a configuration mistake and should read as one.
    """


def load_private_key(api_secret: str) -> Ed25519PrivateKey:
    """Load an Ed25519 private key from whatever form it was stored in.

    Binance's own examples keep the key in a PKCS#8 PEM file, so that is the
    shape most users have. But a credential reaches us through a database
    column and an env var, and both of those routinely mangle a PEM: newlines
    become the two characters ``\\n``, or the armour gets stripped and only the
    base64 body is kept. All three are accepted here, plus the bare 32-byte
    seed some tooling emits, because the alternative is a user who pasted a
    perfectly good key being told it is invalid.

    What is *not* accepted is an RSA or EC key. Binance signs the session logon
    with Ed25519 and nothing else, so a key of another type cannot work, and
    failing here beats failing at logon with a signature error that suggests
    the key is wrong rather than the algorithm.
    """
    text = (api_secret or "").strip()
    if not text:
        raise BinanceAuthError("api_secret is required; it is the Ed25519 private key")

    if "-----BEGIN" in text:
        # An env var or a JSON round trip turns the real newlines into the two
        # characters backslash-n; PEM parsing needs them back.
        pem = text.replace("\\n", "\n").encode("utf-8")
        try:
            key = load_pem_private_key(pem, password=None)
        except (ValueError, TypeError, InvalidSignature) as exc:
            raise BinanceAuthError(
                f"api_secret is not a readable PEM key: {exc}"
            ) from exc
        return _as_ed25519(key)

    try:
        raw = base64.b64decode(_strip(text), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise BinanceAuthError(
            "api_secret is neither a PEM key nor base64; Binance signs with an "
            "Ed25519 private key"
        ) from exc

    if len(raw) == _SEED_LEN:
        return Ed25519PrivateKey.from_private_bytes(raw)
    try:
        key = load_der_private_key(raw, password=None)
    except (ValueError, TypeError, InvalidSignature) as exc:
        raise BinanceAuthError(
            f"api_secret decoded to {len(raw)} bytes, which is neither a "
            f"{_SEED_LEN}-byte Ed25519 seed nor a PKCS#8 key: {exc}"
        ) from exc
    return _as_ed25519(key)


def _as_ed25519(key: object) -> Ed25519PrivateKey:
    if not isinstance(key, Ed25519PrivateKey):
        raise BinanceAuthError(
            f"api_secret is a {type(key).__name__}; Binance's WebSocket API "
            f"signs with Ed25519 keys only"
        )
    return key


def _strip(text: str) -> str:
    """Drop PEM armour remnants and whitespace from a pasted key body."""
    return "".join(
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.startswith("-----")
    )


def decimal_text(value: Decimal) -> str:
    """A number as Binance wants it written.

    Two things go wrong if a ``Decimal`` is handed over as it comes:

    ``1E-8`` — plenty of real tick sizes are small enough for ``str`` to reach
    for exponent notation, which Binance does not parse as a price. ``f``
    formatting keeps it positional.

    ``0.00780000`` — trailing zeros are the ``Decimal``'s *scale*, not
    formatting, and arithmetic propagates them: a size floored against a lot
    step written ``0.00010000`` carries eight decimals whatever the number
    actually is. Binance checks the written precision against the symbol's
    step and answers ``-1111`` for a size with more digits than the step
    allows — so ``0.00780000`` is refused where ``0.0078`` is taken, though
    they are the same quantity. Stripping here means no caller has to know
    that, and none can forget.
    """
    stripped = value.normalize()
    # ``normalize`` renders a whole number exponentially (10 → 1E+1), which is
    # the other spelling Binance will not read.
    if stripped.as_tuple().exponent > 0:
        stripped = stripped.quantize(Decimal(1))
    return f"{stripped:f}"


def wire(value: Any) -> Any:
    """One param value, as it goes into the JSON frame.

    Only ``Decimal`` needs changing: it is not JSON-serializable, and how it is
    written matters to Binance — see :func:`decimal_text`. Everything else —
    ints, bools, arrays — is already a JSON type and stays one, because
    Binance's ``params`` is a real object and a param documented as an array
    must not arrive as a quoted string.
    """
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, (list, tuple)):
        return [wire(item) for item in value]
    return value


def render(value: Any) -> str:
    """One param value, as it goes into the signature.

    Must agree with :func:`wire` on every value: the server rebuilds this
    string from the frame it received, so a bool rendered Python-style
    (``True``), an array rendered with Python's repr, or a number written to a
    different scale would verify against something we never sent. That is why
    both go through :func:`decimal_text` rather than formatting separately.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, (list, tuple)):
        return json.dumps(list(value), separators=(",", ":"))
    return str(value)


def payload_for(params: dict[str, Any]) -> str:
    """The string Binance signs: params sorted by name, ``k=v`` joined by ``&``.

    ``signature`` is excluded, because it is what we are computing.
    """
    return "&".join(
        f"{key}={render(value)}"
        for key, value in sorted(params.items())
        if key != "signature"
    )


def sign(private_key: Ed25519PrivateKey, params: dict[str, Any]) -> str:
    """Base64 Ed25519 signature over :func:`payload_for`."""
    return base64.b64encode(
        private_key.sign(payload_for(params).encode("utf-8"))
    ).decode("ascii")


def now_ms() -> int:
    """Wall clock in milliseconds — the unit every Binance timestamp uses.

    Public because a signed frame is not the only thing that needs one: a
    ``session.logon`` replaces the ``apiKey`` and the ``signature`` on later
    calls, but not the ``timestamp``, so calls made over an authenticated
    session still have to stamp themselves. See
    :meth:`~mft.exchange.binance.spot.client.BinanceSpotWsApi.call` and its
    futures counterpart.
    """
    return int(time.time() * 1000)


def request_frame(
    method: str,
    params: dict[str, Any] | None = None,
    *,
    req_id: str | None = None,
) -> tuple[dict[str, Any], str]:
    """An unsigned request. Returns ``(frame, req_id)``.

    Every value goes through :func:`wire`, so the frame is JSON-serializable
    whatever the caller passed — a ``Decimal`` quantity would otherwise fail at
    ``json.dumps`` rather than at the type that produced it. ``None`` values
    are dropped: Binance reads a null param as a malformed one, and an optional
    argument left unset should simply not be there.
    """
    req_id = req_id or uuid.uuid4().hex
    frame: dict[str, Any] = {"id": req_id, "method": method}
    if params:
        body = {k: wire(v) for k, v in params.items() if v is not None}
        if body:
            frame["params"] = body
    return frame, req_id


def signed_frame(
    method: str,
    params: dict[str, Any] | None = None,
    *,
    api_key: str,
    private_key: Ed25519PrivateKey,
    recv_window: int | None = None,
    req_id: str | None = None,
    ts: int | None = None,
) -> tuple[dict[str, Any], str]:
    """A request carrying its own ``apiKey`` + ``signature``.

    Only needed on a connection that has *not* logged on. After
    ``session.logon`` the session authenticates every call and
    :func:`request_frame` is enough — see the module docstring.
    """
    body: dict[str, Any] = {
        k: wire(v) for k, v in (params or {}).items() if v is not None
    }
    body["apiKey"] = api_key
    body["timestamp"] = now_ms() if ts is None else ts
    if recv_window is not None:
        body["recvWindow"] = recv_window
    body["signature"] = sign(private_key, body)
    req_id = req_id or uuid.uuid4().hex
    return {"id": req_id, "method": method, "params": body}, req_id


def logon_frame(
    *,
    api_key: str,
    private_key: Ed25519PrivateKey,
    recv_window: int | None = None,
    req_id: str | None = None,
    ts: int | None = None,
) -> tuple[dict[str, Any], str]:
    """A ``session.logon`` frame. Returns ``(frame, req_id)``.

    Signs ``apiKey`` and ``timestamp`` — the only params logon takes — which is
    the one signature this adapter computes per connection.
    """
    return signed_frame(
        SESSION_LOGON,
        None,
        api_key=api_key,
        private_key=private_key,
        recv_window=recv_window,
        req_id=req_id,
        ts=ts,
    )


def subscribe_frame(
    method: str,
    params: list[str],
    *,
    req_id: str | None = None,
) -> tuple[dict[str, Any], str]:
    """A market-stream ``SUBSCRIBE`` / ``UNSUBSCRIBE``. Returns ``(frame, id)``.

    Same envelope as the WebSocket API but ``params`` is a bare list of stream
    names rather than an object, which is why it does not go through
    :func:`request_frame`.
    """
    req_id = req_id or uuid.uuid4().hex
    return {"id": req_id, "method": method, "params": list(params)}, req_id


class BinanceResponse:
    """One decoded frame, whichever socket and whichever shape it arrived in.

    Four shapes are normalized onto the same attributes so the read loop has
    one thing to route on:

    * a request reply — ``id`` plus ``status`` and ``result`` or ``error``;
    * a market push — ``{"stream": ..., "data": {...}}`` from the combined
      endpoint;
    * a user data push — ``{"subscriptionId": ..., "event": {...}}`` from a
      logged-on WebSocket API session;
    * a subscribe ack — ``{"id": ..., "result": null}``, which is a reply with
      nothing in it.

    A frame is a reply if and only if it has an ``id``; both push shapes carry
    none. That is the whole routing rule, and it is why this adapter needs no
    arrival-order correlation.
    """

    __slots__ = (
        "raw",
        "id",
        "status",
        "result",
        "error",
        "rate_limits",
        "stream",
        "data",
        "event",
        "event_type",
        "subscription_id",
    )

    def __init__(self, message: dict[str, Any]) -> None:
        self.raw = message
        raw_id = message.get("id")
        self.id: str | None = None if raw_id is None else str(raw_id)
        self.status: int | None = message.get("status")
        self.result: Any = message.get("result")
        self.rate_limits: list[dict[str, Any]] = message.get("rateLimits") or []

        self.stream: str = message.get("stream") or ""
        self.data: Any = message.get("data")

        event = message.get("event")
        self.event: dict[str, Any] | None = event if isinstance(event, dict) else None
        self.event_type: str = self.event.get("e", "") if self.event else ""
        self.subscription_id: int | None = message.get("subscriptionId")

        err = message.get("error")
        self.error: BinanceWsError | None = None
        if isinstance(err, dict):
            self.error = BinanceWsError(
                err.get("code"),
                str(err.get("msg", "")),
                status=self.status,
            )

    @property
    def is_reply(self) -> bool:
        """Whether this frame answers a request we are waiting on."""
        return self.id is not None

    @property
    def is_push(self) -> bool:
        """Whether this frame is an unsolicited market or account update."""
        return self.id is None and (bool(self.stream) or self.event is not None)

    @property
    def ok(self) -> bool:
        return self.error is None

    def raise_for_error(self) -> None:
        if self.error is not None:
            raise self.error

    def rows(self) -> list[dict[str, Any]]:
        """``result`` as a list of dicts, whichever shape the method used."""
        if isinstance(self.result, list):
            return [row for row in self.result if isinstance(row, dict)]
        if isinstance(self.result, dict):
            return [self.result]
        return []

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        what = self.stream or self.event_type or f"id={self.id}"
        return f"BinanceResponse({what}, status={self.status}, error={self.error!r})"


__all__ = [
    "SESSION_LOGON",
    "BinanceAuthError",
    "BinanceResponse",
    "BinanceWsError",
    "decimal_text",
    "load_private_key",
    "logon_frame",
    "now_ms",
    "payload_for",
    "render",
    "request_frame",
    "sign",
    "signed_frame",
    "subscribe_frame",
    "wire",
]
