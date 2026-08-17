"""Binance REST — the transport both products build, sign and read the same way.

Binance's REST surface is one API behind two hosts: ``api.binance.com`` for
spot and ``fapi.binance.com`` for futures. Everything between the caller and
the wire is identical across them — the httpx lifecycle, the ``{"code",
"msg"}`` error body, and the per-call signature — so it lives here, and each
product's ``rest`` module is left holding only its URLs, its paths and its
models. Same split as :mod:`mftik.exchange.binance.protocol`, for the same
reason.

Signing is the same Ed25519 key the sockets use — Binance accepts it on REST
too — but the payload is a **query string**, not a JSON object: the parameters
are rendered ``k=v&k=v``, that exact string is signed, and the signature is
appended to it. Sorting the keys is ours to choose (the server rebuilds the
string from what it receives, in the order it receives it) and it is done for
the same reason the socket path sorts: one canonical rendering means the signed
bytes and the sent bytes cannot drift apart.

Each product keeps its own error subclass. The code Binance returns is numbered
across the whole venue, so nothing downstream needs to tell them apart — but a
traceback that names the product it came from is worth the two lines.
"""

from __future__ import annotations

from typing import Any, ClassVar
from urllib.parse import quote

import httpx

from mftik.exchange.binance.protocol import (
    load_private_key,
    now_ms,
    payload_for,
    sign,
)
from mftik.exchange.errors import ExchangeError


class BinanceRestError(ExchangeError):
    """A non-2xx answer from a Binance REST API.

    Carries the same ``code`` the socket errors do — Binance uses one numbering
    across both transports — so TD and MD can normalize a REST refusal and a
    socket refusal through the same table.
    """

    def __init__(self, status: int, code: int | None, message: str) -> None:
        self.status = status
        self.code = code
        super().__init__(f"[{status}] [{code}] {message}")


class BinanceRestTransport:
    """httpx lifecycle and error decoding for one Binance host.

    Subclasses set :attr:`default_base_url` and :attr:`error_type`; everything
    else about getting bytes out and a decoded body back is the same.
    """

    #: The product's host. A ``base_url`` argument still wins, for testnet.
    default_base_url: ClassVar[str] = ""
    #: Raised on a 4xx/5xx, so a traceback names the product.
    error_type: ClassVar[type[BinanceRestError]] = BinanceRestError

    def __init__(
        self,
        *,
        base_url: str | None = None,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = (base_url or self.default_base_url).rstrip("/")
        self.timeout = timeout
        self._client = client
        self._owns_client = client is None

    async def connect(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url, timeout=self.timeout
            )
            self._owns_client = True

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    async def _get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        headers: dict[str, str] | None = None,
    ) -> Any:
        await self.connect()
        assert self._client is not None
        response = await self._client.get(
            path,
            params=params or None,
            headers=headers or {"Accept": "application/json"},
        )
        return self._parse(response)

    @classmethod
    def _parse(cls, response: httpx.Response) -> Any:
        """The body, or the venue's own error rather than an HTTP one.

        Binance reports refusals as ``{"code": -1121, "msg": "..."}`` with a
        4xx status, and the code says far more than the status does — so it is
        read out rather than left inside a ``raise_for_status`` message.
        """
        try:
            body = response.json()
        except ValueError:
            body = None
        if response.status_code >= 400:
            code = None
            message = response.text[:300]
            if isinstance(body, dict):
                code = body.get("code")
                message = str(body.get("msg") or message)
            raise cls.error_type(response.status_code, code, message)
        return body


class BinanceSignedRest(BinanceRestTransport):
    """A REST client that signs every call with the account's Ed25519 key."""

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        base_url: str | None = None,
        timeout: float = 10.0,
        recv_window: int | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(base_url=base_url, timeout=timeout, client=client)
        if not api_key or not api_secret:
            raise ValueError("api_key and api_secret are required")
        self.api_key = api_key
        self.recv_window = recv_window
        # Parsed at construction, so a malformed key fails where it was
        # configured rather than on the first call.
        self._key = load_private_key(api_secret)

    async def _signed_get(self, path: str, params: dict[str, Any]) -> Any:
        body = {k: v for k, v in params.items() if v is not None}
        body["timestamp"] = now_ms()
        if self.recv_window is not None:
            body["recvWindow"] = self.recv_window
        # Signed and sent from one rendering: ``payload_for`` is the same
        # function the socket path signs with, so the bytes cannot drift.
        # Nothing sent here needs percent-encoding — symbols and integers —
        # and the signature would be over the un-escaped form regardless.
        query = payload_for(body)
        signature = sign(self._key, body)
        return await self._get(
            f"{path}?{query}&signature={quote(signature, safe='')}",
            None,
            headers={"Accept": "application/json", "X-MBX-APIKEY": self.api_key},
        )


__all__ = [
    "BinanceRestError",
    "BinanceRestTransport",
    "BinanceSignedRest",
]
