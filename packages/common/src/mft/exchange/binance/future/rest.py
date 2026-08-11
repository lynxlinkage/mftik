"""Binance futures REST — only what the WebSocket API cannot answer.

The spot adapter has no REST client at all, on purpose: Binance answers every
read that platform needs over ``ws-api``. Futures does not, and the gaps are
not small ones:

* :class:`BinanceFuturePublicRest` — **candles and the instrument listing.**
  ``klines`` and ``exchangeInfo`` are not WebSocket API methods on this market
  at all, and the kline *stream* pushes the window in progress and nothing
  before it, so history has to be asked for.
* :class:`BinanceFutureRest` — **"what is open right now".** Futures has no
  ``openOrders.status``; the WebSocket API can only be asked about one order it
  is given the id of. Recon needs the list at attach time, and answering it
  with nothing would leave the OMS believing the account is flat, which is
  worse than not reconciling at all.

Signing is the same Ed25519 key the sockets use — Binance accepts it on REST
too — but the payload is a **query string**, not a JSON object: the parameters
are rendered ``k=v&k=v``, that exact string is signed, and the signature is
appended to it. Sorting the keys is ours to choose (the server rebuilds the
string from what it receives, in the order it receives it) and it is done for
the same reason the socket path sorts: one canonical rendering means the signed
bytes and the sent bytes cannot drift apart.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any
from urllib.parse import quote

import httpx

from mft.exchange.binance.future.models import (
    BinanceFutureDepth,
    BinanceFutureOrderAck,
    BinanceFutureSymbolConfig,
    instrument_from_row,
    kline_from_row,
)
from mft.exchange.binance.future.protocol import (
    BINANCE_FUTURE_REST_URL,
    load_private_key,
    now_ms,
    payload_for,
    sign,
)
from mft.exchange.binance.models import secs
from mft.exchange.errors import ExchangeError
from mft.exchange.models import Instrument, Kline, OrderBook, Ticker
from mft.exchange.tickers import UniversalTicker

logger = logging.getLogger(__name__)

API_PREFIX = "/fapi/v1"

#: Most candles ``/fapi/v1/klines`` returns in one call. Asking for more is a
#: 400, not a truncated answer.
MAX_KLINES = 1500

#: The only ``status`` that means a contract can be traded right now.
TRADING = "TRADING"

#: The only ``contractType`` this venue trades here. ``exchangeInfo`` also
#: lists dated futures (``BTCUSDT_250926``), which are different instruments
#: that canonicalize onto the perpetual's symbol — see
#: :mod:`mft_sym.sources.binance_future` for what that would collide with.
PERPETUAL = "PERPETUAL"


class BinanceFutureRestError(ExchangeError):
    """A non-2xx answer from Binance's futures REST API.

    Carries the same ``code`` the socket errors do — Binance uses one numbering
    across both transports — so TD and MD can normalize a REST refusal and a
    socket refusal through the same table.
    """

    def __init__(self, status: int, code: int | None, message: str) -> None:
        self.status = status
        self.code = code
        super().__init__(f"[{status}] [{code}] {message}")


class _FutureRestTransport:
    """httpx lifecycle and error decoding, shared by the signed/public pair."""

    def __init__(
        self,
        *,
        base_url: str = BINANCE_FUTURE_REST_URL,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
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

    @staticmethod
    def _parse(response: httpx.Response) -> Any:
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
            raise BinanceFutureRestError(response.status_code, code, message)
        return body


class BinanceFuturePublicRest(_FutureRestTransport):
    """The two public reads the futures WebSocket API does not serve."""

    async def fetch_instruments(self) -> list[Instrument]:
        """``GET /fapi/v1/exchangeInfo`` — every perpetual, in Binance's spelling.

        Left native on purpose: this is what the symbol plane ingests to
        *build* the canonical mapping, so it cannot depend on that mapping
        existing.

        Dated futures are dropped. They are listed on the same endpoint and
        their base and quote are the perpetual's, so ``BTCUSDT_250926`` and
        ``BTCUSDT`` canonicalize to one symbol — keeping both would mean one
        overwriting the other and orders for the perp routing to a contract
        that expires.
        """
        payload = await self._get(f"{API_PREFIX}/exchangeInfo")
        return [
            instrument_from_row(row)
            for row in (payload or {}).get("symbols") or []
            if row.get("status") == TRADING
            and str(row.get("contractType") or "") == PERPETUAL
        ]

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        *,
        ticker: UniversalTicker,
        limit: int = 100,
    ) -> list[Kline]:
        """``GET /fapi/v1/klines`` — recent candles, oldest first.

        ``interval`` is Binance's own spelling; translating from the canonical
        one happens a layer up, in
        :class:`~mft.exchange.binance.future.public.BinanceFuturePublicClient`.
        """
        rows = await self._get(
            f"{API_PREFIX}/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "limit": min(limit, MAX_KLINES),
            },
        )
        return [kline_from_row(row, ticker, interval) for row in rows or []]

    async def fetch_ticker(self, symbol: str, *, ticker: UniversalTicker) -> Ticker:
        """Last price and quote — two endpoints, because futures splits them.

        ``/ticker/24hr`` carries the rolling stats and the last price but no
        bid or ask, and ``/ticker/bookTicker`` carries the quote and no last
        price. Both are asked rather than one being derived: a mid price is not
        a last price, and a last price is not a quote.
        """
        stats = await self._get(f"{API_PREFIX}/ticker/24hr", {"symbol": symbol})
        quote = await self._get(f"{API_PREFIX}/ticker/bookTicker", {"symbol": symbol})
        row = _first(stats)
        book = _first(quote)
        return Ticker(
            universal_ticker=str(ticker),
            bid=Decimal(str(book.get("bidPrice", "0") or "0")),
            ask=Decimal(str(book.get("askPrice", "0") or "0")),
            last=Decimal(str(row.get("lastPrice", "0") or "0")),
            ts=secs(row.get("closeTime") or book.get("time") or 0),
        )

    async def fetch_order_book(
        self, symbol: str, *, ticker: UniversalTicker, depth: int = 100
    ) -> OrderBook:
        """``GET /fapi/v1/depth`` — a whole book, capped at ``depth``.

        The same book the WebSocket API's ``depth`` answers. It is here as well
        because the fetch plane holds this client and not a socket, and opening
        one to ask a single question is the coupling that plane exists to
        avoid.
        """
        payload = await self._get(
            f"{API_PREFIX}/depth", {"symbol": symbol, "limit": depth}
        )
        return BinanceFutureDepth.model_validate(payload or {}).to_order_book(ticker)


class BinanceFutureRest(_FutureRestTransport):
    """The signed recon read futures has nowhere else: open orders.

    Deliberately narrow. Order entry stays on the WebSocket API, where it is
    one authenticated connection and no per-call signature; this exists for the
    one question that has no method there.
    """

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        base_url: str = BINANCE_FUTURE_REST_URL,
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
        # configured rather than on the first recon.
        self._key = load_private_key(api_secret)

    async def fetch_open_orders(
        self, symbol: str | None = None
    ) -> list[BinanceFutureOrderAck]:
        """``GET /fapi/v1/openOrders`` — one symbol's resting orders, or all.

        Asking across the account is heavily weighted by Binance (it scans
        every symbol), so pass a symbol when one is known.
        """
        rows = await self._signed_get(
            f"{API_PREFIX}/openOrders", {"symbol": symbol} if symbol else {}
        )
        return [BinanceFutureOrderAck.model_validate(row) for row in rows or []]

    async def fetch_symbol_config(
        self, symbol: str | None = None
    ) -> list[BinanceFutureSymbolConfig]:
        """``GET /fapi/v1/symbolConfig`` — margin type and configured leverage.

        Answers for flat symbols too. ``positionRisk`` v3 does not — it only
        returns symbols with a position or resting order — so leverage for a
        book about to quote has to come from here.
        """
        rows = await self._signed_get(
            f"{API_PREFIX}/symbolConfig",
            {"symbol": symbol} if symbol else {},
        )
        return [
            BinanceFutureSymbolConfig.model_validate(row) for row in rows or []
        ]

    async def _signed_get(self, path: str, params: dict[str, Any]) -> Any:
        body = {k: v for k, v in params.items() if v is not None}
        body["timestamp"] = now_ms()
        if self.recv_window is not None:
            body["recvWindow"] = self.recv_window
        # Signed and sent from one rendering: ``payload_for`` is the same
        # function the socket path signs with, so the bytes cannot drift.
        # Nothing sent here needs percent-encoding — a symbol and two integers
        # — and the signature would be over the un-escaped form regardless.
        query = payload_for(body)
        signature = sign(self._key, body)
        return await self._get(
            f"{path}?{query}&signature={quote(signature, safe='')}",
            None,
            headers={"Accept": "application/json", "X-MBX-APIKEY": self.api_key},
        )


def _first(payload: Any) -> dict[str, Any]:
    """One row, whether Binance answered with an object or a one-item array.

    The ticker endpoints answer an array when asked for every symbol and a
    bare object when asked for one; reading both shapes here keeps that off
    every caller.
    """
    if isinstance(payload, list):
        return dict(payload[0]) if payload else {}
    return dict(payload or {})


__all__ = [
    "API_PREFIX",
    "MAX_KLINES",
    "PERPETUAL",
    "TRADING",
    "BinanceFuturePublicRest",
    "BinanceFutureRest",
    "BinanceFutureRestError",
]
