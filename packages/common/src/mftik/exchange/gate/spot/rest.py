"""Gate spot REST v4 — only the reads the WebSocket API cannot serve.

Two clients, split by whether the call is signed:

* :class:`GateSpotRest` — TD's recon reads. Gate's spot WebSocket API has
  ``spot.order_status`` for a single order, but no ``spot.order_list`` (futures
  has one; spot does not) and no balance query, and recon needs both at attach
  time. Returning nothing there would leave the OMS believing the account is
  flat, which is worse than not reconciling at all.
* :class:`GateSpotPublicRest` — MD's snapshot reads. Instruments, ticker and
  order book have no WebSocket equivalent Gate will answer on demand: the
  channels push on their own schedule, and a caller asking for "the book right
  now" cannot wait for the next tick. Candles are the same problem in the
  other direction — ``spot.candlesticks`` pushes the window in progress and
  nothing before it, so history has to be asked for.

Deliberately not a general REST client. Order entry and every live feed stay on
the WebSocket, where they belong latency-wise; this covers the request-reply
holes only.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from decimal import Decimal
from typing import Any

import httpx

from mftik.exchange.errors import ExchangeError
from mftik.exchange.gate.spot.models import (
    GateOrderAck,
    GateTicker,
    GateUserTrade,
)
from mftik.exchange.models import (
    Balance,
    BookLevel,
    Instrument,
    Kline,
    Order,
    OrderBook,
    Ticker,
)
from mftik.exchange.tickers import UniversalTicker

logger = logging.getLogger(__name__)

GATE_SPOT_REST_URL = "https://api.gateio.ws"
API_PREFIX = "/api/v4"

#: Most candles ``/spot/candlesticks`` will return in one call. Asking for more
#: is a 400, not a truncated answer.
MAX_CANDLES = 1000

#: Most history rows ``my_trades`` / ``orders`` return per page.
MAX_HISTORY = 1000


class GateRestError(ExchangeError):
    """Non-2xx or error-bodied response from Gate's REST API."""

    def __init__(self, status: int, label: str, message: str) -> None:
        self.status = status
        self.label = label
        super().__init__(f"[{status}] {label}: {message}")


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


class _GateRestTransport:
    """httpx lifecycle and error decoding, shared by the signed/public pair."""

    def __init__(
        self,
        *,
        base_url: str = GATE_SPOT_REST_URL,
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

    def _headers(self, path: str, query: str) -> dict[str, str]:
        """Per-request headers. Public calls send none beyond ``Accept``."""
        return {"Accept": "application/json"}

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        await self.connect()
        assert self._client is not None
        full_path = f"{API_PREFIX}{path}"
        query = "&".join(f"{k}={v}" for k, v in (params or {}).items())
        response = await self._client.get(
            full_path,
            params=params,
            headers=self._headers(full_path, query),
        )
        return self._parse(response)

    def _parse(self, response: httpx.Response) -> Any:
        try:
            payload = response.json()
        except ValueError:
            raise GateRestError(
                response.status_code, "bad_response", response.text[:200]
            ) from None
        if response.status_code >= 400:
            label = "error"
            message = str(payload)
            if isinstance(payload, dict):
                label = str(payload.get("label", label))
                message = str(payload.get("message", message))
            raise GateRestError(response.status_code, label, message)
        return payload


class GateSpotRest(_GateRestTransport):
    """Signed GETs for the two snapshots recon needs."""

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        base_url: str = GATE_SPOT_REST_URL,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(base_url=base_url, timeout=timeout, client=client)
        self.api_key = api_key
        self.api_secret = api_secret

    def _headers(self, path: str, query: str) -> dict[str, str]:
        signature, ts = sign_rest(self.api_secret, "GET", path, query)
        return {
            "KEY": self.api_key,
            "Timestamp": ts,
            "SIGN": signature,
            "Accept": "application/json",
        }

    async def fetch_open_orders(
        self, currency_pair: str, *, ticker: UniversalTicker
    ) -> list[Order]:
        """``GET /spot/orders?status=open`` — one pair's resting orders."""
        rows = await self._get(
            "/spot/orders",
            {"currency_pair": currency_pair, "status": "open"},
        )
        return [_to_order(row, ticker) for row in rows or []]

    async def fetch_my_trades(
        self,
        currency_pair: str,
        *,
        page: int = 1,
        limit: int = MAX_HISTORY,
        since: int | None = None,
    ) -> list[GateUserTrade]:
        """``GET /spot/my_trades`` — this account's executions on one pair.

        Paginated by page number, which Gate offers instead of an id cursor.
        That is weaker than Binance's or Bybit's: page N only means anything
        against a fixed query, so a walk fixes ``from`` for its whole run and
        re-derives it from the settlement line on the next one. The caller is
        what keeps those two together; see the Gate history reader.

        ``since`` is Gate's ``from``, in **seconds** — not the milliseconds
        Binance and Bybit take. Passing millis is not rejected: it asks for
        trades after a date fifty thousand years out and gets ``200`` with an
        empty page, which reads as an account that never traded.

        Unlike the other venues, a Gate trade row carries ``text`` — our own
        client order id — so an execution re-read here needs no join to be
        attributed.
        """
        params: dict[str, Any] = {
            "currency_pair": currency_pair,
            "limit": min(limit, MAX_HISTORY),
            "page": max(1, page),
        }
        if since is not None:
            params["from"] = since
        rows = await self._get("/spot/my_trades", params)
        return [GateUserTrade.model_validate(row) for row in rows or []]

    async def fetch_orders(
        self,
        currency_pair: str,
        *,
        page: int = 1,
        limit: int = MAX_HISTORY,
        since: int | None = None,
        status: str = "finished",
    ) -> list[GateOrderAck]:
        """``GET /spot/orders`` — one pair's orders in a terminal state.

        ``finished`` rather than ``open``: what a history walk wants is orders
        that have stopped moving, and the open ones are recon's business and
        arrive on the socket anyway.

        Read alongside the trades even though Gate's trade rows are already
        attributable, because this is what makes orders placed outside the
        platform visible — the same reason it is read on every other venue.

        ``since`` is in seconds, as on :meth:`fetch_my_trades`.
        """
        params: dict[str, Any] = {
            "currency_pair": currency_pair,
            "status": status,
            "limit": min(limit, MAX_HISTORY),
            "page": max(1, page),
        }
        if since is not None:
            params["from"] = since
        rows = await self._get("/spot/orders", params)
        return [GateOrderAck.model_validate(row) for row in rows or []]

    async def fetch_open_order_rows(self) -> dict[str, list[GateOrderAck]]:
        """``GET /spot/open_orders`` — every pair's resting orders, grouped.

        Venue-native, and grouped rather than flattened, because the two are
        the same fact: each group is a different instrument, and turning a row
        into an :class:`~mftik.exchange.models.Order` needs that instrument's
        ticker. Flattening here would throw away the only thing that says which
        row belongs to which — see
        :meth:`~mftik.exchange.gate.spot.private.GateSpotPrivateClient.\
fetch_open_orders`, which resolves one ticker per group rather than one per
        row.
        """
        grouped = await self._get("/spot/open_orders")
        out: dict[str, list[GateOrderAck]] = {}
        for group in grouped or []:
            pair = str(group.get("currency_pair") or "")
            rows = [
                GateOrderAck.model_validate(row)
                for row in group.get("orders", []) or []
            ]
            if not pair or not rows:
                continue
            out.setdefault(pair, []).extend(rows)
        return out

    async def fetch_balances(self) -> list[Balance]:
        """``GET /spot/accounts`` — per-currency available/locked."""
        rows = await self._get("/spot/accounts")
        return [
            Balance(
                asset=str(row.get("currency", "")),
                free=_dec(row.get("available")),
                locked=_dec(row.get("locked")),
            )
            for row in rows or []
        ]

    async def fetch_order(
        self, order_id: str, *, currency_pair: str, ticker: UniversalTicker
    ) -> Order:
        """``GET /spot/orders/{order_id}`` — used to resolve a single order."""
        row = await self._get(
            f"/spot/orders/{order_id}", {"currency_pair": currency_pair}
        )
        return _to_order(row, ticker)


class GateSpotPublicRest(_GateRestTransport):
    """Unsigned GETs for the market-data snapshots MD asks for on demand.

    Takes no credentials: Gate serves all three of these to anyone, and
    requiring keys for public data would mean MD could not run a feed without
    a trading account.
    """

    async def fetch_instruments(self) -> list[Instrument]:
        """``GET /spot/currency_pairs`` — tradeable pairs and their filters."""
        rows = await self._get("/spot/currency_pairs")
        return [
            _to_instrument(row)
            for row in rows or []
            # The endpoint also lists delisted and pre-launch pairs; the shared
            # contract is "tradeable", and Instrument has nowhere to say
            # otherwise, so a non-tradable pair is dropped rather than returned
            # looking live.
            if row.get("trade_status") == "tradable"
        ]

    async def fetch_ticker(
        self, currency_pair: str, *, ticker: UniversalTicker
    ) -> Ticker:
        """``GET /spot/tickers`` — the same row shape ``spot.tickers`` pushes."""
        rows = await self._get("/spot/tickers", {"currency_pair": currency_pair})
        if not rows:
            raise GateRestError(200, "not_found", f"no ticker for {currency_pair}")
        return GateTicker.model_validate(rows[0]).to_ticker(ticker)

    async def fetch_order_book(
        self, currency_pair: str, *, ticker: UniversalTicker, depth: int = 10
    ) -> OrderBook:
        """``GET /spot/order_book`` — a full snapshot, capped at ``depth``.

        The reply carries no pair, so the caller's ticker is stamped on.
        """
        row = await self._get(
            "/spot/order_book", {"currency_pair": currency_pair, "limit": depth}
        )
        return OrderBook(
            universal_ticker=str(ticker),
            bids=_book_levels(row.get("bids")),
            asks=_book_levels(row.get("asks")),
            # ``current`` is when Gate built the snapshot, in ms.
            ts=float(row.get("current", 0) or 0) / 1000.0,
        )

    async def fetch_klines(
        self,
        currency_pair: str,
        interval: str,
        *,
        ticker: UniversalTicker,
        limit: int = 100,
    ) -> list[Kline]:
        """``GET /spot/candlesticks`` — recent candles, oldest first.

        ``interval`` is Gate's own spelling; translating from the canonical one
        happens a layer up, in :class:`GateSpotPublicClient`.

        Gate caps ``limit`` at :data:`MAX_CANDLES` and answers 400 past it.
        That is left to raise: silently returning fewer candles than asked for
        would be indistinguishable from the pair simply not having that much
        history.
        """
        rows = await self._get(
            "/spot/candlesticks",
            {
                "currency_pair": currency_pair,
                "interval": interval,
                "limit": limit,
            },
        )
        return [_to_kline(row, ticker, interval) for row in rows or []]


def _to_kline(row: list[Any], ticker: UniversalTicker, interval: str) -> Kline:
    """One ``/spot/candlesticks`` row.

    Gate's column order is its own: the close price comes *before* high, low
    and open, so these indices cannot be read as OHLC and must not be
    reordered on the assumption that they are::

        [0] window open time, seconds   [4] low
        [1] volume in quote currency    [5] open
        [2] close                       [6] volume in base currency
        [3] high                        [7] "true" once the window has closed

    Index 7 was added to the endpoint after the others; a row without it is
    treated as still open, which is the safe reading — a candle wrongly called
    closed gets appended to a series and never revised.
    """
    if len(row) < 7:
        raise GateRestError(
            200,
            "bad_response",
            f"candlestick row for {ticker} {interval} has "
            f"{len(row)} columns, expected at least 7: {row!r}",
        )
    return Kline(
        universal_ticker=str(ticker),
        interval=interval,
        open_time=float(row[0]),
        open=Decimal(str(row[5])),
        high=Decimal(str(row[3])),
        low=Decimal(str(row[4])),
        close=Decimal(str(row[2])),
        volume=Decimal(str(row[6])),
        quote_volume=Decimal(str(row[1])),
        closed=len(row) > 7 and str(row[7]).lower() == "true",
    )


def _book_levels(rows: list[Any] | None) -> list[BookLevel]:
    return [
        BookLevel(price=Decimal(str(row[0])), qty=Decimal(str(row[1])))
        for row in rows or []
        if len(row) >= 2
    ]


def _to_instrument(row: dict[str, Any]) -> Instrument:
    """Gate publishes precision as decimal places; Instrument wants a step."""
    return Instrument(
        symbol=str(row.get("id", "")),
        base=str(row.get("base", "")),
        quote=str(row.get("quote", "")),
        tick_size=_step(row.get("precision")),
        lot_size=_step(row.get("amount_precision")),
        min_qty=_opt_dec(row.get("min_base_amount")),
        min_notional=_opt_dec(row.get("min_quote_amount")),
    )


def _step(precision: Any) -> Decimal:
    """``6`` → ``0.000001``. Gate omits it on a few pairs; assume whole units."""
    if precision is None or precision == "":
        return Decimal("1")
    return Decimal(1).scaleb(-int(precision))


def _opt_dec(value: Any) -> Decimal | None:
    """``None`` where Gate publishes no bound, so the filter reads as absent."""
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _dec(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def _to_order(row: dict[str, Any], ticker: UniversalTicker) -> Order:
    """REST order rows share the trading-call reply shape (``status``-based)."""
    return GateOrderAck.model_validate(row).to_order(ticker)


__all__ = [
    "API_PREFIX",
    "GATE_SPOT_REST_URL",
    "GateRestError",
    "GateSpotPublicRest",
    "GateSpotRest",
    "sign_rest",
]
