"""Deribit public JSON-RPC over HTTP.

Public GETs stay unsigned. The path is the method
(``/public/get_instruments``) and the query is the params. Private
trading rides the authenticated socket, not this client.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx

from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.listing import to_listed
from mftik.exchange.deribit.models import (
    DeribitTicker,
    kline_from_chart,
    order_book_from_result,
)
from mftik.exchange.deribit.protocol import (
    DERIBIT_REST_URL,
    KIND_SPOT,
    DeribitRestError,
)
from mftik.exchange.intervals import interval_seconds
from mftik.exchange.models import FundingRate, Kline, OpenInterest, OrderBook, Ticker
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.symbols.listed import ListedInstrument

MAX_KLINES = 500

#: How often an account worker reads public server time. Deribit
#: documents that an HTTP connection expires after 15 minutes of
#: inactivity, and that keep-alive is still terminated after 15 minutes.
#: 60s is under that idle close.
KEEPALIVE_INTERVAL_S = 60.0
#: Longer than :data:`KEEPALIVE_INTERVAL_S`, so the pool does not drop
#: the socket between ticks. Still under Deribit's 15-minute cap.
KEEPALIVE_EXPIRY_S = 180.0
#: Resolved against :data:`~mftik.exchange.deribit.protocol.DERIBIT_REST_URL`
#: (``.../api/v2``). ``public/get_time`` is a public read.
KEEPALIVE_PATH = "/api/v2/public/get_time"
#: Connection counts are httpx's own defaults (100 and 20). Only the
#: expiry changes.
#: Default; adjust from measurement (Appendix D).
POOL_LIMITS = httpx.Limits(
    max_connections=100,
    max_keepalive_connections=20,
    keepalive_expiry=KEEPALIVE_EXPIRY_S,
)


class DeribitPublicRest:
    """Unsigned reads — the market-data snapshots MD asks for on demand."""

    def __init__(
        self,
        *,
        base_url: str = DERIBIT_REST_URL,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client = client
        self._owns_client = client is None
        self.api_secret = None

    async def connect(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                limits=POOL_LIMITS,
            )
            self._owns_client = True

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    async def _get(self, method: str, params: dict[str, Any] | None = None) -> Any:
        await self.connect()
        assert self._client is not None
        path = method[1:] if method.startswith("/") else method
        response = await self._client.get(path, params=params or {})
        return self._parse(response, method)

    def _parse(self, response: httpx.Response, method: str) -> Any:
        try:
            payload = response.json()
        except ValueError:
            raise DeribitRestError(
                None,
                response.text[:200],
                status=response.status_code,
                op=method,
            ) from None
        if not isinstance(payload, dict):
            raise DeribitRestError(
                None,
                f"unexpected body {payload!r}",
                status=response.status_code,
                op=method,
            )
        error = payload.get("error")
        if isinstance(error, dict) or response.status_code >= 400:
            code = error.get("code") if isinstance(error, dict) else None
            try:
                parsed = int(code) if code is not None else None
            except (TypeError, ValueError):
                parsed = None
            message = ""
            if isinstance(error, dict):
                message = str(error.get("message") or "")
            raise DeribitRestError(
                parsed,
                message or response.text[:200],
                status=response.status_code,
                op=method,
            )
        return payload.get("result")

    async def fetch_instruments(
        self, kind: str = KIND_SPOT, *, category: Category | None = None
    ) -> list[ListedInstrument]:
        rows = await self._get(
            ch.PUBLIC_GET_INSTRUMENTS, {"currency": "any", "kind": kind}
        )
        resolved = category
        if resolved is None:
            resolved = (
                Category.SPOT if kind == KIND_SPOT else Category.PERP
            )
        instruments: list[ListedInstrument] = []
        seen: set[str] = set()
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            listed = to_listed(row, category=resolved)
            if listed is None or not listed.is_active:
                continue
            if listed.exch_ticker in seen:
                continue
            seen.add(listed.exch_ticker)
            instruments.append(listed)
        return instruments

    async def fetch_ticker_row(self, instrument: str) -> DeribitTicker:
        data = await self._get(ch.PUBLIC_TICKER, {"instrument_name": instrument})
        if not isinstance(data, dict):
            raise DeribitRestError(
                None, f"no ticker for {instrument}", op=ch.PUBLIC_TICKER
            )
        return DeribitTicker.model_validate(data)

    async def fetch_ticker(
        self, instrument: str, *, ticker: UniversalTicker
    ) -> Ticker:
        return (await self.fetch_ticker_row(instrument)).to_ticker(ticker)

    async def fetch_order_book(
        self,
        instrument: str,
        *,
        ticker: UniversalTicker,
        depth: int = 20,
    ) -> OrderBook:
        data = await self._get(
            ch.PUBLIC_GET_ORDER_BOOK,
            {"instrument_name": instrument, "depth": depth},
        )
        if not isinstance(data, dict):
            raise DeribitRestError(
                None, f"no book for {instrument}", op=ch.PUBLIC_GET_ORDER_BOOK
            )
        return order_book_from_result(data, ticker)

    async def fetch_klines(
        self,
        instrument: str,
        resolution: str,
        *,
        ticker: UniversalTicker,
        interval: str,
        limit: int = 100,
        end_ms: int | None = None,
    ) -> list[Kline]:
        """Recent candles, oldest first.

        ``resolution`` is Deribit's own spelling. The caller maps the
        platform interval and stamps it back on the rows; ``interval``
        is what sizes the window, since Deribit wants both timestamps
        and answers with every candle between them.
        """
        import time

        end = end_ms if end_ms is not None else int(time.time() * 1000)
        # One extra window of slack for a partial candle at either end;
        # we trim to ``limit`` after the zip.
        count = min(max(limit, 1), MAX_KLINES) + 1
        start = end - count * interval_seconds(interval) * 1000
        data = await self._get(
            ch.PUBLIC_GET_TRADINGVIEW,
            {
                "instrument_name": instrument,
                "start_timestamp": start,
                "end_timestamp": end,
                "resolution": resolution,
            },
        )
        if not isinstance(data, dict) or str(data.get("status") or "") == "no_data":
            return []
        rows = kline_from_chart(
            ticker=ticker,
            interval=interval,
            ticks=list(data.get("ticks") or []),
            opens=list(data.get("open") or []),
            highs=list(data.get("high") or []),
            lows=list(data.get("low") or []),
            closes=list(data.get("close") or []),
            volumes=list(data.get("volume") or []),
        )
        if len(rows) > limit:
            rows = rows[-limit:]
        return rows

    async def fetch_funding_history(
        self,
        instrument: str,
        *,
        ticker: UniversalTicker,
        limit: int = 100,
        end_ms: int | None = None,
    ) -> list[FundingRate]:
        """Settled rates, oldest first."""
        import time

        end = end_ms if end_ms is not None else int(time.time() * 1000)
        start = end - max(limit, 1) * 8 * 3600 * 1000
        data = await self._get(
            ch.PUBLIC_GET_FUNDING_HISTORY,
            {
                "instrument_name": instrument,
                "start_timestamp": start,
                "end_timestamp": end,
            },
        )
        rows = data if isinstance(data, list) else []
        out: list[FundingRate] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            rate = row.get("interest_8h")
            if rate is None:
                rate = row.get("interest_1h")
            if rate is None:
                continue
            ts = float(row.get("timestamp") or 0)
            if ts > 1e12:
                ts = ts / 1000.0
            out.append(
                FundingRate(
                    universal_ticker=str(ticker),
                    rate=Decimal(str(rate)),
                    ts=ts,
                )
            )
        out.sort(key=lambda item: item.ts)
        if len(out) > limit:
            out = out[-limit:]
        return out

    async def fetch_open_interest(
        self, instrument: str, *, ticker: UniversalTicker
    ) -> OpenInterest:
        row = await self.fetch_ticker_row(instrument)
        interest = row.to_open_interest(ticker)
        if interest is None:
            raise DeribitRestError(
                None,
                f"no open interest for {instrument}",
                op=ch.PUBLIC_TICKER,
            )
        return interest

    async def server_time(self) -> float:
        """``public/get_time`` — the venue clock, in seconds."""
        result = await self._get(ch.PUBLIC_GET_TIME)
        return float(result) / 1000.0


async def keepalive(client: httpx.AsyncClient) -> None:
    """One public server-time read. No order, no cancel, no key."""
    await DeribitPublicRest(client=client).server_time()


__all__ = [
    "KEEPALIVE_EXPIRY_S",
    "KEEPALIVE_INTERVAL_S",
    "KEEPALIVE_PATH",
    "MAX_KLINES",
    "POOL_LIMITS",
    "DeribitPublicRest",
    "keepalive",
]
