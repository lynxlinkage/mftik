"""Gate USDT-perpetual REST — the reads the WebSocket cannot serve on demand.

Public: contracts, tickers, book, candles. Private: account, positions,
leverage, history. Order entry stays on the socket.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import httpx

from mftik.exchange.errors import ExchangeError
from mftik.exchange.gate.future.models import (
    GateFuturesBalance,
    GateFuturesOrder,
    GateFuturesOrderBook,
    GateFuturesPosition,
    GateFuturesTicker,
    GateFuturesUserTrade,
    contracts_to_base,
)
from mftik.exchange.gate.future.protocol import (
    API_PREFIX,
    GATE_FUTURES_REST_URL,
    SETTLE,
    GateRestError,
    sign_rest,
)
from mftik.exchange.models import (
    Balance,
    BookLevel,
    FundingRate,
    Kline,
    OpenInterest,
    OrderBook,
    Ticker,
)
from mftik.exchange.tickers import UniversalTicker

logger = logging.getLogger(__name__)

MAX_CANDLES = 2000
MAX_HISTORY = 1000
FUTURES_PREFIX = f"/futures/{SETTLE}"


class _Transport:
    def __init__(
        self,
        *,
        base_url: str = GATE_FUTURES_REST_URL,
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


class GateFuturesRest(_Transport):
    """Signed GETs for recon and history."""

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        base_url: str = GATE_FUTURES_REST_URL,
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

    async def fetch_balances(self) -> list[Balance]:
        row = await self._get(f"{FUTURES_PREFIX}/accounts")
        rows = row if isinstance(row, list) else [row] if row else []
        return [GateFuturesBalance.model_validate(item).to_balance() for item in rows]

    async def fetch_positions(self) -> list[GateFuturesPosition]:
        rows = await self._get(f"{FUTURES_PREFIX}/positions")
        return [GateFuturesPosition.model_validate(row) for row in rows or []]

    async def fetch_leverage(self, contract: str) -> Decimal:
        """Configured leverage, including when the account is still flat.

        Isolated reports ``leverage``; cross reports ``0`` there and the
        number on ``cross_leverage_limit``.
        """
        row = await self._get(f"{FUTURES_PREFIX}/get_leverage/{contract}")
        if not isinstance(row, dict):
            raise ExchangeError(
                f"GateFutures leverage for {contract} was not an object"
            )
        isolated = _dec(row.get("leverage"))
        cross = _dec(row.get("cross_leverage_limit"))
        if isolated > 0:
            return isolated
        if cross > 0:
            return cross
        raise ExchangeError(f"GateFutures leverage for {contract} has no value")

    async def fetch_open_orders(
        self, contract: str | None = None
    ) -> list[GateFuturesOrder]:
        params: dict[str, Any] = {"status": "open"}
        if contract:
            params["contract"] = contract
        rows = await self._get(f"{FUTURES_PREFIX}/orders", params)
        return [GateFuturesOrder.model_validate(row) for row in rows or []]

    async def fetch_order(
        self, order_id: str, *, contract: str | None = None
    ) -> GateFuturesOrder:
        params: dict[str, Any] = {}
        if contract:
            params["contract"] = contract
        row = await self._get(f"{FUTURES_PREFIX}/orders/{order_id}", params)
        return GateFuturesOrder.model_validate(row)

    async def fetch_my_trades(
        self,
        contract: str,
        *,
        offset: int = 0,
        limit: int = MAX_HISTORY,
        since: int | None = None,
    ) -> list[GateFuturesUserTrade]:
        """``GET /futures/usdt/my_trades``. ``since`` is seconds."""
        params: dict[str, Any] = {
            "contract": contract,
            "limit": min(limit, MAX_HISTORY),
            "offset": max(0, offset),
        }
        if since is not None:
            params["from"] = since
        rows = await self._get(f"{FUTURES_PREFIX}/my_trades", params)
        return [GateFuturesUserTrade.model_validate(row) for row in rows or []]

    async def fetch_orders(
        self,
        contract: str,
        *,
        offset: int = 0,
        limit: int = MAX_HISTORY,
        since: int | None = None,
        status: str = "finished",
    ) -> list[GateFuturesOrder]:
        """``GET /futures/usdt/orders``. ``since`` is seconds."""
        params: dict[str, Any] = {
            "contract": contract,
            "status": status,
            "limit": min(limit, MAX_HISTORY),
            "offset": max(0, offset),
        }
        if since is not None:
            params["from"] = since
        rows = await self._get(f"{FUTURES_PREFIX}/orders", params)
        return [GateFuturesOrder.model_validate(row) for row in rows or []]


class GateFuturesPublicRest(_Transport):
    """Unsigned GETs for on-demand market-data snapshots."""

    async def fetch_contracts(self) -> list[dict[str, Any]]:
        rows = await self._get(f"{FUTURES_PREFIX}/contracts")
        return list(rows or [])

    async def fetch_ticker_row(self, contract: str) -> GateFuturesTicker:
        """``tickers`` — the venue's row, sizes and all.

        The shared :class:`~mftik.exchange.models.Ticker` drops
        ``total_size``. A caller that needs the contract figure asks
        here rather than reconstructing it.
        """
        rows = await self._get(f"{FUTURES_PREFIX}/tickers", {"contract": contract})
        if not rows:
            raise GateRestError(200, "not_found", f"no ticker for {contract}")
        return GateFuturesTicker.model_validate(rows[0])

    async def fetch_ticker(
        self, contract: str, *, ticker: UniversalTicker
    ) -> Ticker:
        return (await self.fetch_ticker_row(contract)).to_ticker(ticker)

    async def fetch_open_interest(
        self,
        contract: str,
        *,
        ticker: UniversalTicker,
        contract_size: Decimal,
    ) -> OpenInterest:
        """``tickers`` — current size, one side, converted to base.

        ``total_size`` is both sides; the converter halves it.
        ``contract_stats`` is a history series and is not this read.
        """
        row = await self.fetch_ticker_row(contract)
        interest = row.to_open_interest(ticker, contract_size=contract_size)
        if interest is None:
            raise GateRestError(
                200, "not_found", f"no total_size for {contract}"
            )
        return interest

    async def fetch_funding_history(
        self, contract: str, *, ticker: UniversalTicker, limit: int = 100
    ) -> list[FundingRate]:
        """``GET /futures/{settle}/funding_rate`` — settled rates, oldest first.

        Gate answers newest first and stamps ``t`` in unix seconds. Reverse
        here; do not divide ``t`` by 1000.
        """
        rows = await self._get(
            f"{FUTURES_PREFIX}/funding_rate",
            {"contract": contract, "limit": min(limit, MAX_HISTORY)},
        )
        return [
            FundingRate(
                universal_ticker=str(ticker),
                rate=Decimal(str(row.get("r") or "0")),
                ts=float(row.get("t") or 0),
            )
            for row in reversed(rows or [])
        ]

    async def fetch_order_book(
        self,
        contract: str,
        *,
        ticker: UniversalTicker,
        contract_size: Decimal,
        depth: int = 10,
    ) -> OrderBook:
        row = await self._get(
            f"{FUTURES_PREFIX}/order_book",
            {"contract": contract, "limit": depth},
        )
        if isinstance(row, dict) and ("bids" in row or "asks" in row):
            return GateFuturesOrderBook.model_validate(
                {**row, "s": contract}
            ).to_order_book(ticker, contract_size)
        levels = row if isinstance(row, dict) else {}
        return OrderBook(
            universal_ticker=str(ticker),
            bids=_book_levels(levels.get("bids"), contract_size),
            asks=_book_levels(levels.get("asks"), contract_size),
            ts=_rest_ts(row),
        )

    async def fetch_klines(
        self,
        contract: str,
        interval: str,
        *,
        ticker: UniversalTicker,
        contract_size: Decimal,
        limit: int = 100,
    ) -> list[Kline]:
        rows = await self._get(
            f"{FUTURES_PREFIX}/candlesticks",
            {"contract": contract, "interval": interval, "limit": limit},
        )
        return [
            _to_kline(row, ticker, interval, contract_size) for row in rows or []
        ]


def _dec(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def _book_levels(rows: list[Any] | None, contract_size: Decimal) -> list[BookLevel]:
    return [
        BookLevel(
            price=Decimal(str(row[0])),
            qty=contracts_to_base(Decimal(str(row[1])), contract_size),
        )
        for row in rows or []
        if len(row) >= 2
    ]


def _rest_ts(row: Any) -> float:
    if not isinstance(row, dict):
        return 0.0
    raw = row.get("current") or row.get("update") or row.get("t") or 0
    number = float(raw or 0)
    return number / 1000.0 if number > 1e12 else number


def _to_kline(
    row: Any,
    ticker: UniversalTicker,
    interval: str,
    contract_size: Decimal,
) -> Kline:
    if isinstance(row, dict):
        volume = _dec(row.get("v"))
        return Kline(
            universal_ticker=str(ticker),
            interval=interval,
            open_time=float(row.get("t") or 0),
            open=_dec(row.get("o")),
            high=_dec(row.get("h")),
            low=_dec(row.get("l")),
            close=_dec(row.get("c")),
            volume=contracts_to_base(volume, contract_size),
            quote_volume=_dec(row.get("sum")),
            closed=True,
        )
    if not isinstance(row, list) or len(row) < 6:
        raise GateRestError(
            200,
            "bad_response",
            f"candlestick row for {ticker} {interval} is unusable: {row!r}",
        )
    return Kline(
        universal_ticker=str(ticker),
        interval=interval,
        open_time=float(row[0]),
        open=_dec(row[5] if len(row) > 5 else row[1]),
        high=_dec(row[3]),
        low=_dec(row[4]),
        close=_dec(row[2]),
        volume=contracts_to_base(_dec(row[1]), contract_size),
        quote_volume=_dec(row[6]) if len(row) > 6 else Decimal("0"),
        closed=True,
    )


__all__ = [
    "FUTURES_PREFIX",
    "GATE_FUTURES_REST_URL",
    "MAX_HISTORY",
    "GateFuturesPublicRest",
    "GateFuturesRest",
    "GateRestError",
]
