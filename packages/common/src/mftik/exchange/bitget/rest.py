"""Bitget UTA REST.

Public GETs stay unsigned. Signed calls use HMAC + passphrase (BG-2 / V10).
The query string is built here and put on the path so httpx cannot
reserialise a different order than the signature covered.

Balances come from ``GET /api/v3/account/assets`` only (V9 / I7).
``funding-assets`` is not a method.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx

from mftik.exchange.bitget import channels as ch
from mftik.exchange.bitget.listing import to_listed
from mftik.exchange.bitget.models import (
    BitgetAsset,
    BitgetOrderAck,
    BitgetOrderUpdate,
    BitgetPosition,
    BitgetSettings,
    BitgetTicker,
    category_of,
    kline_from_row,
    order_book_from_result,
)
from mftik.exchange.bitget.protocol import (
    BITGET_REST_URL,
    RET_OK,
    SPOT,
    BitgetAuthError,
    BitgetRestError,
    json_body,
    query_string,
    rest_headers,
)
from mftik.exchange.models import (
    Balance,
    FundingRate,
    Kline,
    OpenInterest,
    OrderBook,
    Ticker,
)
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.symbols.listed import ListedInstrument

MAX_KLINES = 200
MAX_HISTORY = 100


class _BitgetRestTransport:
    """httpx lifecycle and envelope decoding, shared by the signed/public pair."""

    def __init__(
        self,
        *,
        base_url: str = BITGET_REST_URL,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
        demo: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.demo = demo
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

    def _headers(self, method: str, request_path: str, body: str) -> dict[str, str]:
        headers = {"Accept": "application/json", "locale": "en-US"}
        return headers

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        await self.connect()
        assert self._client is not None
        query = query_string(params)
        request_path = f"{path}?{query}" if query else path
        response = await self._client.get(
            request_path, headers=self._headers("GET", request_path, "")
        )
        return self._parse(response, path)

    async def _post(self, path: str, args: dict[str, Any] | None = None) -> Any:
        await self.connect()
        assert self._client is not None
        body = json_body(args or {})
        headers = self._headers("POST", path, body)
        headers["Content-Type"] = "application/json"
        response = await self._client.post(path, content=body, headers=headers)
        return self._parse(response, path)

    def _parse(self, response: httpx.Response, path: str) -> Any:
        try:
            payload = response.json()
        except ValueError:
            raise BitgetRestError(
                None,
                response.text[:200],
                status=response.status_code,
                op=path,
            ) from None
        if not isinstance(payload, dict):
            raise BitgetRestError(
                None,
                f"unexpected body {payload!r}",
                status=response.status_code,
                op=path,
            )
        raw_code = payload.get("code")
        parsed = _as_int(raw_code)
        if response.status_code >= 400 or (
            raw_code not in (None, "") and str(raw_code) != RET_OK
        ):
            raise BitgetRestError(
                parsed,
                str(payload.get("msg") or response.text[:200]),
                status=response.status_code,
                op=path,
            )
        data = payload.get("data")
        return data if data is not None else []


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _rows(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


class BitgetPublicRest(_BitgetRestTransport):
    """Unsigned reads — the market-data snapshots MD asks for on demand.

    ``api_secret`` stays ``None`` so a test can assert this client never
    signed anything.
    """

    def __init__(
        self,
        *,
        base_url: str = BITGET_REST_URL,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
        demo: bool = False,
    ) -> None:
        super().__init__(
            base_url=base_url, timeout=timeout, client=client, demo=demo
        )
        self.api_secret = None

    async def fetch_instruments(
        self, product: str = SPOT
    ) -> list[ListedInstrument]:
        rows = await self._get(ch.MARKET_INSTRUMENTS, {"category": product})
        category = category_of(product, Category.SPOT)
        instruments: list[ListedInstrument] = []
        for row in _rows(rows):
            if not isinstance(row, dict):
                continue
            listed = to_listed(row, category=category)
            if listed is not None and listed.is_active:
                instruments.append(listed)
        return instruments

    async def fetch_ticker_row(
        self, product: str, symbol: str
    ) -> BitgetTicker:
        rows = await self._get(
            ch.MARKET_TICKERS, {"category": product, "symbol": symbol}
        )
        for row in _rows(rows):
            if isinstance(row, dict) and row.get("symbol") == symbol:
                return BitgetTicker.model_validate({**row, "category": product})
        if _rows(rows) and isinstance(_rows(rows)[0], dict):
            return BitgetTicker.model_validate(
                {**_rows(rows)[0], "category": product}
            )
        raise BitgetRestError(
            None, f"no ticker for {symbol}", op=ch.MARKET_TICKERS
        )

    async def fetch_ticker(
        self, product: str, symbol: str, *, ticker: UniversalTicker
    ) -> Ticker:
        return (await self.fetch_ticker_row(product, symbol)).to_ticker(ticker)

    async def fetch_order_book(
        self,
        product: str,
        symbol: str,
        *,
        ticker: UniversalTicker,
        depth: int = 100,
    ) -> OrderBook:
        data = await self._get(
            ch.MARKET_ORDERBOOK,
            {"category": product, "symbol": symbol, "limit": str(depth)},
        )
        if not isinstance(data, dict):
            raise BitgetRestError(
                None, f"no book for {symbol}", op=ch.MARKET_ORDERBOOK
            )
        return order_book_from_result(data, ticker)

    async def fetch_klines(
        self,
        product: str,
        symbol: str,
        interval: str,
        *,
        ticker: UniversalTicker,
        limit: int = 100,
    ) -> list[Kline]:
        """Recent candles, **reversed to oldest first**.

        Bitget answers newest first. Reversing at the boundary means no
        caller has to know that. ``interval`` is Bitget's own spelling.
        """
        rows = await self._get(
            ch.MARKET_CANDLES,
            {
                "category": product,
                "symbol": symbol,
                "interval": interval,
                "limit": min(limit, MAX_KLINES),
            },
        )
        return [
            kline_from_row(row, ticker, interval)
            for row in reversed(rows or [])
            if isinstance(row, list)
        ]

    async def fetch_funding_history(
        self,
        product: str,
        symbol: str,
        *,
        ticker: UniversalTicker,
        limit: int = 100,
    ) -> list[FundingRate]:
        """Settled rates, **reversed to oldest first**.

        Bitget wraps the rows in ``resultList``.
        """
        data = await self._get(
            ch.MARKET_FUNDING_HISTORY,
            {
                "category": product,
                "symbol": symbol,
                "limit": min(limit, MAX_HISTORY),
            },
        )
        wrapped = data if isinstance(data, dict) else {}
        rows = wrapped.get("resultList") or []
        out: list[FundingRate] = []
        for row in reversed(rows if isinstance(rows, list) else []):
            if not isinstance(row, dict):
                continue
            out.append(
                FundingRate(
                    universal_ticker=str(ticker),
                    rate=Decimal(str(row.get("fundingRate") or "0")),
                    ts=float(row.get("fundingRateTimestamp") or 0) / 1000.0,
                )
            )
        return out

    async def fetch_open_interest(
        self,
        product: str,
        symbol: str,
        *,
        ticker: UniversalTicker,
    ) -> OpenInterest:
        data = await self._get(
            ch.MARKET_OPEN_INTEREST,
            {"category": product, "symbol": symbol},
        )
        wrapped = data if isinstance(data, dict) else {}
        rows = wrapped.get("list") or []
        chosen: dict[str, Any] | None = None
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict) and row.get("symbol") == symbol:
                    chosen = row
                    break
            if chosen is None and rows and isinstance(rows[0], dict):
                chosen = rows[0]
        if chosen is None:
            raise BitgetRestError(
                None,
                f"no open interest for {symbol}",
                op=ch.MARKET_OPEN_INTEREST,
            )
        qty = Decimal(str(chosen.get("openInterest") or "0"))
        ts = float(wrapped.get("ts") or 0) / 1000.0
        return OpenInterest(universal_ticker=str(ticker), qty=qty, ts=ts)


class BitgetRest(_BitgetRestTransport):
    """Signed UTA REST. Settings and assets are the only account reads."""

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        passphrase: str,
        base_url: str = BITGET_REST_URL,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
        demo: bool = False,
    ) -> None:
        if not passphrase:
            raise BitgetAuthError("Bitget passphrase is required")
        super().__init__(
            base_url=base_url, timeout=timeout, client=client, demo=demo
        )
        self.api_key = api_key
        self._api_secret = api_secret
        self.passphrase = passphrase

    def _headers(self, method: str, request_path: str, body: str) -> dict[str, str]:
        return rest_headers(
            api_key=self.api_key,
            api_secret=self._api_secret,
            passphrase=self.passphrase,
            method=method,
            request_path=request_path,
            body=body,
            demo=self.demo,
        )

    def _ack(self, data: Any, *, op: str) -> BitgetOrderAck:
        rows = _rows(data)
        if not rows or not isinstance(rows[0], dict):
            raise BitgetRestError(None, "empty order reply", op=op)
        return BitgetOrderAck.model_validate(rows[0])

    async def fetch_settings(self) -> BitgetSettings:
        data = await self._get(ch.ACCOUNT_SETTINGS)
        if isinstance(data, dict):
            row = data
        else:
            rows = _rows(data)
            row = rows[0] if rows else {}
        if not isinstance(row, dict):
            raise BitgetRestError(
                None, "empty settings reply", op=ch.ACCOUNT_SETTINGS
            )
        return BitgetSettings.model_validate(row)

    async def fetch_balances(self, *, asset: str | None = None) -> list[Balance]:
        params: dict[str, Any] = {}
        if asset:
            params["asset"] = asset
        rows = await self._get(ch.ACCOUNT_ASSETS, params or None)
        balances: list[Balance] = []
        for row in _rows(rows):
            if not isinstance(row, dict):
                continue
            balance = BitgetAsset.model_validate(row).to_balance()
            if balance is not None:
                balances.append(balance)
        return balances

    async def place_order(self, args: dict[str, Any]) -> BitgetOrderAck:
        return self._ack(await self._post(ch.ORDER_PLACE, args), op=ch.ORDER_PLACE)

    async def cancel_order(
        self,
        *,
        category: str,
        symbol: str,
        order_id: str | None = None,
        client_oid: str | None = None,
    ) -> BitgetOrderAck:
        if not order_id and not client_oid:
            raise BitgetRestError(
                None,
                "Bitget cancel needs orderId or clientOid, got neither",
                op=ch.ORDER_CANCEL,
            )
        args: dict[str, Any] = {"category": category, "symbol": symbol}
        if order_id:
            args["orderId"] = order_id
        if client_oid:
            args["clientOid"] = client_oid
        return self._ack(
            await self._post(ch.ORDER_CANCEL, args), op=ch.ORDER_CANCEL
        )

    async def fetch_open_orders(
        self, product: str, symbol: str | None = None
    ) -> list[BitgetOrderUpdate]:
        params: dict[str, Any] = {"category": product}
        if symbol:
            params["symbol"] = symbol
        rows = await self._get(ch.ORDERS_UNFILLED, params)
        return [
            BitgetOrderUpdate.model_validate(row)
            for row in _rows(rows)
            if isinstance(row, dict)
        ]

    async def fetch_order(
        self,
        *,
        category: str,
        order_id: str | None = None,
        client_oid: str | None = None,
    ) -> BitgetOrderUpdate | None:
        params: dict[str, Any] = {"category": category}
        if order_id:
            params["orderId"] = order_id
        if client_oid:
            params["clientOid"] = client_oid
        try:
            data = await self._get(ch.ORDER_INFO, params)
        except BitgetRestError as exc:
            if exc.not_found:
                return None
            raise
        rows = _rows(data)
        if rows and isinstance(rows[0], dict):
            return BitgetOrderUpdate.model_validate(rows[0])
        return None

    async def fetch_position_rows(
        self, product: str, symbol: str | None = None
    ) -> list[BitgetPosition]:
        if product == SPOT:
            return []
        params: dict[str, Any] = {"category": product}
        if symbol:
            params["symbol"] = symbol
        rows = await self._get(ch.POSITION_CURRENT, params)
        return [
            row
            for row in (
                BitgetPosition.model_validate(raw)
                for raw in _rows(rows)
                if isinstance(raw, dict)
            )
            if row.signed_size != 0
        ]


__all__ = [
    "MAX_HISTORY",
    "MAX_KLINES",
    "BitgetPublicRest",
    "BitgetRest",
]
