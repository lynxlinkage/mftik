"""OKX v5 REST — the reads the sockets cannot serve, and order entry.

Split by whether the call is signed:

* :class:`OkxPublicRest` — MD's snapshot reads. Instruments, ticker, order
  book and candle history. No credential: requiring keys for public data
  would mean MD could not run a feed without a trading account.
* :class:`OkxRest` — TD's recon reads and order entry. OKX has no trade
  socket; placing and cancelling an order is a signed HTTP call, and "what
  is open right now" exists only over REST.

**Every successful body is HTTP 200 with ``code: "0"``.** Refusals live in
``code`` / ``msg``, and a per-order refusal on a place/cancel lives in
``sCode`` / ``sMsg`` on the row. The status line is not the check.

**The reads that answer a shared model take a ``ticker``** as well as the
venue symbol: the symbol is what goes on the wire, the ticker is what the
answer is labelled with. Only the symbol plane can map between them.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx

from mftik.exchange.errors import ExchangeError
from mftik.exchange.models import Balance, Instrument, Kline, OrderBook, Ticker
from mftik.exchange.okx import channels as ch
from mftik.exchange.okx.models import (
    OkxAccount,
    OkxFill,
    OkxLeverage,
    OkxOrderAck,
    OkxOrderUpdate,
    OkxPosition,
    OkxTicker,
    instrument_from_row,
    kline_from_row,
    order_book_from_result,
)
from mftik.exchange.okx.protocol import (
    DEMO_HEADER,
    OKX_REST_URL,
    RET_OK,
    SPOT,
    SWAP,
    OkxRestError,
    json_body,
    query_string,
    rest_headers,
)
from mftik.exchange.tickers import UniversalTicker

MAX_KLINES = 300
MAX_HISTORY = 100
LIVE = "live"
LINEAR = "linear"


class _OkxRestTransport:
    """httpx lifecycle and envelope decoding, shared by the signed/public pair."""

    def __init__(
        self,
        *,
        base_url: str = OKX_REST_URL,
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
        headers = {"Accept": "application/json"}
        if self.demo:
            headers[DEMO_HEADER] = "1"
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
            raise OkxRestError(
                None,
                response.text[:200],
                status=response.status_code,
                op=path,
            ) from None
        if not isinstance(payload, dict):
            raise OkxRestError(
                None,
                f"unexpected body {payload!r}",
                status=response.status_code,
                op=path,
            )
        code = payload.get("code")
        parsed = None if code is None or code == "" else int(code)
        if response.status_code >= 400 or (parsed is not None and parsed != RET_OK):
            raise OkxRestError(
                parsed,
                str(payload.get("msg") or response.text[:200]),
                status=response.status_code,
                op=path,
            )
        data = payload.get("data")
        return data if data is not None else []


class OkxPublicRest(_OkxRestTransport):
    """Unsigned reads — the market-data snapshots MD asks for on demand."""

    async def fetch_instruments(
        self, product: str = SPOT, *, inst_id: str | None = None
    ) -> list[Instrument]:
        """``instruments`` — tradeable symbols, left in OKX's own spelling.

        This is what the symbol plane ingests to *build* the canonical
        mapping, so it cannot depend on that mapping existing. SWAP rows that
        are not live linear perpetuals are dropped: dated futures and inverse
        contracts would otherwise collide on the canonical symbol.
        """
        params: dict[str, Any] = {"instType": product}
        if inst_id:
            params["instId"] = inst_id
        rows = await self._get(ch.MARKET_INSTRUMENTS, params)
        instruments: list[Instrument] = []
        for row in rows or []:
            if str(row.get("state") or "") != LIVE:
                continue
            if product == SWAP and str(row.get("ctType") or "") != LINEAR:
                continue
            instruments.append(instrument_from_row(row))
        return instruments

    async def fetch_ticker_row(self, inst_id: str) -> OkxTicker:
        rows = await self._get(ch.MARKET_TICKER, {"instId": inst_id})
        if not rows:
            raise OkxRestError(None, f"no ticker for {inst_id}", op=ch.MARKET_TICKER)
        return OkxTicker.model_validate(rows[0])

    async def fetch_ticker(self, inst_id: str, *, ticker: UniversalTicker) -> Ticker:
        return (await self.fetch_ticker_row(inst_id)).to_ticker(ticker)

    async def fetch_order_book(
        self,
        inst_id: str,
        *,
        ticker: UniversalTicker,
        depth: int = 50,
        contract_size: Decimal | None = None,
    ) -> OrderBook:
        rows = await self._get(
            ch.MARKET_BOOKS, {"instId": inst_id, "sz": str(depth)}
        )
        if not rows:
            raise OkxRestError(None, f"no book for {inst_id}", op=ch.MARKET_BOOKS)
        return order_book_from_result(
            rows[0], ticker, contract_size=contract_size
        )

    async def fetch_klines(
        self,
        inst_id: str,
        bar: str,
        *,
        ticker: UniversalTicker,
        limit: int = 100,
        contract_size: Decimal | None = None,
    ) -> list[Kline]:
        """Recent candles, **reversed to oldest first**.

        OKX answers newest first. Reversing at the boundary means no caller
        has to know that. ``bar`` is OKX's own spelling.
        """
        rows = await self._get(
            ch.MARKET_CANDLES,
            {
                "instId": inst_id,
                "bar": bar,
                "limit": min(limit, MAX_KLINES),
            },
        )
        return [
            kline_from_row(row, ticker, bar, contract_size=contract_size)
            for row in reversed(rows or [])
        ]

    async def server_time(self) -> float:
        rows = await self._get(ch.MARKET_TIME)
        if not rows:
            return 0.0
        return float(rows[0].get("ts") or 0) / 1000.0


class OkxRest(_OkxRestTransport):
    """Signed calls — recon reads, positions, and order entry."""

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        passphrase: str,
        base_url: str = OKX_REST_URL,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
        demo: bool = False,
    ) -> None:
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

    def _ack(self, rows: Any, *, op: str) -> OkxOrderAck:
        if not rows:
            raise OkxRestError(None, "empty order reply", op=op)
        ack = OkxOrderAck.model_validate(rows[0])
        if not ack.ok:
            code = int(ack.s_code) if ack.s_code.isdigit() else None
            raise OkxRestError(code, ack.s_msg or "order refused", op=op)
        return ack

    # --- order entry -------------------------------------------------------

    async def place_order(self, args: dict[str, Any]) -> OkxOrderAck:
        return self._ack(await self._post(ch.ORDER_PLACE, args), op=ch.ORDER_PLACE)

    async def cancel_order(
        self,
        *,
        inst_id: str,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ) -> OkxOrderAck:
        if not ord_id and not cl_ord_id:
            raise OkxRestError(
                None,
                "OKX cancel needs ordId or clOrdId, got neither",
                op=ch.ORDER_CANCEL,
            )
        args: dict[str, Any] = {"instId": inst_id}
        if ord_id:
            args["ordId"] = ord_id
        if cl_ord_id:
            args["clOrdId"] = cl_ord_id
        return self._ack(await self._post(ch.ORDER_CANCEL, args), op=ch.ORDER_CANCEL)

    # --- recon reads -------------------------------------------------------

    async def fetch_open_orders(
        self, product: str, inst_id: str | None = None
    ) -> list[OkxOrderUpdate]:
        params: dict[str, Any] = {"instType": product}
        if inst_id:
            params["instId"] = inst_id
        rows = await self._get(ch.ORDERS_PENDING, params)
        return [OkxOrderUpdate.model_validate(row) for row in rows or []]

    async def fetch_order(
        self,
        *,
        inst_id: str | None = None,
        product: str | None = None,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ) -> OkxOrderUpdate | None:
        """What became of one order, live or finished.

        ``/trade/order`` knows only a live one when given ``instId``; a
        finished order has to be asked of ``/trade/orders-history``. ``None``
        means neither has it.
        """
        params: dict[str, Any] = {}
        if inst_id:
            params["instId"] = inst_id
        if ord_id:
            params["ordId"] = ord_id
        if cl_ord_id:
            params["clOrdId"] = cl_ord_id
        if inst_id:
            try:
                rows = await self._get(ch.ORDER_PLACE, params)
            except OkxRestError as exc:
                if not exc.not_found:
                    raise
                rows = []
            if rows:
                return OkxOrderUpdate.model_validate(rows[0])
        history: dict[str, Any] = dict(params)
        if product:
            history["instType"] = product
        elif inst_id:
            history.setdefault("instType", SPOT if inst_id.count("-") == 1 else SWAP)
        try:
            rows = await self._get(ch.ORDERS_HISTORY, history)
        except OkxRestError as exc:
            if exc.not_found:
                return None
            raise
        if rows:
            return OkxOrderUpdate.model_validate(rows[0])
        return None

    async def fetch_balances(self, *, ccy: str | None = None) -> list[Balance]:
        params: dict[str, Any] = {}
        if ccy:
            params["ccy"] = ccy
        rows = await self._get(ch.ACCOUNT_BALANCE, params)
        balances: list[Balance] = []
        for row in rows or []:
            balances.extend(OkxAccount.model_validate(row).to_balances())
        return balances

    async def fetch_position_rows(
        self, product: str = SWAP, inst_id: str | None = None
    ) -> list[OkxPosition]:
        if product == SPOT:
            return []
        params: dict[str, Any] = {"instType": product}
        if inst_id:
            params["instId"] = inst_id
        rows = await self._get(ch.ACCOUNT_POSITIONS, params)
        return [
            row
            for row in (OkxPosition.model_validate(raw) for raw in rows or [])
            if row.signed_size != 0
        ]

    async def fetch_leverage_row(
        self, inst_id: str, *, mgn_mode: str = "cross"
    ) -> OkxLeverage:
        if not inst_id:
            raise ValueError("instId is required")
        rows = await self._get(
            ch.ACCOUNT_LEVERAGE, {"instId": inst_id, "mgnMode": mgn_mode}
        )
        if not rows:
            raise ExchangeError(
                f"OKX leverage-info returned no row for {inst_id}"
            )
        return OkxLeverage.model_validate(rows[0])

    async def fetch_fills(
        self,
        product: str,
        inst_id: str | None = None,
        *,
        after: str | None = None,
        limit: int = MAX_HISTORY,
    ) -> list[OkxFill]:
        params: dict[str, Any] = {
            "instType": product,
            "limit": min(limit, MAX_HISTORY),
        }
        if inst_id:
            params["instId"] = inst_id
        if after:
            params["after"] = after
        rows = await self._get(ch.FILLS_HISTORY, params)
        return [OkxFill.model_validate(row) for row in rows or []]

    async def fetch_order_history(
        self,
        product: str,
        inst_id: str | None = None,
        *,
        after: str | None = None,
        limit: int = MAX_HISTORY,
    ) -> list[OkxOrderUpdate]:
        params: dict[str, Any] = {
            "instType": product,
            "limit": min(limit, MAX_HISTORY),
        }
        if inst_id:
            params["instId"] = inst_id
        if after:
            params["after"] = after
        rows = await self._get(ch.ORDERS_HISTORY, params)
        return [OkxOrderUpdate.model_validate(row) for row in rows or []]


__all__ = [
    "LINEAR",
    "LIVE",
    "MAX_HISTORY",
    "MAX_KLINES",
    "OkxPublicRest",
    "OkxRest",
]
