"""Gate spot REST v4 — only the reads the WebSocket API cannot serve.

Gate's spot WebSocket API has ``spot.order_status`` for a single order, but no
``spot.order_list`` (futures has one; spot does not) and no balance query. TD's
recon needs both: open orders and balances at attach time. Returning nothing
there would leave the OMS believing the account is flat, which is worse than
not reconciling at all — so those two reads come over REST.

Deliberately not a general REST client. Order entry stays on the WebSocket,
where it belongs latency-wise; this exists for the two recon calls only.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from decimal import Decimal
from typing import Any

import httpx

from mft.exchange.errors import ExchangeError
from mft.exchange.gate.spot.models import GateOrderAck
from mft.exchange.models import Balance, Order

logger = logging.getLogger(__name__)

GATE_SPOT_REST_URL = "https://api.gateio.ws"
API_PREFIX = "/api/v4"


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


class GateSpotRest:
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
        self.api_key = api_key
        self.api_secret = api_secret
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

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        await self.connect()
        assert self._client is not None
        full_path = f"{API_PREFIX}{path}"
        query = "&".join(f"{k}={v}" for k, v in (params or {}).items())
        signature, ts = sign_rest(self.api_secret, "GET", full_path, query)
        response = await self._client.get(
            full_path,
            params=params,
            headers={
                "KEY": self.api_key,
                "Timestamp": ts,
                "SIGN": signature,
                "Accept": "application/json",
            },
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

    async def fetch_open_orders(
        self, currency_pair: str | None = None
    ) -> list[Order]:
        """Open orders, either for one pair or across the account.

        ``GET /spot/orders?status=open`` needs a pair; ``GET /spot/open_orders``
        returns every pair grouped, so the two are served by different
        endpoints and flattened to the same list here.
        """
        if currency_pair:
            rows = await self._get(
                "/spot/orders",
                {"currency_pair": currency_pair, "status": "open"},
            )
            return [_to_order(row) for row in rows or []]

        grouped = await self._get("/spot/open_orders")
        orders: list[Order] = []
        for group in grouped or []:
            for row in group.get("orders", []) or []:
                orders.append(_to_order(row))
        return orders

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

    async def fetch_order(self, order_id: str, *, currency_pair: str) -> Order:
        """``GET /spot/orders/{order_id}`` — used to resolve a single order."""
        row = await self._get(
            f"/spot/orders/{order_id}", {"currency_pair": currency_pair}
        )
        return _to_order(row)


def _dec(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def _to_order(row: dict[str, Any]) -> Order:
    """REST order rows share the trading-call reply shape (``status``-based)."""
    return GateOrderAck.model_validate(row).to_order()


__all__ = [
    "API_PREFIX",
    "GATE_SPOT_REST_URL",
    "GateRestError",
    "GateSpotRest",
    "sign_rest",
]
