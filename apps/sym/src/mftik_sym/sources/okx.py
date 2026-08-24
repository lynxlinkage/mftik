"""OKX instrument source — ``GET /api/v5/public/instruments``.

Public endpoint, no signing.

**One source per book, not per venue.** OKX is a unified account: one
credential (plus a passphrase) trades spot and USDT-margined swaps, but the
listing endpoint still answers one ``instType`` at a time, and that is also
the unit :meth:`~mftik_sym.plane.SymbolPlane.refresh` can safely delist
within — a spot refresh must not deactivate perp rows just because they were
absent from a spot response. So ``Okx`` contributes two sources, ``Spot``
and ``Perp``, and they differ only in the category they carry.

Two things about OKX's payload shape drive the code below.

**The SWAP book is not only linear perpetuals.** ``instType=SWAP`` lists
inverse coin-margined contracts beside them (``BTC-USD-SWAP`` next to
``BTC-USDT-SWAP``). Inverse settles in the base coin and would still
canonicalize to a ``BTC*`` symbol; storing one as a Perp would hand TD an
``exch_ticker`` it cannot trade on a UTA linear book. So the Perp source
keeps only ``ctType=linear``. Dated expiries live on ``instType=FUTURES``
and never arrive here.

**SWAP sizes are contracts.** ``lotSz`` / ``minSz`` are contract counts;
``ctVal`` (times ``ctMult``) is how much base one contract is. Filters are
stored in **base** so STS and the ledger never have to know about that
multiplier — the same conversion Gate futures does with its quanto. Spot
sizes are already base and need none of this.

The endpoint is not paginated: one ``instType`` is one response.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import httpx
from mftik.exchange import venues
from mftik.exchange.okx import channels as ch
from mftik.exchange.okx.protocol import OKX_REST_URL, product_of
from mftik.exchange.tickers import Category
from mftik_db.models.symbol import FilterName

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)

VENUE = venues.OKX.name

#: The only ``state`` that means the instrument can be traded right now.
#: OKX also lists ``suspend``, ``preopen`` and ``test`` here.
LIVE = "live"

#: Linear USDT (or USDC) margined swaps. Inverse coin-m is still ``SWAP``
#: but a different ``ctType``, and is not published as a Perp.
LINEAR = "linear"


class OkxInstrumentSource:
    """Every instrument OKX lists on one of its books.

    ``category`` picks the book in the platform's vocabulary and is what the
    stored tickers carry; the OKX ``instType`` it maps to comes from
    :func:`~mftik.exchange.okx.protocol.product_of`.
    """

    venue = VENUE

    def __init__(
        self,
        *,
        category: Category = Category.SPOT,
        base_url: str = OKX_REST_URL,
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.category = category
        self.product = product_of(category)
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client = client
        self._owns_client = client is None

    async def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url, timeout=self.timeout
            )
            self._owns_client = True
        return self._client

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    async def fetch(self) -> list[Instrument]:
        client = await self._http()
        response = await client.get(
            ch.MARKET_INSTRUMENTS, params={"instType": self.product}
        )
        response.raise_for_status()
        payload = response.json() or {}
        code = str(payload.get("code") or "0")
        if code != "0":
            raise RuntimeError(
                f"OKX instruments {code}: {payload.get('msg') or 'refused'}"
            )
        out: list[Instrument] = []
        for row in payload.get("data") or []:
            instrument = self._to_instrument(row)
            if instrument is not None:
                out.append(instrument)
        logger.info(
            "%s instruments category=%s fetched=%s",
            VENUE,
            self.product,
            len(out),
        )
        return out

    def _to_instrument(self, row: dict[str, Any]) -> Instrument | None:
        if not self._is_ours(row):
            return None
        # Spot publishes baseCcy/quoteCcy. SWAP leaves those empty and puts
        # the underlier in ctValCcy / settleCcy.
        base = str(row.get("baseCcy") or row.get("ctValCcy") or "").upper()
        quote = str(row.get("quoteCcy") or row.get("settleCcy") or "").upper()
        exch_ticker = str(row.get("instId") or "")
        if not base or not quote or not exch_ticker:
            logger.warning("%s skipping malformed instrument: %r", VENUE, row)
            return None

        contract_size = self._contract_size(row, exch_ticker)
        if self.category is Category.PERP and contract_size is None:
            return None

        lot = _dec(row.get("lotSz"))
        minimum = _dec(row.get("minSz"))
        maximum = _dec(row.get("maxLmtSz"))
        scale = contract_size if contract_size is not None else Decimal("1")

        filters: dict[str, Decimal | None] = {
            FilterName.PRICE_TICK.value: _dec(row.get("tickSz")),
            FilterName.QTY_STEP.value: _times(lot, scale),
            FilterName.MIN_QTY.value: _times(minimum, scale),
            FilterName.MAX_QTY.value: _times(maximum, scale),
            # OKX does not publish a notional floor on this endpoint.
            FilterName.MIN_NOTIONAL.value: None,
            FilterName.MAX_NOTIONAL.value: None,
            FilterName.MIN_PRICE.value: None,
            FilterName.MAX_PRICE.value: None,
        }

        settle = str(row.get("settleCcy") or "").upper()
        return Instrument(
            venue=self.venue,
            base=base,
            quote=quote,
            exch_ticker=exch_ticker,
            category=self.category,
            contract_size=contract_size,
            # Spot settles in its quote currency; repeating that says nothing.
            settlement_asset=settle or None,
            is_active=str(row.get("state") or "") == LIVE,
            filters=filters,
        )

    def _is_ours(self, row: dict[str, Any]) -> bool:
        """Whether this row belongs to the category this source publishes.

        Only the Perp source has anything to decide: SWAP lists inverse
        contracts beside the linear ones, and one stored as a ``Perp`` would
        hand TD an ``exch_ticker`` the UTA linear book does not trade.
        """
        if self.category is not Category.PERP:
            return True
        if str(row.get("ctType") or "") != LINEAR:
            return False
        # A SWAP with an expiry is not a perpetual. OKX puts dated contracts
        # on FUTURES, but refuse rather than publish one if the wire changes.
        exp = row.get("expTime")
        return exp in (None, "", "0", 0)

    def _contract_size(
        self, row: dict[str, Any], exch_ticker: str
    ) -> Decimal | None:
        if self.category is not Category.PERP:
            return None
        value = _dec(row.get("ctVal"))
        if value is None:
            logger.warning("%s skipping %s: no ctVal", VENUE, exch_ticker)
            return None
        mult = _dec(row.get("ctMult")) or Decimal("1")
        return value * mult


def _dec(value: Any) -> Decimal | None:
    """A published bound, or ``None`` where OKX enforces none.

    ``"0"`` and the empty string both mean the filter is present but
    unbounded. Trailing zeros are stripped: on a ``Decimal`` they are the
    scale, and a size floored against ``0.1000`` comes out written to four
    decimals where the venue's tick is one.
    """
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    if parsed <= 0:
        return None
    stripped = parsed.normalize()
    if stripped.as_tuple().exponent > 0:
        return stripped.quantize(Decimal(1))
    return stripped


def _times(size: Decimal | None, scale: Decimal) -> Decimal | None:
    if size is None:
        return None
    return size * scale


__all__ = [
    "LINEAR",
    "LIVE",
    "VENUE",
    "OkxInstrumentSource",
]
