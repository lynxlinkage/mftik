"""Bybit instrument source — ``GET /v5/market/instruments-info``.

Public endpoint, no signing.

**One source per book, not per venue.** Bybit is a unified account: one
credential trades spot and perps, but the listing endpoint still answers one
``category`` at a time, and that is also the unit
:meth:`~mftik_sym.plane.SymbolPlane.refresh` can safely delist within — a spot
refresh must not deactivate perp rows just because they were absent from a spot
response. So ``Bybit`` contributes two sources, ``Spot`` and ``Perp``, and they
differ only in the category they carry.

Two things about Bybit's payload shape drive the code below.

**The linear book is not only perpetuals.** ``category=linear`` lists dated
futures alongside them — 40 of them against 760 perpetuals, at the time of
writing — and a future's base and quote are the perpetual's. ``BTCUSDT-25DEC26``
and ``BTCUSDT`` both canonicalize to ``BTCUSDT``, so storing futures as perps
would not merely mislabel them: eight symbols currently collide, and the upsert
keeps whichever was written last. ``Bybit_Perp_BTCUSDT`` would end up holding a
December 2026 future's ``exch_ticker``, and TD would route every perp order
there. So the Perp source keeps only ``contractType`` ending in ``Perpetual``;
dated futures belong to a ``Future`` source that does not exist yet.

**It is paginated behind a cursor**, with no total. A refresh that read only the
first page would quietly publish a fraction of the venue and then deactivate
everything it did not see — which is worse than not refreshing at all.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import httpx
from mftik.exchange import venues
from mftik.exchange.bybit import channels as ch
from mftik.exchange.bybit.protocol import BYBIT_REST_URL, product_of
from mftik.exchange.tickers import Category
from mftik_db.models.symbol import FilterName

from mftik_sym.sources.base import Instrument

logger = logging.getLogger(__name__)

VENUE = venues.BYBIT.name

#: The only ``status`` that means the instrument can be traded right now.
#: Bybit also lists ``PreLaunch``, ``Delivering`` and ``Closed`` here.
TRADING = "Trading"

#: Most rows one page returns. Bybit caps it here and answers a cursor for the
#: rest.
PAGE_LIMIT = 1000

#: How many pages a single fetch will follow before giving up. Bybit's spot
#: book is one page and its linear book two or three; a cursor that never
#: empties is a bug at one end or the other, and looping forever on it would
#: hang the whole refresh cycle rather than fail one venue.
MAX_PAGES = 50

#: ``contractType`` values that are perpetual. The linear and inverse books
#: also carry dated futures, which are a different category.
PERPETUAL = frozenset({"LinearPerpetual", "InversePerpetual"})


class BybitInstrumentSource:
    """Every instrument Bybit lists on one of its books.

    ``category`` picks the book in the platform's vocabulary and is what the
    stored tickers carry; the Bybit ``category`` parameter it maps to comes
    from :func:`~mftik.exchange.bybit.protocol.product_of`.
    """

    venue = VENUE

    def __init__(
        self,
        *,
        category: Category = Category.SPOT,
        base_url: str = BYBIT_REST_URL,
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
        out: list[Instrument] = []
        cursor = ""
        for _ in range(MAX_PAGES):
            params: dict[str, Any] = {
                "category": self.product,
                "limit": PAGE_LIMIT,
            }
            if cursor:
                params["cursor"] = cursor
            response = await client.get(ch.MARKET_INSTRUMENTS, params=params)
            response.raise_for_status()
            result = (response.json() or {}).get("result") or {}
            for row in result.get("list") or []:
                instrument = self._to_instrument(row)
                if instrument is not None:
                    out.append(instrument)
            cursor = str(result.get("nextPageCursor") or "")
            if not cursor:
                break
        else:
            logger.warning(
                "%s %s listing still had a cursor after %s pages",
                VENUE,
                self.product,
                MAX_PAGES,
            )
        logger.info(
            "%s instruments category=%s fetched=%s", VENUE, self.product, len(out)
        )
        return out

    def _to_instrument(self, row: dict[str, Any]) -> Instrument | None:
        base = str(row.get("baseCoin") or "").upper()
        quote = str(row.get("quoteCoin") or "").upper()
        exch_ticker = str(row.get("symbol") or "")
        if not base or not quote or not exch_ticker:
            logger.warning("%s skipping malformed instrument: %r", VENUE, row)
            return None
        if not self._is_ours(row):
            return None

        lot = row.get("lotSizeFilter") or {}
        price = row.get("priceFilter") or {}

        filters: dict[str, Decimal | None] = {
            FilterName.PRICE_TICK.value: _dec(price.get("tickSize")),
            # Spot spells the quantity step ``basePrecision`` and the contract
            # books spell it ``qtyStep``; they mean the same thing.
            FilterName.QTY_STEP.value: _dec(
                lot.get("basePrecision") or lot.get("qtyStep")
            ),
            FilterName.MIN_QTY.value: _dec(lot.get("minOrderQty")),
            FilterName.MAX_QTY.value: _dec(lot.get("maxOrderQty")),
            # And the notional floor is ``minOrderAmt`` on spot,
            # ``minNotionalValue`` on the contract books.
            FilterName.MIN_NOTIONAL.value: _dec(
                lot.get("minOrderAmt") or lot.get("minNotionalValue")
            ),
            FilterName.MAX_NOTIONAL.value: _dec(lot.get("maxOrderAmt")),
            # Published on the contract books only; the keys stay so a caller
            # can tell "unbounded" from "not published".
            FilterName.MIN_PRICE.value: _dec(price.get("minPrice")),
            FilterName.MAX_PRICE.value: _dec(price.get("maxPrice")),
        }

        return Instrument(
            venue=self.venue,
            base=base,
            quote=quote,
            exch_ticker=exch_ticker,
            category=self.category,
            # Only the contract books settle in something; on spot the quote
            # currency is the settlement and repeating it says nothing.
            settlement_asset=str(row.get("settleCoin") or "") or None,
            is_active=str(row.get("status", "")) == TRADING,
            filters=filters,
        )

    def _is_ours(self, row: dict[str, Any]) -> bool:
        """Whether this row belongs to the category this source publishes.

        Only the Perp source has anything to decide: Bybit's linear book lists
        dated futures beside the perpetuals, and one stored as a ``Perp`` would
        both claim the wrong market and overwrite the perpetual it shares a
        canonical symbol with — see the module docstring.
        """
        if self.category is not Category.PERP:
            return True
        return str(row.get("contractType") or "") in PERPETUAL


def _dec(value: Any) -> Decimal | None:
    """A published bound, or ``None`` where Bybit enforces none.

    Bybit writes ``"0"`` for a filter that is present but unbounded, which is
    not the same as the bound being zero — an order cannot be a multiple of a
    zero step, and a zero minimum is no minimum. Both read as "the venue
    publishes the restriction but sets no limit", which is what a ``None``
    value on a present key means to this plane.

    Trailing zeros are stripped, because on a ``Decimal`` they are the scale
    rather than decoration, and the scale propagates: a size floored against a
    step stored as ``0.00010000`` comes out written to eight decimals, which
    Bybit rejects where the same quantity written to four is taken.
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
    # ``normalize`` renders a whole number exponentially (10 → 1E+1); keep it
    # written the way it was published.
    if stripped.as_tuple().exponent > 0:
        return stripped.quantize(Decimal(1))
    return stripped


__all__ = [
    "MAX_PAGES",
    "PAGE_LIMIT",
    "PERPETUAL",
    "TRADING",
    "VENUE",
    "BybitInstrumentSource",
]
