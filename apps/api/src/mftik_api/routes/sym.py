"""Symbol plane HTTP facade — venues and instruments the plane tracks."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from mftik.exchange import tickers, venues
from mftik.exchange.symbols import normalize_symbol
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    SYM_ERROR,
    SYM_LIST,
    SYM_VENUES,
    SymListRequest,
    SymListRequestEnvelope,
    SymListResult,
    SymVenuesResult,
    Topics,
    UntypedEnvelope,
)

from mftik_api.broker_rpc import DomainRpcError, request_domain
from mftik_api.deps import BrokerDep
from mftik_api.paging import ListOffset
from mftik_api.schemas import SymSymbolListResponse, SymVenueListResponse

router = APIRouter(prefix="/sym", tags=["sym"])

_ERRORS = frozenset({SYM_ERROR})


@router.get("/venues", response_model=SymVenueListResponse)
async def list_venues(broker: BrokerDep) -> SymVenueListResponse:
    """Every venue the plane is configured to track, with instrument counts.

    Unlike ``GET /venues`` (the credential registry) this reflects what the
    plane has actually loaded, so a venue with a count of 0 has not refreshed.
    """
    try:
        result = await request_domain(
            broker,
            Topics.SYM,
            # sym.venues takes no arguments; the envelope only carries the type.
            UntypedEnvelope.wrap({}, type=SYM_VENUES, source="api"),
            result_type=SymVenuesResult,
            error_types=_ERRORS,
        )
    except DomainRpcError as exc:
        raise HTTPException(status_code=502, detail=exc.message) from exc
    return SymVenueListResponse(venues=result.venues, counts=result.counts)


@router.get("/symbols", response_model=SymSymbolListResponse)
async def list_symbols(
    broker: BrokerDep,
    universal_ticker: str | None = None,
    venue: str | None = None,
    category: str | None = None,
    symbol: str | None = None,
    active_only: bool = True,
    q: str | None = None,
    limit: Annotated[int | None, Query(ge=1, le=500)] = None,
    offset: ListOffset = 0,
    slim: bool = False,
) -> SymSymbolListResponse:
    """Instruments from the golden tables. Omitted filters widen the result.

    ``universal_ticker`` names exactly one instrument; the other three are
    parts of it and each widens the result by being left out. All are
    normalized here rather than matched literally, so a query typed
    ``gate/spot`` finds the rows stored as ``Gate_Spot_…``.

    ``symbol`` is matched as an exact suffix, so it has to be normalized
    the way the row was stored — and on a dated or option book that
    spelling depends on the category (``BTCUSDT250926`` and
    ``BTCUSDT-250926`` are both the stored ``BTCUSDT-250926``, but only
    once something says the book is ``Future``). Pass ``category``
    alongside ``symbol`` for those; without one the symbol is folded as a
    pair, which is right for spot and perp and cannot find a dated row.
    ``universal_ticker`` carries its own category and needs no such help.

    ``q`` / ``limit`` / ``offset`` page a browse. ``slim`` returns only the
    filters the table shows; pass ``universal_ticker`` without ``slim`` for
    the full filter set on one row.
    """
    try:
        venue = venues.normalize(venue) if venue else None
        category = tickers.category(category).value if category else None
        symbol = (
            normalize_symbol(symbol, category=category) if symbol else None
        )
        universal_ticker = (
            str(UniversalTicker.resolve(universal_ticker))
            if universal_ticker
            else None
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        result = await request_domain(
            broker,
            Topics.SYM,
            SymListRequestEnvelope.wrap(
                SymListRequest(
                    universal_ticker=universal_ticker,
                    venue=venue,
                    category=category,
                    symbol=symbol,
                    active_only=active_only,
                    q=q,
                    limit=limit,
                    offset=offset,
                    slim=slim,
                ),
                type=SYM_LIST,
                source="api",
            ),
            result_type=SymListResult,
            error_types=_ERRORS,
        )
    except DomainRpcError as exc:
        raise HTTPException(status_code=502, detail=exc.message) from exc
    return SymSymbolListResponse(symbols=result.symbols, total=result.total)
