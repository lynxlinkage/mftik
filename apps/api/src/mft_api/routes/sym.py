"""Symbol plane HTTP facade — venues and instruments the plane tracks."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from mft.protocol import (
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

from mft_api.broker_rpc import DomainRpcError, request_domain
from mft_api.deps import BrokerDep
from mft_api.schemas import SymSymbolListResponse, SymVenueListResponse

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
    venue: str | None = None,
    category: str | None = None,
    symbol: str | None = None,
    active_only: bool = True,
) -> SymSymbolListResponse:
    """Instruments from the golden tables. Omitted filters widen the result."""
    try:
        result = await request_domain(
            broker,
            Topics.SYM,
            SymListRequestEnvelope.wrap(
                SymListRequest(
                    venue=venue,
                    category=category,
                    symbol=symbol,
                    active_only=active_only,
                ),
                type=SYM_LIST,
                source="api",
            ),
            result_type=SymListResult,
            error_types=_ERRORS,
        )
    except DomainRpcError as exc:
        raise HTTPException(status_code=502, detail=exc.message) from exc
    return SymSymbolListResponse(symbols=result.symbols)
