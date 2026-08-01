"""MD session listing HTTP facade."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from mft.protocol import (
    MD_SESSION_LIST,
    ListSessionsRequest,
    ListSessionsRequestEnvelope,
    ListSessionsResult,
    Topics,
)

from mft_api.broker_rpc import DomainRpcError, request_domain
from mft_api.deps import BrokerDep
from mft_api.schemas import SessionListResponse, SessionOut

router = APIRouter(prefix="/md", tags=["md"])


@router.get("/sessions", response_model=SessionListResponse)
async def list_sessions(
    broker: BrokerDep, status: str | None = "live"
) -> SessionListResponse:
    try:
        result = await request_domain(
            broker,
            Topics.MD,
            ListSessionsRequestEnvelope.wrap(
                ListSessionsRequest(domain="md", status=status),
                type=MD_SESSION_LIST,
                source="api",
            ),
            result_type=ListSessionsResult,
        )
    except DomainRpcError as exc:
        raise HTTPException(status_code=502, detail=exc.message) from exc
    return SessionListResponse(
        sessions=[SessionOut.model_validate(s.model_dump()) for s in result.sessions]
    )
