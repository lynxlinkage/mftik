"""TD session listing HTTP facade."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from mft.protocol import (
    TD_SESSION_LIST,
    ListSessionsRequest,
    ListSessionsRequestEnvelope,
    ListSessionsResult,
    Topics,
)
from mft_db.repositories import AccountRepository
from mft_db.session import session_scope

from mft_api.broker_rpc import DomainRpcError, request_domain
from mft_api.deps import BrokerDep
from mft_api.schemas import SessionListResponse, SessionOut

router = APIRouter(prefix="/td", tags=["td"])


@router.get("/sessions", response_model=SessionListResponse)
async def list_sessions(
    broker: BrokerDep, status: str | None = "live"
) -> SessionListResponse:
    try:
        result = await request_domain(
            broker,
            Topics.TD,
            ListSessionsRequestEnvelope.wrap(
                ListSessionsRequest(domain="td", status=status),
                type=TD_SESSION_LIST,
                source="api",
            ),
            result_type=ListSessionsResult,
        )
    except DomainRpcError as exc:
        raise HTTPException(status_code=502, detail=exc.message) from exc

    label_by_api = await _api_labels()
    sessions: list[SessionOut] = []
    for s in result.sessions:
        out = SessionOut.model_validate(s.model_dump())
        if out.api_id is not None:
            label = label_by_api.get(out.api_id)
            if label is not None:
                out = out.model_copy(
                    update={"venue": label[0], "api_name": label[1]}
                )
        sessions.append(out)
    return SessionListResponse(sessions=sessions)


async def _api_labels() -> dict[int, tuple[str, str]]:
    """api_id → (venue, account name)."""
    async with session_scope() as db:
        accounts = await AccountRepository(db).list_with_api()
    out: dict[int, tuple[str, str]] = {}
    for account in accounts:
        api = account.api
        if api is None:
            continue
        out[api.id] = (api.venue, account.name)
    return out
