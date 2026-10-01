"""MD session listing HTTP facade."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from mftik_api.deps import BrokerDep
from mftik_api.schemas import SessionListResponse

router = APIRouter(prefix="/md", tags=["md"])


@router.get("/sessions", response_model=SessionListResponse)
async def list_sessions(
    broker: BrokerDep, status: str | None = "live"
) -> SessionListResponse:
    """Placeholder until IF-09 (#187) gives MD a desired set to list again.

    RM-05 (#168) deleted MD's session RPCs, so ``md.session.list`` comes back
    as ``unknown_type`` — a 502 that reads as a broken plane rather than as a
    route with nothing behind it.
    """
    del broker, status
    raise HTTPException(
        status_code=501,
        detail="md session listing is not implemented — waiting for IF-09 "
        "(#187)",
    )
