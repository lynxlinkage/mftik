"""MD HTTP facade — placeholder until MD sessions exist."""

from __future__ import annotations

from fastapi import APIRouter

from mft_api.schemas import SessionListResponse

router = APIRouter(prefix="/md", tags=["md"])


@router.get("/sessions", response_model=SessionListResponse)
async def list_sessions(status: str | None = "live") -> SessionListResponse:
    del status
    return SessionListResponse(sessions=[])
