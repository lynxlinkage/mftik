"""Venue API credential listing (for STS deploy TD selection)."""

from __future__ import annotations

from fastapi import APIRouter
from mft_db.repositories import ApiRepository
from mft_db.session import session_scope

from mft_api.schemas import ApiListResponse, ApiOut

router = APIRouter(tags=["apis"])


@router.get("/apis", response_model=ApiListResponse)
async def list_apis() -> ApiListResponse:
    async with session_scope() as db:
        rows = await ApiRepository(db).list_all()
    return ApiListResponse(
        apis=[
            ApiOut(
                id=row.id,
                venue=row.venue,
                api_key=row.api_key,
                type=row.type,
            )
            for row in rows
        ]
    )
