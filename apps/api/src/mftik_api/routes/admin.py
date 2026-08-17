"""Owner-only proxy to the standing updater. The API never holds the socket."""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import ValidationError

from mftik_api.auth.deps import SessionDep
from mftik_api.schemas import UpdateStatusOut

logger = logging.getLogger(__name__)
router = APIRouter(tags=["admin"])

_UNAVAILABLE = UpdateStatusOut(available=False)


def updater_url() -> str:
    return os.getenv("MFTIK_UPDATER_URL", "").strip().rstrip("/")


def updater_token() -> str:
    return os.getenv("MFTIK_UPDATER_TOKEN", "").strip()


def _status_from_updater(payload: dict[str, Any]) -> UpdateStatusOut:
    return UpdateStatusOut(
        available=True,
        state=payload.get("state") or "idle",
        step=payload.get("step") or "done",
        from_version=payload.get("from_version"),
        to_version=payload.get("to_version"),
        feeds_published=int(payload.get("feeds_published") or 0),
        feeds_total=int(payload.get("feeds_total") or 0),
        error=payload.get("error"),
        updated_at=payload.get("updated_at"),
    )


async def call_updater(method: str, path: str) -> httpx.Response:
    """One hop to the updater. Tests replace this; the browser never sees it."""
    base = updater_url()
    if not base:
        raise RuntimeError("updater is not configured")
    async with httpx.AsyncClient(timeout=10.0) as client:
        return await client.request(
            method,
            f"{base}{path}",
            headers={"Authorization": f"Bearer {updater_token()}"},
        )


def _proxy_error(exc: Exception) -> HTTPException:
    logger.warning("updater unreachable: %s", exc)
    return HTTPException(status_code=502, detail="updater unreachable")


@router.get("/admin/update", response_model=UpdateStatusOut)
async def get_update(_owner: SessionDep) -> UpdateStatusOut:
    if not updater_url():
        return _UNAVAILABLE
    try:
        response = await call_updater("GET", "/status")
    except httpx.HTTPError as exc:
        raise _proxy_error(exc) from exc
    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=response.text or "updater refused status",
        )
    try:
        return _status_from_updater(response.json())
    except (ValueError, TypeError, ValidationError, httpx.DecodingError) as exc:
        raise HTTPException(
            status_code=502, detail="updater returned an unreadable status"
        ) from exc


@router.post("/admin/update", response_model=UpdateStatusOut, status_code=202)
async def start_update(_owner: SessionDep) -> UpdateStatusOut:
    if not updater_url():
        raise HTTPException(status_code=404, detail="updater is not configured")
    try:
        response = await call_updater("POST", "/update")
    except httpx.HTTPError as exc:
        raise _proxy_error(exc) from exc
    if response.status_code == 409:
        raise HTTPException(
            status_code=409,
            detail=_detail(response, "an update is already running"),
        )
    if response.status_code != 202:
        raise HTTPException(
            status_code=502,
            detail=_detail(response, "updater refused the update"),
        )
    try:
        body = _status_from_updater(response.json())
    except (ValueError, TypeError, ValidationError, httpx.DecodingError) as exc:
        raise HTTPException(
            status_code=502, detail="updater returned an unreadable status"
        ) from exc
    return body


def _detail(response: httpx.Response, fallback: str) -> str:
    try:
        payload = response.json()
    except (ValueError, httpx.DecodingError):
        return response.text or fallback
    if isinstance(payload, dict):
        error = payload.get("error") or payload.get("detail")
        if error:
            return str(error)
    return fallback
