"""``GET /workers``: one row per worker from the latest procman report.

Read-only. MD and TD pages (B10-03) read this same list. The route does
not restart a worker and does not filter to stale releases (B8-06, F24).
"""

from __future__ import annotations

from fastapi import APIRouter

from mftik_api.procman_reports import WorkerRow, report_store
from mftik_api.schemas import WorkerListResponse, WorkerOut

router = APIRouter(tags=["workers"])


def _out(row: WorkerRow) -> WorkerOut:
    return WorkerOut(
        plane=row.plane,
        instance=row.instance,
        id=row.id,
        incarnation=row.incarnation,
        phase=row.phase,
        ready=row.ready,
        code_ref=row.code_ref,
        rss_bytes=row.rss_bytes,
        age_s=row.age_s,
    )


@router.get("/workers", response_model=WorkerListResponse)
async def list_workers() -> WorkerListResponse:
    """Every worker in the latest ``procman.report`` of each plane instance.

    ``age_s`` is how long ago that instance's report arrived. An instance
    that stops publishing stays, and the age grows. The list is not a
    reason to reclaim intents (F32).
    """
    return WorkerListResponse(
        workers=[_out(row) for row in report_store().rows()]
    )
