"""Historical session log listing (Postgres) and dated file download."""

from __future__ import annotations

import io
import re
import tarfile
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from mftik_db.repositories import (
    AccountRepository,
    ApiRepository,
    SessionLogRepository,
)
from mftik_db.session import session_scope

from mftik_api.schemas import SessionLogListResponse, SessionLogOut

router = APIRouter(tags=["logs"])

_VALID_DOMAINS = frozenset({"sts", "td", "md"})
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")
_MAX_DAYS = 31


@router.get("/logs/{domain}/{stream_id}", response_model=SessionLogListResponse)
async def list_session_logs(
    domain: str,
    stream_id: str,
    before_ts: float | None = Query(default=None),
    before_id: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> SessionLogListResponse:
    _require_domain(domain)
    if not stream_id:
        raise HTTPException(status_code=400, detail="stream_id is required")

    # Fetch one extra to compute has_more without a separate COUNT.
    fetch_limit = limit + 1
    async with session_scope() as db:
        repo = SessionLogRepository(db)
        rows = await repo.list_before(
            domain,
            stream_id,
            before_ts=before_ts,
            before_id=before_id,
            limit=fetch_limit,
        )
    has_more = len(rows) > limit
    page = rows[:limit]
    return SessionLogListResponse(
        logs=[
            SessionLogOut(
                id=row.envelope_id,
                db_id=row.id,
                ts=row.ts,
                source=row.source,
                level=row.level,
                message=row.message,
            )
            for row in page
        ],
        has_more=has_more,
    )


@router.get("/logs/{domain}/{stream_id}/download")
async def download_session_logs(
    domain: str,
    stream_id: str,
    from_day: str = Query(alias="from"),
    to_day: str = Query(alias="to"),
) -> Response:
    """Persisted logs for one stream, as a daily file or a tar.gz of them."""
    _require_domain(domain)
    if not stream_id:
        raise HTTPException(status_code=400, detail="stream_id is required")

    start = parse_utc_day(from_day)
    end = parse_utc_day(to_day)
    if end < start:
        raise HTTPException(status_code=400, detail="to must be on or after from")
    days = (end - start).days + 1
    if days > _MAX_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"range must be at most {_MAX_DAYS} days",
        )

    start_ts = datetime(start.year, start.month, start.day, tzinfo=UTC).timestamp()
    end_ts = (
        datetime(end.year, end.month, end.day, tzinfo=UTC) + timedelta(days=1)
    ).timestamp()

    async with session_scope() as db:
        rows = await SessionLogRepository(db).list_between(
            domain, stream_id, start_ts=start_ts, end_ts=end_ts
        )
        prefix = await filename_prefix(db, domain, stream_id)

    if not rows:
        raise HTTPException(status_code=404, detail="no logs in that range")

    by_day: dict[date, list[Any]] = defaultdict(list)
    for row in rows:
        day = datetime.fromtimestamp(row.ts, tz=UTC).date()
        by_day[day].append(row)

    if days == 1:
        body = "".join(format_log_line(row) for row in rows)
        name = daily_log_name(prefix, domain, start)
        return Response(
            content=body,
            media_type="text/plain; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{name}"'},
        )

    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        for day in sorted(by_day):
            payload = "".join(format_log_line(row) for row in by_day[day]).encode(
                "utf-8"
            )
            info = tarfile.TarInfo(name=daily_log_name(prefix, domain, day))
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))
    name = f"{prefix}_{domain}_{start.isoformat()}_{end.isoformat()}.tar.gz"
    return Response(
        content=archive.getvalue(),
        media_type="application/gzip",
        headers={"Content-Disposition": f'attachment; filename="{name}"'},
    )


def _require_domain(domain: str) -> None:
    if domain not in _VALID_DOMAINS:
        raise HTTPException(
            status_code=400,
            detail=f"domain must be one of {sorted(_VALID_DOMAINS)}",
        )


def parse_utc_day(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail="from/to must be YYYY-MM-DD"
        ) from exc


def safe_filename_part(value: str) -> str:
    cleaned = _UNSAFE.sub("_", value.strip().replace(" ", "_"))
    cleaned = cleaned.strip("._")
    return cleaned or "unknown"


def daily_log_name(prefix: str, domain: str, day: date) -> str:
    return f"{prefix}_{domain}_{day.isoformat()}.log"


def format_log_line(row: Any) -> str:
    dt = datetime.fromtimestamp(row.ts, tz=UTC)
    stamp = dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{dt.microsecond // 1000:03d}Z"
    level = str(row.level).upper().ljust(5)
    source = str(row.source).ljust(16)
    return f"{stamp}  {level}  {source}  {row.message}\n"


async def filename_prefix(db: Any, domain: str, stream_id: str) -> str:
    if domain == "sts":
        return safe_filename_part(stream_id)
    if domain == "md":
        return safe_filename_part(stream_id)
    try:
        api_id = int(stream_id)
    except ValueError:
        return safe_filename_part(stream_id)
    account = await AccountRepository(db).get_by_api_id(api_id)
    if account is not None and account.api is not None:
        venue = safe_filename_part(account.api.venue)
        name = safe_filename_part(account.name)
        return f"{venue}_{name}"
    api = await ApiRepository(db).get(api_id)
    if api is not None:
        return f"{safe_filename_part(api.venue)}_{api_id}"
    return str(api_id)
