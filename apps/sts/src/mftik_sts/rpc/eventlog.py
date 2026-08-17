"""Serve a session's event log over the control plane, a slice at a time.

The log is a file on whatever machine STS runs on, and the API — which is what
a browser can reach — is not that machine. Rather than assume they share a
filesystem, the file comes back the same way every other answer does: over the
broker, through STS's own RPC subject.

Paged, because that subject is served in turn. A handler that streamed a whole
file would hold every session create, pause and stop behind it for as long as
the transfer took. One bounded read per request keeps the loop moving, costs
Redis one chunk at a time rather than the file, and lets an interrupted
download resume where it stopped instead of starting again.

Compressed at this end. Event logs are jsonl, which gives up about a factor of
ten, and the broker carrying these bytes is the same one carrying order acks.
Each chunk is a standalone gzip member, so whoever collects them concatenates
and is left holding one valid ``.gz`` without having decompressed anything.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import (
    STS_ERROR,
    STS_EVENTLOG_INFO,
    STS_EVENTLOG_READ,
    RpcError,
    RpcErrorEnvelope,
    StsEventLogChunk,
    StsEventLogChunkEnvelope,
    StsEventLogInfo,
    StsEventLogInfoEnvelope,
    StsEventLogInfoRequest,
    StsEventLogPart,
    StsEventLogReadRequest,
)

from mftik_sts.eventlog import eventlog_dir, log_parts, part_path

if TYPE_CHECKING:
    from mftik_sts.session import SessionManager

logger = logging.getLogger(__name__)

#: Ceiling on one read, whatever was asked for. Bounds the reply Redis has to
#: hold and the time this handler keeps the RPC loop — both of which a caller
#: choosing its own page size should not get to decide.
MAX_READ_BYTES = 1_048_576

#: Cheap end of the scale on purpose. The gain from 6 to 9 on jsonl is a few
#: percent for several times the CPU, and this runs on the process that is
#: also running strategies.
GZIP_LEVEL = 4


async def handle_eventlog_info(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    """Answer what this process holds for one session."""
    try:
        payload = StsEventLogInfoRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return

    enabled = eventlog_dir() is not None
    parts = await asyncio.to_thread(log_parts, payload.session_id)
    stats = await asyncio.to_thread(_stat_all, parts)
    live = sessions is not None and sessions.get(payload.session_id) is not None

    await req.reply(
        StsEventLogInfoEnvelope.wrap(
            StsEventLogInfo(
                session_id=payload.session_id,
                available=bool(stats),
                enabled=enabled,
                parts=stats,
                total_bytes=sum(part.size for part in stats),
                live=live,
            ),
            type=STS_EVENTLOG_INFO,
            source="sts",
            session_id=payload.session_id,
        )
    )


async def handle_eventlog_read(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    """Return one slice of one part, gzipped."""
    del sessions
    try:
        payload = StsEventLogReadRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return

    path = await asyncio.to_thread(part_path, payload.session_id, payload.part)
    if path is None:
        # Said plainly, because on a multi-process STS the likeliest cause is
        # not a missing file but a request that reached the wrong process.
        await _error(
            req,
            "not_found",
            f"no event log part {payload.part!r} for session "
            f"{payload.session_id!r} on this STS",
        )
        return

    length = max(0, min(payload.length, MAX_READ_BYTES))
    try:
        raw, eof = await asyncio.to_thread(
            _read_slice, path, max(0, payload.offset), length
        )
    except OSError as exc:
        logger.exception(
            "STS event log read failed session=%s part=%s",
            payload.session_id,
            payload.part,
        )
        await _error(req, "read_failed", str(exc))
        return

    data = (
        base64.b64encode(gzip.compress(raw, GZIP_LEVEL)).decode("ascii")
        if raw
        else ""
    )
    await req.reply(
        StsEventLogChunkEnvelope.wrap(
            StsEventLogChunk(
                session_id=payload.session_id,
                part=payload.part,
                offset=max(0, payload.offset),
                data=data,
                raw_bytes=len(raw),
                eof=eof,
            ),
            type=STS_EVENTLOG_READ,
            source="sts",
            session_id=payload.session_id,
        )
    )


def _stat_all(paths: list[Path]) -> list[StsEventLogPart]:
    """Size every part, skipping any that vanished between listing and stat."""
    out: list[StsEventLogPart] = []
    for path in paths:
        try:
            stat = path.stat()
        except OSError:
            continue
        out.append(
            StsEventLogPart(
                name=path.name, size=stat.st_size, modified=stat.st_mtime
            )
        )
    return out


def _read_slice(path: Path, offset: int, length: int) -> tuple[bytes, bool]:
    """Read ``length`` bytes from ``offset``, and say whether more follow.

    ``eof`` is decided by a short read rather than by comparing against a size
    taken earlier: a live session is appending to this file, and a size from
    the listing would have been stale before the first chunk was sent.
    """
    with path.open("rb") as handle:
        handle.seek(offset)
        raw = handle.read(length)
        return raw, len(raw) < length


async def _error(req: IncomingRequest, code: str, message: str) -> None:
    await req.reply(
        RpcErrorEnvelope.wrap(
            RpcError(code=code, message=message),
            type=STS_ERROR,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )
