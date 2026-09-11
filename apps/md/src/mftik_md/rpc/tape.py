"""MD tape tail RPC — chunked prints from this region's Redis."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import (
    MD_ERROR,
    MD_TAPE_TAIL,
    MdTapeRecord,
    MdTapeTailChunk,
    MdTapeTailChunkEnvelope,
    MdTapeTailRequest,
    RpcError,
    RpcErrorEnvelope,
)

from mftik_md.tape_store import decode_tape_gaps

if TYPE_CHECKING:
    from mftik_md.session import SessionManager
    from mftik_md.tape_store import TapeStore

logger = logging.getLogger(__name__)

#: Records per reply. ``DEFAULT_LIMIT`` is 200k (~40 MB); a core NATS
#: message is about 1 MiB. At ~200 bytes a print this is a comfortable
#: half-megabyte page.
TAPE_RPC_CHUNK = 2_500


def _int_or_none(raw: str | None) -> int | None:
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


async def handle_tape_tail(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
    store: TapeStore | None = None,
    chunk: int = TAPE_RPC_CHUNK,
) -> None:
    """Answer ``md.tape.tail`` from this process's Redis.

    An empty slice is the honest answer when recording is off, this
    region never saw the feed, or the caller asked the wrong instance.
    There is no fallback to another region's disk.
    """
    try:
        payload = MdTapeTailRequest.model_validate(req.envelope.payload or {})
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return

    if store is None and sessions is not None:
        store = sessions.tape_store
    if store is None:
        await req.reply(_empty_chunk(payload.feed))
        return

    try:
        coverage = await store.coverage(payload.feed)
        page, more = await store.tail_page(
            payload.feed,
            limit=max(0, payload.limit),
            before=payload.before,
            chunk=max(1, chunk),
        )
    except Exception as exc:
        logger.exception("md.tape.tail failed feed=%s", payload.feed)
        await _error(req, "tape_failed", str(exc))
        return

    records = [MdTapeRecord(ms=ms, fields=fields) for _id, ms, fields in page]
    before = page[0][0] if page else ""
    await req.reply(
        MdTapeTailChunkEnvelope.wrap(
            MdTapeTailChunk(
                feed=payload.feed,
                records=records,
                before=before,
                more=more,
                continuous_since_ms=_int_or_none(coverage.get("continuous_since_ms")),
                recording=coverage.get("recording") == "1",
                gaps=decode_tape_gaps(coverage.get("gaps")),
            ),
            type=MD_TAPE_TAIL,
            source="md",
            session_id=req.envelope.session_id,
        )
    )


def _empty_chunk(feed: str) -> MdTapeTailChunkEnvelope:
    return MdTapeTailChunkEnvelope.wrap(
        MdTapeTailChunk(feed=feed, recording=False),
        type=MD_TAPE_TAIL,
        source="md",
    )


async def _error(req: IncomingRequest, code: str, message: str) -> None:
    await req.reply(
        RpcErrorEnvelope.wrap(
            RpcError(code=code, message=message),
            type=MD_ERROR,
            source="md",
            session_id=req.envelope.session_id,
        )
    )
