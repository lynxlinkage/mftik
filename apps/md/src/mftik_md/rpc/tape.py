"""MD tape tail RPC — chunked prints from this region's Redis."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mftik.protocol import (
    MD_ERROR,
    MD_TAPE_TAIL,
    MdTapeRecord,
    MdTapeTailChunk,
    MdTapeTailChunkEnvelope,
    MdTapeTailRequest,
    RpcError,
    RpcErrorEnvelope,
    UntypedEnvelope,
)

from mftik_md.tape_store import decode_tape_gaps

if TYPE_CHECKING:
    from mftik.broker.handler import Reply

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
    message: UntypedEnvelope,
    *,
    store: TapeStore | None = None,
    chunk: int = TAPE_RPC_CHUNK,
) -> Reply:
    """Answer ``md.tape.tail`` from this process's Redis.

    An empty slice is the honest answer when recording is off, this
    region never saw the feed, or the caller asked the wrong instance.
    There is no fallback to another region's disk.
    """
    try:
        payload = MdTapeTailRequest.model_validate(message.payload or {})
    except Exception as exc:
        return _error(message, "invalid_payload", str(exc))

    if store is None:
        return _empty_chunk(payload.feed)

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
        return _error(message, "tape_failed", str(exc))

    records = [MdTapeRecord(ms=ms, fields=fields) for _id, ms, fields in page]
    before = page[0][0] if page else ""
    return MdTapeTailChunkEnvelope.wrap(
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
        session_id=message.session_id,
    )


def _empty_chunk(feed: str) -> MdTapeTailChunkEnvelope:
    return MdTapeTailChunkEnvelope.wrap(
        MdTapeTailChunk(feed=feed, recording=False),
        type=MD_TAPE_TAIL,
        source="md",
    )


def _error(message: UntypedEnvelope, code: str, text: str) -> RpcErrorEnvelope:
    return RpcErrorEnvelope.wrap(
        RpcError(code=code, message=text),
        type=MD_ERROR,
        source="md",
        session_id=message.session_id,
    )
