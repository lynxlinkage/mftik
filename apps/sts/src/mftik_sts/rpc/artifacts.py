"""Serve this STS's artifact store over the control plane.

The files live on the machine this process runs on. The API does not open
them. A whole object does not fit in one broker message, so a download is a
slice and an upload is begin / chunk / commit, with the part file named by
the token so two writers of one key do not share it.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import (
    STS_ARTIFACT_ABORT,
    STS_ARTIFACT_BEGIN,
    STS_ARTIFACT_CHUNK,
    STS_ARTIFACT_COMMIT,
    STS_ARTIFACT_DELETE,
    STS_ARTIFACT_LIST,
    STS_ARTIFACT_READ,
    STS_ERROR,
    RpcError,
    RpcErrorEnvelope,
    StsArtifactAck,
    StsArtifactAckEnvelope,
    StsArtifactBeginRequest,
    StsArtifactBeginResult,
    StsArtifactBeginResultEnvelope,
    StsArtifactChunkRequest,
    StsArtifactCommitRequest,
    StsArtifactCommitResult,
    StsArtifactCommitResultEnvelope,
    StsArtifactDeleteRequest,
    StsArtifactListRequest,
    StsArtifactListResult,
    StsArtifactListResultEnvelope,
    StsArtifactObject,
    StsArtifactReadChunk,
    StsArtifactReadChunkEnvelope,
    StsArtifactReadRequest,
    StsArtifactTokenRequest,
)
from mftik.strategy.artifacts import (
    ArtifactNotFound,
    ArtifactUploadError,
    BadArtifactKey,
    get_store,
)

if TYPE_CHECKING:
    from mftik_sts.session import SessionManager

logger = logging.getLogger(__name__)

#: Ceiling on one slice, whatever was asked for. A caller does not get to
#: decide how long this handler holds the RPC loop.
MAX_CHUNK_BYTES = 1_048_576


def _instance(sessions: SessionManager | None) -> str | None:
    return sessions.instance if sessions is not None else None


async def handle_artifact_list(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    try:
        payload = StsArtifactListRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    store = get_store()
    try:
        if payload.session_id:
            rows = await asyncio.to_thread(store.list_session, payload.session_id)
        else:
            rows = await asyncio.to_thread(store.list_catalog)
    except BadArtifactKey as exc:
        await _error(req, "bad_key", str(exc))
        return
    await req.reply(
        StsArtifactListResultEnvelope.wrap(
            StsArtifactListResult(
                objects=[
                    StsArtifactObject(
                        path=row.path,
                        size=row.size,
                        mtime=row.mtime,
                        digest=row.digest,
                    )
                    for row in rows
                ],
                instance=_instance(sessions),
            ),
            type=STS_ARTIFACT_LIST,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


async def handle_artifact_read(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    del sessions
    try:
        payload = StsArtifactReadRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    length = min(max(0, payload.length), MAX_CHUNK_BYTES)
    try:
        raw, eof = await asyncio.to_thread(
            get_store().read_at, payload.path, max(0, payload.offset), length
        )
    except BadArtifactKey as exc:
        await _error(req, "bad_key", str(exc))
        return
    except ArtifactNotFound as exc:
        await _error(req, "not_found", str(exc))
        return
    await req.reply(
        StsArtifactReadChunkEnvelope.wrap(
            StsArtifactReadChunk(
                path=payload.path,
                offset=max(0, payload.offset),
                data=base64.b64encode(raw).decode("ascii") if raw else "",
                raw_bytes=len(raw),
                eof=eof,
            ),
            type=STS_ARTIFACT_READ,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


async def handle_artifact_begin(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    del sessions
    try:
        payload = StsArtifactBeginRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    try:
        token = await asyncio.to_thread(get_store().begin, payload.path)
    except BadArtifactKey as exc:
        await _error(req, "bad_key", str(exc))
        return
    await req.reply(
        StsArtifactBeginResultEnvelope.wrap(
            StsArtifactBeginResult(token=token),
            type=STS_ARTIFACT_BEGIN,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


async def handle_artifact_chunk(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    del sessions
    try:
        payload = StsArtifactChunkRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    try:
        raw = base64.b64decode(payload.data, validate=True) if payload.data else b""
    except (binascii.Error, ValueError) as exc:
        await _error(req, "bad_chunk", f"chunk is not base64: {exc}")
        return
    if len(raw) > MAX_CHUNK_BYTES:
        await _error(
            req,
            "bad_chunk",
            f"chunk is {len(raw)} bytes; max is {MAX_CHUNK_BYTES}",
        )
        return
    try:
        await asyncio.to_thread(get_store().chunk, payload.token, payload.offset, raw)
    except ArtifactUploadError as exc:
        await _error(req, "unknown_upload", str(exc))
        return
    except BadArtifactKey as exc:
        await _error(req, "bad_key", str(exc))
        return
    await req.reply(
        StsArtifactAckEnvelope.wrap(
            StsArtifactAck(),
            type=STS_ARTIFACT_CHUNK,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


async def handle_artifact_commit(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    del sessions
    try:
        payload = StsArtifactCommitRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    try:
        meta = await asyncio.to_thread(get_store().commit, payload.token)
    except ArtifactUploadError as exc:
        await _error(req, "unknown_upload", str(exc))
        return
    except BadArtifactKey as exc:
        await _error(req, "bad_key", str(exc))
        return
    await req.reply(
        StsArtifactCommitResultEnvelope.wrap(
            StsArtifactCommitResult(
                path=meta.path,
                size=meta.size,
                mtime=meta.mtime,
                digest=meta.digest,
            ),
            type=STS_ARTIFACT_COMMIT,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


async def handle_artifact_abort(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    del sessions
    try:
        payload = StsArtifactTokenRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    await asyncio.to_thread(get_store().abort, payload.token)
    await req.reply(
        StsArtifactAckEnvelope.wrap(
            StsArtifactAck(),
            type=STS_ARTIFACT_ABORT,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


async def handle_artifact_delete(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    del sessions
    try:
        payload = StsArtifactDeleteRequest.model_validate(req.envelope.payload)
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    try:
        await asyncio.to_thread(get_store().remove, payload.path)
    except BadArtifactKey as exc:
        await _error(req, "bad_key", str(exc))
        return
    except ArtifactNotFound as exc:
        await _error(req, "not_found", str(exc))
        return
    await req.reply(
        StsArtifactAckEnvelope.wrap(
            StsArtifactAck(),
            type=STS_ARTIFACT_DELETE,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


async def _error(req: IncomingRequest, code: str, message: str) -> None:
    await req.reply(
        RpcErrorEnvelope.wrap(
            RpcError(code=code, message=message),
            type=STS_ERROR,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )
