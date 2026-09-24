"""HTTP facade for one STS's artifact store.

The API does not open the directory. Each verb is an RPC to
``Topics.sts(instance)``, sliced so a checkpoint never has to fit in one
broker message. A read may fall back to the shared STS subject when no
instance is declared. A write may not: a put that lands on whichever disk
answered first is not a place anybody chose.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import re
from collections.abc import AsyncIterator

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from mftik.protocol import (
    STS_ARTIFACT_ABORT,
    STS_ARTIFACT_BEGIN,
    STS_ARTIFACT_CHUNK,
    STS_ARTIFACT_COMMIT,
    STS_ARTIFACT_DELETE,
    STS_ARTIFACT_LIST,
    STS_ARTIFACT_READ,
    StsArtifactAck,
    StsArtifactBeginRequest,
    StsArtifactBeginRequestEnvelope,
    StsArtifactBeginResult,
    StsArtifactChunkRequest,
    StsArtifactChunkRequestEnvelope,
    StsArtifactCommitRequest,
    StsArtifactCommitRequestEnvelope,
    StsArtifactCommitResult,
    StsArtifactDeleteRequest,
    StsArtifactDeleteRequestEnvelope,
    StsArtifactListRequest,
    StsArtifactListRequestEnvelope,
    StsArtifactListResult,
    StsArtifactReadChunk,
    StsArtifactReadRequest,
    StsArtifactReadRequestEnvelope,
    StsArtifactTokenRequest,
    StsArtifactTokenRequestEnvelope,
    Topics,
)
from mftik_db.models.session import SessionDomain
from mftik_db.repositories import InstanceRepository
from mftik_db.session import session_scope

from mftik_api.audit_util import record_audit
from mftik_api.auth import ANONYMOUS, OwnerId, PrincipalDep
from mftik_api.broker_rpc import DomainRpcError, request_domain
from mftik_api.deps import DEFAULT_USER_ID, BrokerDep
from mftik_api.schemas import ArtifactListResponse, ArtifactObjectOut

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sts", tags=["sts"])

#: Bytes of an object per RPC. Same budget as an event-log slice: one reply
#: stays under the broker's payload cap, and the RPC loop is not held for the
#: whole file.
_CHUNK_BYTES = 262_144
_UNSAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


async def _declared_sts() -> list[str] | None:
    """Names of declared STS instances, or None when the table cannot be read.

    None is not an empty list. Empty means the node has declared none. None
    means this process could not ask, and a write must stop rather than guess
    a disk.
    """
    try:
        async with session_scope() as db:
            rows = await InstanceRepository(db).list_all(domain=SessionDomain.STS.value)
    except Exception:
        logger.warning("artifact: could not list STS instances", exc_info=True)
        return None
    return [row.name for row in rows]


def _subject(instance: str | None) -> str:
    return Topics.STS if instance is None else Topics.sts(instance)


async def _read_instance(instance: str | None) -> str | None:
    """The STS a list or a download is addressed to.

    A named instance is used as given. One declared STS is that one. None
    declared, or a table that cannot be read, falls back to the shared
    subject — a read either finds the object or does not. Several declared
    STS processes have no default disk.
    """
    if instance:
        return instance
    names = await _declared_sts()
    if not names:
        return None
    if len(names) > 1:
        raise HTTPException(
            status_code=400,
            detail="instance is required when more than one STS is declared",
        )
    return names[0]


async def _write_instance(instance: str | None) -> str | None:
    """The STS a put or a delete is addressed to.

    A named instance is used as given. Otherwise the single declared STS, or
    a refusal: several disks have no default, and an unreadable table is not
    "send it to whoever answers".
    """
    if instance:
        return instance
    names = await _declared_sts()
    if not names:
        raise HTTPException(
            status_code=503,
            detail=(
                "no STS instance to write to — the instance list is empty "
                "or unreadable, and a write is not sent to the shared subject"
            ),
        )
    if len(names) > 1:
        raise HTTPException(
            status_code=400,
            detail="instance is required when more than one STS is declared",
        )
    return names[0]


def _http_error(exc: DomainRpcError) -> HTTPException:
    if exc.code == "bad_key":
        return HTTPException(status_code=400, detail=exc.message)
    if exc.code == "not_found":
        return HTTPException(status_code=404, detail=exc.message)
    if exc.code in {"unknown_upload", "bad_chunk"}:
        return HTTPException(status_code=409, detail=exc.message)
    return HTTPException(status_code=502, detail=exc.message)


async def _list_one(
    broker: BrokerDep, instance: str | None, *, session_id: str | None
) -> StsArtifactListResult:
    return await request_domain(
        broker,
        _subject(instance),
        StsArtifactListRequestEnvelope.wrap(
            StsArtifactListRequest(session_id=session_id),
            type=STS_ARTIFACT_LIST,
            source="api",
            session_id=session_id,
        ),
        result_type=StsArtifactListResult,
        timeout=30.0,
    )


def _rows(
    result: StsArtifactListResult, instance: str | None
) -> list[ArtifactObjectOut]:
    name = result.instance or instance
    return [
        ArtifactObjectOut(
            path=obj.path,
            size=obj.size,
            mtime=obj.mtime,
            digest=obj.digest,
            instance=name,
        )
        for obj in result.objects
    ]


@router.get("/artifacts", response_model=None)
async def artifacts(
    broker: BrokerDep,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
    instance: str | None = None,
    path: str | None = None,
    stat: bool = False,
) -> ArtifactListResponse | ArtifactObjectOut | StreamingResponse:
    """List one STS's uploaded objects, or download / stat one key.

    ``sessions/`` is not in the list. A download is streamed a slice at a
    time. ``stat`` returns the metadata without the body; the list already
    carries the same fields.
    """
    target = await _read_instance(instance)

    if path is None:
        if stat:
            raise HTTPException(status_code=400, detail="path is required")
        try:
            result = await _list_one(broker, target, session_id=None)
        except DomainRpcError as exc:
            raise _http_error(exc) from exc
        body = ArtifactListResponse(objects=_rows(result, target))
        await record_audit(
            user_id=owner,
            operation="sts.artifact.list",
            result=f"instance={target} count={len(body.objects)}",
            principal=principal,
        )
        return body

    if stat:
        session_id = (
            _session_id_of(path)
            if path == "sessions" or path.startswith("sessions/")
            else None
        )
        try:
            result = await _list_one(broker, target, session_id=session_id)
        except DomainRpcError as exc:
            raise _http_error(exc) from exc
        match = next((row for row in result.objects if row.path == path), None)
        if match is None:
            raise HTTPException(status_code=404, detail=f"no artifact {path!r}")
        await record_audit(
            user_id=owner,
            operation="sts.artifact.stat",
            result=f"instance={target} path={path}",
            principal=principal,
        )
        return match

    try:
        first = await _read_slice(broker, target, path, 0)
    except DomainRpcError as exc:
        raise _http_error(exc) from exc
    await record_audit(
        user_id=owner,
        operation="sts.artifact.get",
        result=f"instance={target} path={path}",
        principal=principal,
    )
    name = _safe_name(path.rsplit("/", 1)[-1])
    return StreamingResponse(
        _object_chunks(broker, target, path, first),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{name}"'},
    )


def _session_id_of(path: str) -> str:
    parts = path.split("/")
    if len(parts) < 3 or parts[0] != "sessions" or not parts[1]:
        raise HTTPException(
            status_code=400, detail=f"not a session artifact path: {path!r}"
        )
    return parts[1]


@router.put("/artifacts", response_model=ArtifactObjectOut)
async def put_artifact(
    request: Request,
    broker: BrokerDep,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
    instance: str | None = None,
    path: str = Query(...),
) -> ArtifactObjectOut:
    """Replace ``path`` on one STS with the request body."""
    target = await _write_instance(instance)
    try:
        begun = await request_domain(
            broker,
            _subject(target),
            StsArtifactBeginRequestEnvelope.wrap(
                StsArtifactBeginRequest(path=path),
                type=STS_ARTIFACT_BEGIN,
                source="api",
            ),
            result_type=StsArtifactBeginResult,
            timeout=30.0,
        )
    except DomainRpcError as exc:
        raise _http_error(exc) from exc

    offset = 0
    try:
        async for piece in request.stream():
            view = memoryview(piece)
            pos = 0
            while pos < len(view):
                raw = bytes(view[pos : pos + _CHUNK_BYTES])
                pos += len(raw)
                await request_domain(
                    broker,
                    _subject(target),
                    StsArtifactChunkRequestEnvelope.wrap(
                        StsArtifactChunkRequest(
                            token=begun.token,
                            offset=offset,
                            data=base64.b64encode(raw).decode("ascii"),
                        ),
                        type=STS_ARTIFACT_CHUNK,
                        source="api",
                    ),
                    result_type=StsArtifactAck,
                    timeout=30.0,
                )
                offset += len(raw)
        committed = await request_domain(
            broker,
            _subject(target),
            StsArtifactCommitRequestEnvelope.wrap(
                StsArtifactCommitRequest(token=begun.token),
                type=STS_ARTIFACT_COMMIT,
                source="api",
            ),
            result_type=StsArtifactCommitResult,
            timeout=60.0,
        )
    except DomainRpcError as exc:
        await _abort(broker, target, begun.token)
        raise _http_error(exc) from exc
    except Exception:
        await _abort(broker, target, begun.token)
        raise

    await record_audit(
        user_id=owner,
        operation="sts.artifact.put",
        result=f"instance={target} path={path} bytes={committed.size}",
        principal=principal,
    )
    return ArtifactObjectOut(
        path=committed.path,
        size=committed.size,
        mtime=committed.mtime,
        digest=committed.digest,
        instance=target,
    )


@router.delete("/artifacts", response_model=ArtifactObjectOut)
async def delete_artifact(
    broker: BrokerDep,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
    instance: str | None = None,
    path: str = Query(...),
) -> ArtifactObjectOut:
    """Remove one uploaded key. A ``sessions/`` key is refused by STS."""
    target = await _write_instance(instance)
    try:
        await request_domain(
            broker,
            _subject(target),
            StsArtifactDeleteRequestEnvelope.wrap(
                StsArtifactDeleteRequest(path=path),
                type=STS_ARTIFACT_DELETE,
                source="api",
            ),
            result_type=StsArtifactAck,
            timeout=30.0,
        )
    except DomainRpcError as exc:
        raise _http_error(exc) from exc
    await record_audit(
        user_id=owner,
        operation="sts.artifact.delete",
        result=f"instance={target} path={path}",
        principal=principal,
    )
    return ArtifactObjectOut(path=path, size=0, mtime=0, digest="", instance=target)


@router.get("/sessions/{session_id}/artifacts")
async def session_artifacts(
    session_id: str,
    broker: BrokerDep,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> ArtifactListResponse:
    """Objects this session has written, on every declared STS.

    Rows are not merged. Two instances holding the same key hold two objects,
    and each row names the disk it came from. An instance that does not
    answer is listed in ``unanswered`` rather than rendered as empty.
    """
    names = await _declared_sts()
    targets: list[str | None] = list(names) if names else [None]
    objects: list[ArtifactObjectOut] = []
    unanswered: list[str] = []

    async def _ask(name: str | None) -> tuple[str | None, StsArtifactListResult | None]:
        try:
            result = await _list_one(broker, name, session_id=session_id)
        except DomainRpcError:
            logger.warning(
                "artifact session list: %s did not answer for session=%s",
                name or "the shared subject",
                session_id,
            )
            return name, None
        return name, result

    replies = await asyncio.gather(*(_ask(name) for name in targets))
    answered = False
    for name, result in replies:
        if result is None:
            if name is not None:
                unanswered.append(name)
            continue
        answered = True
        objects.extend(_rows(result, name))

    if not answered and not names:
        raise HTTPException(
            status_code=502,
            detail=f"no STS answered for the artifacts of {session_id}",
        )

    await record_audit(
        user_id=owner,
        operation="sts.artifact.session_list",
        result=(
            f"session_id={session_id} count={len(objects)} "
            f"unanswered={','.join(unanswered)}"
        ),
        principal=principal,
    )
    return ArtifactListResponse(objects=objects, unanswered=unanswered)


async def _abort(broker: BrokerDep, instance: str | None, token: str) -> None:
    try:
        await request_domain(
            broker,
            _subject(instance),
            StsArtifactTokenRequestEnvelope.wrap(
                StsArtifactTokenRequest(token=token),
                type=STS_ARTIFACT_ABORT,
                source="api",
            ),
            result_type=StsArtifactAck,
            timeout=15.0,
        )
    except DomainRpcError:
        logger.exception("artifact abort failed instance=%s", instance)


async def _read_slice(
    broker: BrokerDep, instance: str | None, path: str, offset: int
) -> StsArtifactReadChunk:
    return await request_domain(
        broker,
        _subject(instance),
        StsArtifactReadRequestEnvelope.wrap(
            StsArtifactReadRequest(path=path, offset=offset, length=_CHUNK_BYTES),
            type=STS_ARTIFACT_READ,
            source="api",
        ),
        result_type=StsArtifactReadChunk,
        timeout=30.0,
    )


async def _object_chunks(
    broker: BrokerDep,
    instance: str | None,
    path: str,
    first: StsArtifactReadChunk,
) -> AsyncIterator[bytes]:
    """Yield ``first``, then the slices after it. A later failure ends the body."""
    if first.data:
        yield base64.b64decode(first.data)
    offset = first.raw_bytes
    if first.eof or first.raw_bytes == 0:
        return
    while True:
        try:
            chunk = await _read_slice(broker, instance, path, offset)
        except DomainRpcError as exc:
            logger.error(
                "artifact download failed instance=%s path=%s offset=%d: %s",
                instance,
                path,
                offset,
                exc.message,
            )
            return
        if chunk.data:
            yield base64.b64decode(chunk.data)
        offset += chunk.raw_bytes
        if chunk.eof or chunk.raw_bytes == 0:
            break


def _safe_name(name: str) -> str:
    cleaned = _UNSAFE_NAME.sub("_", name).strip("._")
    return cleaned or "artifact"
