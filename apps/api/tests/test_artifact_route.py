"""GET/PUT/DELETE /sts/artifacts — the API slices, STS holds the file."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest
from fastapi import HTTPException
from mftik.broker.errors import RequestTimeoutError
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
    ArtifactStore,
    BadArtifactKey,
)
from mftik_api.routes import artifacts as art


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def stream(self):
        yield self._data


class Disk:
    """One STS disk behind ``broker.request``."""

    def __init__(self, root: Path, *, silent: set[str] | None = None) -> None:
        self.store = ArtifactStore(root)
        self.silent = silent or set()

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        instance = subject.removeprefix("sts.") if subject.startswith("sts.") else None
        if instance in self.silent:
            raise RequestTimeoutError(subject, envelope.id, timeout or 0.0)
        try:
            return self._handle(instance, envelope)
        except BadArtifactKey as exc:
            return RpcErrorEnvelope.wrap(
                RpcError(code="bad_key", message=str(exc)),
                type=STS_ERROR,
                source="sts",
            )
        except ArtifactNotFound as exc:
            return RpcErrorEnvelope.wrap(
                RpcError(code="not_found", message=str(exc)),
                type=STS_ERROR,
                source="sts",
            )

    def _handle(self, instance: str | None, envelope):  # noqa: ANN001
        kind = envelope.type
        if kind == STS_ARTIFACT_BEGIN:
            req = StsArtifactBeginRequest.model_validate(envelope.payload)
            token = self.store.begin(req.path)
            return StsArtifactBeginResultEnvelope.wrap(
                StsArtifactBeginResult(token=token),
                type=STS_ARTIFACT_BEGIN,
                source="sts",
            )
        if kind == STS_ARTIFACT_CHUNK:
            req = StsArtifactChunkRequest.model_validate(envelope.payload)
            self.store.chunk(req.token, req.offset, base64.b64decode(req.data))
            return StsArtifactAckEnvelope.wrap(
                StsArtifactAck(), type=STS_ARTIFACT_CHUNK, source="sts"
            )
        if kind == STS_ARTIFACT_COMMIT:
            req = StsArtifactCommitRequest.model_validate(envelope.payload)
            meta = self.store.commit(req.token)
            return StsArtifactCommitResultEnvelope.wrap(
                StsArtifactCommitResult(
                    path=meta.path, size=meta.size, mtime=meta.mtime, digest=meta.digest
                ),
                type=STS_ARTIFACT_COMMIT,
                source="sts",
            )
        if kind == STS_ARTIFACT_ABORT:
            req = StsArtifactTokenRequest.model_validate(envelope.payload)
            self.store.abort(req.token)
            return StsArtifactAckEnvelope.wrap(
                StsArtifactAck(), type=STS_ARTIFACT_ABORT, source="sts"
            )
        if kind == STS_ARTIFACT_DELETE:
            req = StsArtifactDeleteRequest.model_validate(envelope.payload)
            self.store.remove(req.path)
            return StsArtifactAckEnvelope.wrap(
                StsArtifactAck(), type=STS_ARTIFACT_DELETE, source="sts"
            )
        if kind == STS_ARTIFACT_LIST:
            req = StsArtifactListRequest.model_validate(envelope.payload)
            rows = (
                self.store.list_session(req.session_id)
                if req.session_id
                else self.store.list_catalog()
            )
            return StsArtifactListResultEnvelope.wrap(
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
                    instance=instance,
                ),
                type=STS_ARTIFACT_LIST,
                source="sts",
            )
        if kind == STS_ARTIFACT_READ:
            req = StsArtifactReadRequest.model_validate(envelope.payload)
            data, eof = self.store.read_at(req.path, req.offset, req.length)
            return StsArtifactReadChunkEnvelope.wrap(
                StsArtifactReadChunk(
                    path=req.path,
                    offset=req.offset,
                    data=base64.b64encode(data).decode() if data else "",
                    raw_bytes=len(data),
                    eof=eof,
                ),
                type=STS_ARTIFACT_READ,
                source="sts",
            )
        raise AssertionError(kind)


@pytest.fixture(autouse=True)
def no_audit(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _audit(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(art, "record_audit", _audit)


async def _bytes(response) -> bytes:  # noqa: ANN001
    return b"".join([chunk async for chunk in response.body_iterator])


async def test_put_is_sliced_and_get_stitches_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(art, "_CHUNK_BYTES", 4)
    disk = Disk(tmp_path)

    put = await art.put_artifact(
        _Body(b"abcdefghij"), disk, instance="sts-jp", path="weights/model.pt"
    )
    assert put.size == 10
    assert put.digest.startswith("sha256:")
    assert disk.store.read("weights/model.pt").body == b"abcdefghij"  # type: ignore[union-attr]

    response = await art.artifacts(disk, instance="sts-jp", path="weights/model.pt")
    assert await _bytes(response) == b"abcdefghij"

    listed = await art.artifacts(disk, instance="sts-jp")
    assert [row.path for row in listed.objects] == ["weights/model.pt"]


async def test_put_refuses_a_session_key(tmp_path: Path) -> None:
    disk = Disk(tmp_path)
    with pytest.raises(HTTPException) as caught:
        await art.put_artifact(
            _Body(b"x"), disk, instance="sts-jp", path="sessions/abc/weights/model.pt"
        )
    assert caught.value.status_code == 400


async def test_a_write_without_an_instance_does_not_guess(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    async def two() -> list[str]:
        return ["sts-jp", "sts-tw"]

    monkeypatch.setattr(art, "_declared_sts", two)
    with pytest.raises(HTTPException) as caught:
        await art.put_artifact(_Body(b"x"), Disk(tmp_path), path="weights/model.pt")
    assert caught.value.status_code == 400


async def test_an_unreadable_instance_list_refuses_the_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    async def unread() -> None:
        return None

    monkeypatch.setattr(art, "_declared_sts", unread)
    with pytest.raises(HTTPException) as caught:
        await art.put_artifact(_Body(b"x"), Disk(tmp_path), path="weights/model.pt")
    assert caught.value.status_code == 503


async def test_session_objects_stay_apart_per_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    disk = Disk(tmp_path, silent={"sts-tw"})
    disk.store.write("sessions/abc/weights/model.pt", b"jp")

    async def both() -> list[str]:
        return ["sts-jp", "sts-tw"]

    monkeypatch.setattr(art, "_declared_sts", both)
    listed = await art.session_artifacts("abc", disk)
    assert [row.path for row in listed.objects] == ["sessions/abc/weights/model.pt"]
    assert listed.objects[0].instance == "sts-jp"
    assert listed.unanswered == ["sts-tw"]
    catalog = await art.artifacts(disk, instance="sts-jp")
    assert catalog.objects == []
