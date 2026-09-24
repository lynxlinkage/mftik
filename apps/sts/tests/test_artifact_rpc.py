"""Artifact RPC: list, sliced read, and a chunked replace on this process's disk."""

from __future__ import annotations

import asyncio
import base64
from pathlib import Path
from types import SimpleNamespace

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.protocol import (
    STS_ARTIFACT_BEGIN,
    STS_ARTIFACT_CHUNK,
    STS_ARTIFACT_COMMIT,
    STS_ARTIFACT_DELETE,
    STS_ARTIFACT_LIST,
    STS_ARTIFACT_READ,
    STS_ERROR,
    RpcError,
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
    Topics,
)
from mftik.strategy.artifacts import DIR_ENV, reset_store
from mftik_sts.rpc import dispatch


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-artifact-rpc") as client:
        yield client


@pytest.fixture
async def serving(broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    reset_store()
    stop = asyncio.Event()
    sessions = SimpleNamespace(get=lambda _sid: None, instance="sts-jp")

    async def serve() -> None:
        async for req in broker.serve(Topics.sts("sts-jp"), stop=stop):
            await dispatch(req, sessions=sessions)

    task = asyncio.create_task(serve())
    await asyncio.sleep(0.02)
    yield tmp_path
    stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    reset_store()


async def _ask(broker: Broker, envelope):  # noqa: ANN001
    return await broker.request(Topics.sts("sts-jp"), envelope, timeout=5.0)


@pytest.mark.asyncio
async def test_a_chunked_put_lists_and_reads_back(
    broker: Broker, serving: Path
) -> None:
    begun = StsArtifactBeginResult.model_validate(
        (
            await _ask(
                broker,
                StsArtifactBeginRequestEnvelope.wrap(
                    StsArtifactBeginRequest(path="weights/model.pt"),
                    type=STS_ARTIFACT_BEGIN,
                    source="api",
                ),
            )
        ).payload
    )
    body = b"abcdefghij"
    await _ask(
        broker,
        StsArtifactChunkRequestEnvelope.wrap(
            StsArtifactChunkRequest(
                token=begun.token,
                offset=0,
                data=base64.b64encode(body[:4]).decode(),
            ),
            type=STS_ARTIFACT_CHUNK,
            source="api",
        ),
    )
    await _ask(
        broker,
        StsArtifactChunkRequestEnvelope.wrap(
            StsArtifactChunkRequest(
                token=begun.token,
                offset=4,
                data=base64.b64encode(body[4:]).decode(),
            ),
            type=STS_ARTIFACT_CHUNK,
            source="api",
        ),
    )
    committed = StsArtifactCommitResult.model_validate(
        (
            await _ask(
                broker,
                StsArtifactCommitRequestEnvelope.wrap(
                    StsArtifactCommitRequest(token=begun.token),
                    type=STS_ARTIFACT_COMMIT,
                    source="api",
                ),
            )
        ).payload
    )
    assert committed.size == len(body)
    assert committed.path == "weights/model.pt"

    listed = StsArtifactListResult.model_validate(
        (
            await _ask(
                broker,
                StsArtifactListRequestEnvelope.wrap(
                    StsArtifactListRequest(),
                    type=STS_ARTIFACT_LIST,
                    source="api",
                ),
            )
        ).payload
    )
    assert [row.path for row in listed.objects] == ["weights/model.pt"]
    assert listed.instance == "sts-jp"

    first = StsArtifactReadChunk.model_validate(
        (
            await _ask(
                broker,
                StsArtifactReadRequestEnvelope.wrap(
                    StsArtifactReadRequest(path="weights/model.pt", offset=0, length=4),
                    type=STS_ARTIFACT_READ,
                    source="api",
                ),
            )
        ).payload
    )
    rest = StsArtifactReadChunk.model_validate(
        (
            await _ask(
                broker,
                StsArtifactReadRequestEnvelope.wrap(
                    StsArtifactReadRequest(
                        path="weights/model.pt", offset=4, length=100
                    ),
                    type=STS_ARTIFACT_READ,
                    source="api",
                ),
            )
        ).payload
    )
    assert base64.b64decode(first.data) + base64.b64decode(rest.data) == body
    assert rest.eof is True

    ack = StsArtifactAck.model_validate(
        (
            await _ask(
                broker,
                StsArtifactDeleteRequestEnvelope.wrap(
                    StsArtifactDeleteRequest(path="weights/model.pt"),
                    type=STS_ARTIFACT_DELETE,
                    source="api",
                ),
            )
        ).payload
    )
    assert ack.ok is True
    assert not (serving / "weights" / "model.pt").exists()


@pytest.mark.asyncio
async def test_an_upload_under_sessions_is_refused(
    broker: Broker, serving: Path
) -> None:
    reply = await _ask(
        broker,
        StsArtifactBeginRequestEnvelope.wrap(
            StsArtifactBeginRequest(path="sessions/abc/weights/model.pt"),
            type=STS_ARTIFACT_BEGIN,
            source="api",
        ),
    )
    assert reply.type == STS_ERROR
    err = RpcError.model_validate(reply.payload)
    assert err.code == "bad_key"
    assert list(serving.rglob("*.part")) == []


@pytest.mark.asyncio
async def test_a_key_that_is_a_directory_is_answered_not_dropped(
    broker: Broker, serving: Path
) -> None:
    """A filesystem failure comes back as an error, not as silence.

    The serve loop logs an exception out of a handler and moves on without
    replying, so an uncaught ``IsADirectoryError`` would cost the caller the
    whole RPC timeout and then read as "the STS did not answer".
    """
    (serving / "weights").mkdir()
    (serving / "weights" / "model.pt").write_bytes(b"first")

    begun = StsArtifactBeginResult.model_validate(
        (
            await _ask(
                broker,
                StsArtifactBeginRequestEnvelope.wrap(
                    StsArtifactBeginRequest(path="weights"),
                    type=STS_ARTIFACT_BEGIN,
                    source="api",
                ),
            )
        ).payload
    )
    reply = await _ask(
        broker,
        StsArtifactCommitRequestEnvelope.wrap(
            StsArtifactCommitRequest(token=begun.token),
            type=STS_ARTIFACT_COMMIT,
            source="api",
        ),
    )
    assert reply.type == STS_ERROR
    assert RpcError.model_validate(reply.payload).code == "io_failed"
    # The object that was there is untouched, and the part is not left behind.
    assert (serving / "weights" / "model.pt").read_bytes() == b"first"
    assert list(serving.rglob("*.part")) == []
