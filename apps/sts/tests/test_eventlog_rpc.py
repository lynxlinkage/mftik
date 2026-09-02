"""Serving a session's event log over STS's RPC subject.

Everything here goes through the real broker path — request on the subject,
dispatch, reply — because the point of this transport is that the caller is on
another machine, and a handler tested by direct call would not exercise the
part that has to survive the wire.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
from pathlib import Path
from types import SimpleNamespace

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.protocol import (
    STS_ERROR,
    STS_EVENTLOG_INFO,
    STS_EVENTLOG_READ,
    RpcError,
    StsEventLogChunk,
    StsEventLogInfo,
    StsEventLogInfoRequest,
    StsEventLogInfoRequestEnvelope,
    StsEventLogReadRequest,
    StsEventLogReadRequestEnvelope,
    Topics,
)
from mftik.strategy.eventlog import DIR_ENV
from mftik_sts.rpc import dispatch


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-evrpc") as client:
        yield client


@pytest.fixture
async def sts_rpc(broker: Broker):
    """A running STS RPC server, with a stub session manager."""
    stop = asyncio.Event()
    live: dict[str, object] = {}
    sessions = SimpleNamespace(get=live.get)

    async def serve() -> None:
        async for req in broker.serve(Topics.STS, stop=stop):
            await dispatch(req, sessions=sessions)

    task = asyncio.create_task(serve())
    await asyncio.sleep(0.02)
    yield live
    stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def _info(broker: Broker, session_id: str):
    return await broker.request(
        Topics.STS,
        StsEventLogInfoRequestEnvelope.wrap(
            StsEventLogInfoRequest(session_id=session_id),
            type=STS_EVENTLOG_INFO,
            source="api",
        ),
        timeout=5.0,
    )


async def _read(
    broker: Broker, session_id: str, part: str, offset: int, length: int
):
    return await broker.request(
        Topics.STS,
        StsEventLogReadRequestEnvelope.wrap(
            StsEventLogReadRequest(
                session_id=session_id,
                part=part,
                offset=offset,
                length=length,
            ),
            type=STS_EVENTLOG_READ,
            source="api",
        ),
        timeout=5.0,
    )


def _write_parts(tmp_path: Path, session_id: str) -> dict[str, bytes]:
    """One current file and two rotations, each with its own content."""
    bodies = {
        f"{session_id}.jsonl.2": b'{"seq":1}\n{"seq":2}\n',
        f"{session_id}.jsonl.1": b'{"seq":3}\n{"seq":4}\n',
        f"{session_id}.jsonl": b'{"seq":5}\n',
    }
    for name, body in bodies.items():
        (tmp_path / name).write_bytes(body)
    return bodies


async def test_info_lists_the_parts_oldest_first(
    broker: Broker, sts_rpc, tmp_path: Path, monkeypatch
) -> None:
    """Rotation numbers backwards from the order a reader wants them."""
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    bodies = _write_parts(tmp_path, "s1")

    reply = await _info(broker, "s1")
    info = StsEventLogInfo.model_validate(reply.payload)

    assert info.available is True
    assert info.enabled is True
    assert [p.name for p in info.parts] == [
        "s1.jsonl.2",
        "s1.jsonl.1",
        "s1.jsonl",
    ]
    assert [p.size for p in info.parts] == [len(b) for b in bodies.values()]
    assert info.total_bytes == sum(len(b) for b in bodies.values())
    assert info.live is False


async def test_info_separates_off_from_absent(
    broker: Broker, sts_rpc, tmp_path: Path, monkeypatch
) -> None:
    """"We keep none" and "we keep some, not that one" are different answers."""
    monkeypatch.delenv(DIR_ENV, raising=False)
    off = StsEventLogInfo.model_validate((await _info(broker, "s1")).payload)
    assert off.enabled is False
    assert off.available is False

    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    absent = StsEventLogInfo.model_validate((await _info(broker, "s1")).payload)
    assert absent.enabled is True
    assert absent.available is False
    assert absent.parts == []


async def test_info_flags_a_session_still_running(
    broker: Broker, sts_rpc, tmp_path: Path, monkeypatch
) -> None:
    """A download of a live session is a prefix, and should say so."""
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    _write_parts(tmp_path, "s1")
    sts_rpc["s1"] = object()

    info = StsEventLogInfo.model_validate((await _info(broker, "s1")).payload)
    assert info.live is True


async def test_read_returns_the_bytes_gzipped(
    broker: Broker, sts_rpc, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    bodies = _write_parts(tmp_path, "s1")

    reply = await _read(broker, "s1", "s1.jsonl.2", 0, 4096)
    chunk = StsEventLogChunk.model_validate(reply.payload)

    body = bodies["s1.jsonl.2"]
    assert gzip.decompress(base64.b64decode(chunk.data)) == body
    assert chunk.raw_bytes == len(body)
    assert chunk.eof is True


async def test_chunks_concatenate_into_one_valid_gzip(
    broker: Broker, sts_rpc, tmp_path: Path, monkeypatch
) -> None:
    """The whole transport rests on this: gzip members concatenate."""
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    body = b"".join(b'{"seq":%d}\n' % n for n in range(200))
    (tmp_path / "s1.jsonl").write_bytes(body)

    blob = b""
    offset = 0
    while True:
        reply = await _read(broker, "s1", "s1.jsonl", offset, 64)
        chunk = StsEventLogChunk.model_validate(reply.payload)
        blob += base64.b64decode(chunk.data)
        offset += chunk.raw_bytes
        if chunk.eof:
            break

    assert offset == len(body)
    # Many members, one gzip — which is what the API hands the browser.
    assert gzip.decompress(blob) == body


async def test_a_part_that_was_not_listed_is_refused(
    broker: Broker, sts_rpc, tmp_path: Path, monkeypatch
) -> None:
    """The part name is matched against the listing, never joined to a path."""
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    _write_parts(tmp_path, "s1")
    (tmp_path.parent / "secret.txt").write_bytes(b"not yours")

    for part in ("../secret.txt", "/etc/passwd", "s2.jsonl", "s1.jsonl.9"):
        reply = await _read(broker, "s1", part, 0, 4096)
        assert reply.type == STS_ERROR, part
        assert RpcError.model_validate(reply.payload).code == "not_found"


async def test_reading_past_the_end_is_empty_and_final(
    broker: Broker, sts_rpc, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    _write_parts(tmp_path, "s1")

    reply = await _read(broker, "s1", "s1.jsonl", 9_999, 4096)
    chunk = StsEventLogChunk.model_validate(reply.payload)

    assert chunk.data == ""
    assert chunk.raw_bytes == 0
    assert chunk.eof is True


async def test_a_read_cannot_ask_for_an_unbounded_slice(
    broker: Broker, sts_rpc, tmp_path: Path, monkeypatch
) -> None:
    """The caller picks a page size; it does not get to pick the ceiling."""
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    body = b"x" * (2 * 1024 * 1024)
    (tmp_path / "s1.jsonl").write_bytes(body)

    reply = await _read(broker, "s1", "s1.jsonl", 0, 8 * 1024 * 1024)
    chunk = StsEventLogChunk.model_validate(reply.payload)

    assert chunk.raw_bytes == 1_048_576
    assert chunk.eof is False
