"""GET /sts/sessions/{id}/eventlog — paging STS's log into one download.

The API never sees a file here. It asks STS for slices and forwards them, so
what is worth testing is the paging: that every part is walked, in order, from
the right offsets, and that what comes out the other end is the log.
"""

from __future__ import annotations

import base64
import gzip

import pytest
from fastapi import HTTPException
from mft.protocol import (
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
from mft_api.routes import sts as sts_routes


class FakeSts:
    """Stands in for the STS on the other side of the broker."""

    def __init__(self, parts: dict[str, bytes], *, enabled: bool = True) -> None:
        self.parts = parts
        self.enabled = enabled
        #: (part, offset, length) of every read, in the order they arrived.
        self.reads: list[tuple[str, int, int]] = []

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        if envelope.type == STS_EVENTLOG_INFO:
            return self._info(envelope)
        if envelope.type == STS_EVENTLOG_READ:
            return self._read(envelope)
        raise AssertionError(f"unexpected request: {envelope.type}")

    def _info(self, envelope):  # noqa: ANN001
        # In-process the payload is still a model; over the wire it would be a
        # dict. ``model_validate`` takes either.
        req = StsEventLogInfoRequest.model_validate(envelope.payload)
        return StsEventLogInfoEnvelope.wrap(
            StsEventLogInfo(
                session_id=req.session_id,
                available=bool(self.parts),
                enabled=self.enabled,
                parts=[
                    StsEventLogPart(name=name, size=len(body))
                    for name, body in self.parts.items()
                ],
                total_bytes=sum(len(b) for b in self.parts.values()),
            ),
            type=STS_EVENTLOG_INFO,
            source="sts",
        )

    def _read(self, envelope):  # noqa: ANN001
        req = StsEventLogReadRequest.model_validate(envelope.payload)
        body = self.parts.get(req.part)
        if body is None:
            return RpcErrorEnvelope.wrap(
                RpcError(code="not_found", message=req.part),
                type=STS_ERROR,
                source="sts",
            )
        self.reads.append((req.part, req.offset, req.length))
        raw = body[req.offset : req.offset + req.length]
        return StsEventLogChunkEnvelope.wrap(
            StsEventLogChunk(
                session_id=req.session_id,
                part=req.part,
                offset=req.offset,
                data=base64.b64encode(gzip.compress(raw)).decode("ascii"),
                raw_bytes=len(raw),
                eof=len(raw) < req.length,
            ),
            type=STS_EVENTLOG_READ,
            source="sts",
        )


@pytest.fixture(autouse=True)
def no_audit(monkeypatch):
    async def _audit(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(sts_routes, "record_audit", _audit)


async def _collect(response) -> bytes:  # noqa: ANN001
    return b"".join([chunk async for chunk in response.body_iterator])


async def test_download_walks_every_part_in_order(monkeypatch) -> None:
    monkeypatch.setattr(sts_routes, "_EVENTLOG_CHUNK_BYTES", 8)
    parts = {
        "s1.jsonl.1": b'{"seq":1}\n{"seq":2}\n',
        "s1.jsonl": b'{"seq":3}\n',
    }
    sts = FakeSts(parts)

    response = await sts_routes.download_eventlog("s1", sts)
    body = await _collect(response)

    # One gzip out, whatever it took to fetch it.
    assert gzip.decompress(body) == b"".join(parts.values())
    assert response.media_type == "application/gzip"
    assert 's1.jsonl.gz"' in response.headers["content-disposition"]
    # Offsets advance by what was actually read, part by part.
    assert sts.reads == [
        ("s1.jsonl.1", 0, 8),
        ("s1.jsonl.1", 8, 8),
        ("s1.jsonl.1", 16, 8),
        ("s1.jsonl", 0, 8),
        ("s1.jsonl", 8, 8),
    ]


async def test_an_empty_part_does_not_stall_the_walk() -> None:
    """A rotation that exists but is empty is the degenerate paging case."""
    sts = FakeSts({"s1.jsonl.1": b"", "s1.jsonl": b'{"seq":1}\n'})

    body = await _collect(await sts_routes.download_eventlog("s1", sts))

    assert gzip.decompress(body) == b'{"seq":1}\n'


async def test_no_log_for_the_session_is_a_404_that_says_why() -> None:
    sts = FakeSts({}, enabled=True)

    with pytest.raises(HTTPException) as caught:
        await sts_routes.download_eventlog("s1", sts)

    assert caught.value.status_code == 404
    assert "no event log for session" in caught.value.detail


async def test_logging_switched_off_says_that_instead() -> None:
    """An operator who never set the env var needs a different sentence."""
    sts = FakeSts({}, enabled=False)

    with pytest.raises(HTTPException) as caught:
        await sts_routes.download_eventlog("s1", sts)

    assert caught.value.status_code == 404
    assert "STS_EVENTLOG_DIR" in caught.value.detail


async def test_info_reports_size_without_transferring_it() -> None:
    sts = FakeSts({"s1.jsonl.1": b"a" * 10, "s1.jsonl": b"b" * 5})

    info = await sts_routes.eventlog_info("s1", sts)

    assert info.available is True
    assert info.parts == 2
    assert info.total_bytes == 15
    assert sts.reads == []


async def test_a_failure_mid_stream_ends_the_download(monkeypatch) -> None:
    """Headers are long gone, so the only honest end is a short file."""
    monkeypatch.setattr(sts_routes, "_EVENTLOG_CHUNK_BYTES", 8)
    sts = FakeSts({"s1.jsonl": b"x" * 100})

    response = await sts_routes.download_eventlog("s1", sts)
    # The part disappears after the info call — an STS restart mid-download.
    sts.parts = {}
    body = await _collect(response)

    assert body == b""
