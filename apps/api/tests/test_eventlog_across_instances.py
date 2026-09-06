"""A session's event log can live on two disks, and the download has both.

The file is written by whichever STS ran the session, and one session can be
run by two of them: a rebuild elsewhere leaves the earlier parts on the volume
of the process that died. So there is no single right instance to ask —
asking one reports a prefix as the whole story, and asking whichever answered
reports somebody else's silence as "no log".

This is why the event log could not be addressed the way stop and fail are.
Those need the process *holding* a session, and a finished session has no
holder; this needs the disk, and a finished session's disk is still there.
"""

from __future__ import annotations

import base64
import gzip

import pytest
from db_harness import a_database, an_instance, an_owner
from mftik.protocol import (
    STS_EVENTLOG_INFO,
    STS_EVENTLOG_READ,
    StsEventLogChunk,
    StsEventLogChunkEnvelope,
    StsEventLogInfo,
    StsEventLogInfoEnvelope,
    StsEventLogInfoRequest,
    StsEventLogPart,
    StsEventLogReadRequest,
    Topics,
)
from mftik_api.routes import sts as sts_routes

ONE = "sts-1"
TWO = "sts-2"
SESSION = "split-log"

#: What each instance has on its disk. Both wrote a file with the same name —
#: every process writes ``{session}.jsonl`` — and only the mtime says which
#: came first.
DISKS: dict[str, dict[str, tuple[bytes, float]]] = {
    ONE: {"split-log.jsonl": (b"older-from-one\n", 100.0)},
    TWO: {"split-log.jsonl": (b"newer-from-two\n", 200.0)},
}


class TwoInstances:
    """A broker where each named STS answers only for its own disk."""

    def __init__(self, *, down: set[str] | None = None) -> None:
        self.down = down or set()
        #: (instance, part, offset) of every read, in arrival order.
        self.reads: list[tuple[str, str, int]] = []

    def _instance_of(self, subject: str) -> str:
        return subject.split(".", 1)[1]

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        instance = self._instance_of(subject)
        if instance in self.down:
            from mftik_api.broker_rpc import DomainRpcError

            raise DomainRpcError("timeout", f"{instance} did not answer")
        disk = DISKS.get(instance, {})
        if envelope.type == STS_EVENTLOG_INFO:
            req = StsEventLogInfoRequest.model_validate(envelope.payload)
            return StsEventLogInfoEnvelope.wrap(
                StsEventLogInfo(
                    session_id=req.session_id,
                    available=bool(disk),
                    enabled=True,
                    parts=[
                        StsEventLogPart(
                            name=name,
                            size=len(body),
                            modified=mtime,
                            instance=instance,
                        )
                        for name, (body, mtime) in disk.items()
                    ],
                    total_bytes=sum(len(b) for b, _ in disk.values()),
                ),
                type=STS_EVENTLOG_INFO,
                source="sts",
            )
        req = StsEventLogReadRequest.model_validate(envelope.payload)
        body, _ = disk[req.part]
        self.reads.append((instance, req.part, req.offset))
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


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            await an_instance(session, ONE, "sts")
            await an_instance(session, TWO, "sts")
            await session.commit()
        monkeypatch.setattr(sts_routes, "session_scope", database.scope)
        yield database.scope


async def test_the_listing_has_both_disks_oldest_first(db) -> None:
    broker = TwoInstances()

    info = await sts_routes._eventlog_info(broker, SESSION)

    assert info.available is True
    assert [p.instance for p in info.parts] == [ONE, TWO], (
        "ordered by when they were written, across instances"
    )
    assert info.total_bytes == sum(
        len(b) for disk in DISKS.values() for b, _ in disk.values()
    )


async def test_each_part_is_read_from_the_disk_that_has_it(db) -> None:
    """Names collide, so the instance is the only thing that disambiguates.

    Both files are called ``split-log.jsonl``. A read sent to whichever
    answered would return one instance's bytes twice under the right name.
    """
    broker = TwoInstances()
    info = await sts_routes._eventlog_info(broker, SESSION)

    stream = b"".join(
        [
            chunk
            async for chunk in sts_routes._eventlog_chunks(
                broker, SESSION, info
            )
        ]
    )
    # The download is a ``.gz`` built from one member per chunk; multi-member
    # is what ``decompress`` and every gzip reader already handle.
    body = gzip.decompress(stream)

    assert [instance for instance, _, _ in broker.reads] == [ONE, TWO]
    assert body == b"older-from-one\nnewer-from-two\n", (
        "both disks, concatenated oldest first"
    )


async def test_one_instance_being_down_does_not_lose_the_other(db) -> None:
    """A log on the instance that answered is still worth returning."""
    broker = TwoInstances(down={ONE})

    info = await sts_routes._eventlog_info(broker, SESSION)

    assert info.available is True
    assert [p.instance for p in info.parts] == [TWO]


async def test_no_instance_answering_is_an_error_not_an_empty_log(db) -> None:
    """"We could not ask" and "there is no log" are different answers."""
    from fastapi import HTTPException

    broker = TwoInstances(down={ONE, TWO})

    with pytest.raises(HTTPException) as caught:
        await sts_routes._eventlog_info(broker, SESSION)

    assert caught.value.status_code == 502


async def test_a_part_with_no_instance_is_asked_of_the_shared_subject(
    db,
) -> None:
    """An STS that predates the field still answers reads for its parts."""
    part = StsEventLogPart(name="x.jsonl", size=1, modified=1.0)
    assert (
        Topics.STS
        if part.instance is None
        else Topics.sts(part.instance)
    ) == Topics.STS
