"""The procman-report store and ``GET /workers`` (B3-07).

The store is fed decoded envelopes. No NATS connection. The route is
the API's test client over ASGI, aimed at a store this test owns.
"""

from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI
from mftik.clock import FakeClock
from mftik.protocol import (
    PROCMAN_REPORT,
    PROTOCOL_VERSION,
    ProcmanReport,
    ProcmanReportEnvelope,
    ProcmanWorker,
    Topics,
    UntypedEnvelope,
)
from mftik_api.procman_reports import ProcmanReportStore
from mftik_api.routes import workers as workers_route


def _worker(worker_id: str, code_ref: str, *, incarnation: int = 1) -> ProcmanWorker:
    return ProcmanWorker(
        id=worker_id,
        code_ref=code_ref,
        rss_bytes=128,
        phase="running",
        ready=True,
        incarnation=incarnation,
    )


def _envelope(
    workers: list[ProcmanWorker],
    *,
    generation: int = 1,
    pv: int = PROTOCOL_VERSION,
    type_name: str = PROCMAN_REPORT,
) -> UntypedEnvelope:
    report = ProcmanReport(generation=generation, workers=workers)
    wrapped = ProcmanReportEnvelope.wrap(
        report, type=type_name, source="md"
    )
    body = wrapped.model_dump()
    body["pv"] = pv
    return UntypedEnvelope.model_validate(body)


def test_a_report_lists_every_worker_and_silence_keeps_it() -> None:
    clock = FakeClock()
    store = ProcmanReportStore(clock=clock)
    assert store.note(
        Topics.procman_report("md", "md-1"),
        _envelope(
            [_worker("md/conn/b", "1.4.0"), _worker("md/conn/a", "1.5.0")]
        ),
    )
    assert store.note(
        Topics.procman_report("td", "td-1"),
        _envelope([_worker("td/account/7", "1.4.0")]),
    )
    rows = store.rows()
    assert [(row.plane, row.instance, row.id, row.code_ref) for row in rows] == [
        ("md", "md-1", "md/conn/a", "1.5.0"),
        ("md", "md-1", "md/conn/b", "1.4.0"),
        ("td", "td-1", "td/account/7", "1.4.0"),
    ]
    assert {row.age_s for row in rows} == {0.0}
    clock.advance(4.0)
    aged = store.rows()
    assert [row.id for row in aged] == [row.id for row in rows]
    assert {row.age_s for row in aged} == {4.0}
    assert store.held_instances() == (("md", "md-1"), ("td", "td-1"))


def test_a_later_report_replaces_that_instance_only() -> None:
    """A new process starts ``generation`` at 1. Arrival order wins."""
    clock = FakeClock()
    store = ProcmanReportStore(clock=clock)
    subject = Topics.procman_report("sts", "sts-jp")
    store.note(subject, _envelope([_worker("sts/session/old", "v1")], generation=9))
    clock.advance(2.0)
    store.note(
        subject,
        _envelope([_worker("sts/session/new", "v2")], generation=1),
    )
    rows = store.rows()
    assert [row.id for row in rows] == ["sts/session/new"]
    assert rows[0].code_ref == "v2"
    assert rows[0].age_s == 0.0
    assert rows[0].incarnation == 1
    assert rows[0].phase == "running"
    assert rows[0].ready is True
    assert rows[0].rss_bytes == 128


def test_an_empty_report_is_an_observation_and_silence_is_not() -> None:
    clock = FakeClock()
    store = ProcmanReportStore(clock=clock)
    subject = Topics.procman_report("md", "md-1")
    store.note(subject, _envelope([_worker("md/conn/a", "1.4.0")]))
    store.note(subject, _envelope([]))
    assert store.rows() == []
    assert store.held_instances() == (("md", "md-1"),)
    clock.advance(30.0)
    assert store.rows() == []
    assert store.held_instances() == (("md", "md-1"),)


def test_a_bad_frame_leaves_the_previous_report() -> None:
    clock = FakeClock()
    store = ProcmanReportStore(clock=clock)
    subject = Topics.procman_report("md", "md-1")
    store.note(subject, _envelope([_worker("md/conn/a", "1.4.0")]))
    assert not store.note("procman.report.md", _envelope([]))
    assert not store.note(subject, _envelope([], pv=1))
    assert not store.note(subject, _envelope([], type_name="other"))
    broken = _envelope([_worker("md/conn/a", "1.4.0")])
    payload = dict(broken.payload)
    payload["workers"] = [{"id": "md/conn/a"}]
    bad = broken.model_copy(update={"payload": payload})
    assert not store.note(subject, bad)
    assert [row.id for row in store.rows()] == ["md/conn/a"]


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    clock = FakeClock()
    store = ProcmanReportStore(clock=clock)
    monkeypatch.setattr(workers_route, "report_store", lambda: store)
    app = FastAPI()
    app.include_router(workers_route.router)
    transport = httpx.ASGITransport(app=app)
    return clock, store, httpx.AsyncClient(transport=transport, base_url="http://mftik.test")


async def test_get_workers_returns_each_row_and_keeps_a_quiet_instance(
    client,
) -> None:
    clock, store, http = client
    store.note(
        Topics.procman_report("td", "td-1"),
        _envelope(
            [
                ProcmanWorker(
                    id="td/account/7",
                    code_ref="1.4.0",
                    rss_bytes=None,
                    phase="starting",
                    ready=False,
                    incarnation=3,
                )
            ]
        ),
    )
    async with http:
        first = await http.get("/workers")
        assert first.status_code == 200
        body = first.json()
        assert body["workers"] == [
            {
                "plane": "td",
                "instance": "td-1",
                "id": "td/account/7",
                "incarnation": 3,
                "phase": "starting",
                "ready": False,
                "code_ref": "1.4.0",
                "rss_bytes": None,
                "age_s": 0.0,
            }
        ]
        clock.advance(6.25)
        second = await http.get("/workers")
    assert second.status_code == 200
    row = second.json()["workers"][0]
    assert row["code_ref"] == "1.4.0"
    assert row["age_s"] == 6.25
    assert row["id"] == "td/account/7"


async def test_get_workers_is_empty_before_any_report(client) -> None:
    _clock, _store, http = client
    async with http:
        response = await http.get("/workers")
    assert response.status_code == 200
    assert response.json() == {"workers": []}
