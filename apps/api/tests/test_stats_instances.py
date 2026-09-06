"""/stats — one row per declared instance, and *down* when it does not answer.

The row that matters is the one nothing answers for. A declared instance that
is silent is the only state a presence-free design can report and a
registry-free one would lose entirely: without the table it would simply
vanish from the page, with nothing anywhere remembering it should be there.
"""

from __future__ import annotations

import asyncio
import time

import pytest
from auth_harness import a_client
from db_harness import a_database, an_instance, an_owner
from fastapi import FastAPI
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import Envelope, HealthStatus, Topics, UntypedEnvelope
from mftik_api.routes import stats as stats_routes
from mftik_api.routes.stats import router as stats_router


class _Answering:
    """A broker where the named instances answer and the rest time out."""

    def __init__(self, *, up: dict[str, HealthStatus], delay: float = 0.0):
        self._up = up
        self._delay = delay
        self.probed: list[str] = []
        self.concurrent = 0
        self.peak = 0

    async def probe(self, subject, envelope, *, timeout=None) -> UntypedEnvelope:
        self.probed.append(subject)
        self.concurrent += 1
        self.peak = max(self.peak, self.concurrent)
        try:
            if self._delay:
                await asyncio.sleep(self._delay)
            status = self._up.get(subject)
            if status is None:
                raise RequestTimeoutError(subject, envelope.id, timeout or 0)
            return UntypedEnvelope.model_validate(
                Envelope[HealthStatus]
                .wrap(status, type="health", source="test")
                .model_dump()
            )
        finally:
            self.concurrent -= 1


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            await an_instance(session, "md-jp-1", "md")
            await an_instance(session, "md-jp-2", "md")
            await session.commit()
        monkeypatch.setattr(stats_routes, "session_scope", database.scope)
        monkeypatch.setenv("MFTIK_AUTH_ENABLED", "0")
        yield database.scope


def _app(broker) -> FastAPI:
    app = FastAPI()
    app.state.broker = broker
    app.include_router(stats_router)
    return app


def _ok(instance: str) -> HealthStatus:
    return HealthStatus(
        status="ok",
        service="md",
        instance=instance,
        domain="md",
        version="1.2.3",
        venues=["Bybit"],
    )


async def test_a_declared_instance_that_answers_is_connected(db) -> None:
    broker = _Answering(up={Topics.health("md", "md-jp-1"): _ok("md-jp-1")})
    async with a_client(_app(broker)) as client:
        res = await client.get("/stats")

    rows = {row["instance"]: row for row in res.json()["domains"]}
    assert rows["md-jp-1"]["state"] == "connected"
    assert rows["md-jp-1"]["healthy"] is True
    assert rows["md-jp-1"]["version"] == "1.2.3"
    assert rows["md-jp-1"]["venues"] == ["Bybit"]


async def test_a_declared_instance_that_is_silent_is_down_not_missing(
    db,
) -> None:
    """The whole reason the table exists.

    Without a declared row this instance would not appear at all, and an MD
    that was OOM-killed and never restarted would look exactly like one that
    was never deployed.
    """
    broker = _Answering(up={Topics.health("md", "md-jp-1"): _ok("md-jp-1")})
    async with a_client(_app(broker)) as client:
        res = await client.get("/stats")

    rows = {row["instance"]: row for row in res.json()["domains"]}
    assert set(rows) == {"md-jp-1", "md-jp-2"}
    assert rows["md-jp-2"]["state"] == "down"
    assert rows["md-jp-2"]["healthy"] is False
    assert rows["md-jp-2"]["version"] is None


async def test_every_declared_instance_is_probed_on_its_own_subject(db) -> None:
    broker = _Answering(up={})
    async with a_client(_app(broker)) as client:
        await client.get("/stats")

    assert sorted(broker.probed) == [
        Topics.health("md", "md-jp-1"),
        Topics.health("md", "md-jp-2"),
    ]


async def test_probes_run_concurrently_so_the_page_costs_one_timeout(
    db,
) -> None:
    """Serially this is the difference between a dashboard and a wait.

    Two down instances at 60ms each: concurrent is ~60ms, serial ~120ms. The
    assertion is on the overlap rather than the clock, because a timing
    threshold is the kind of test that fails on a loaded machine for no reason.
    """
    broker = _Answering(up={}, delay=0.06)
    started = time.monotonic()
    async with a_client(_app(broker)) as client:
        await client.get("/stats")
    elapsed = time.monotonic() - started

    assert broker.peak == 2, "both probes were in flight at once"
    assert elapsed < 0.12, "two 60ms probes did not run back to back"


async def test_session_counts_are_not_repeated_on_every_instance(db) -> None:
    """They belong to the plane, and the tables do not record a split.

    Showing the same numbers on both MD cards would claim each instance ran
    them, which is a statement nothing here can support.
    """
    broker = _Answering(up={})
    async with a_client(_app(broker)) as client:
        res = await client.get("/stats")

    md_rows = [r for r in res.json()["domains"] if r["domain"] == "md"]
    assert len(md_rows) == 2
    assert sum(1 for r in md_rows if r["live"] or r["done"]) <= 1
