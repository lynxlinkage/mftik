"""One real shim. Start reaches running, end reaches done, detach adopts.

Each test stays under the integration cap. The worker is a stand-in: it
reads the request file, heartbeats ready, and exits 0 on SIGTERM. The
session worker itself is B4-03.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest
from db_harness import a_database, an_owner
from mftik.procman import CloseMode, Supervisor, WorkerPhase
from mftik.protocol import (
    STS_REASON_OPERATOR_STOP,
    STS_SESSION_END,
    STS_SESSION_START,
    Envelope,
    StsCreateSessionRequest,
    StsSessionEndRequest,
    StsSessionStatus,
)
from mftik_db.repositories.session import StsSessionRepository
from mftik_sts.controller import (
    StsOrchestrator,
    end_handler,
    session_worker_id,
    start_handler,
)
from mftik_sts.controller.status import DbStatusStore

pytestmark = pytest.mark.integration

_STAND_IN = """
import os, signal, sys, time
def _term(signum, frame):
    raise SystemExit(0)
signal.signal(signal.SIGTERM, _term)
open(sys.argv[1], encoding="utf-8").read()
fd = int(os.environ["MFTIK_STATUS_FD"])
while True:
    os.write(fd, b'{"ready":true}\\n')
    time.sleep(0.1)
"""


def _script(tmp_path: Path) -> Path:
    path = tmp_path / "stand_in.py"
    path.write_text(_STAND_IN)
    return path


def _argv(script: Path, spawned: list[Path]):
    def argv_for(path: Path) -> tuple[str, ...]:
        spawned.append(path)
        return (sys.executable, str(script), str(path))

    return argv_for


async def _until(check, *, seconds: float = 5.0) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if await check():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("timed out")


def _start_message() -> Envelope[dict]:
    body = StsCreateSessionRequest(
        session_id="abc123", created_by=1, strategy="noop"
    )
    return Envelope[dict].wrap(
        body.model_dump(), type=STS_SESSION_START, source="api"
    )


def _end_message() -> Envelope[dict]:
    body = StsSessionEndRequest(
        session_id="abc123", reason=STS_REASON_OPERATOR_STOP
    )
    return Envelope[dict].wrap(body.model_dump(), type=STS_SESSION_END, source="api")


async def test_start_reaches_running_and_end_reaches_done(tmp_path: Path) -> None:
    script = _script(tmp_path)
    spawned: list[Path] = []
    published: list[tuple[str, Envelope]] = []
    work = tmp_path / "supervisor"

    async def _publish(subject: str, envelope: Envelope) -> None:
        published.append((subject, envelope))

    async with a_database() as database:
        async with database.scope() as session:
            await an_owner(session)
            await StsSessionRepository(session).create_live(
                session_id="abc123",
                created_by=1,
                type="noop",
                restart="never",
            )
        supervisor = Supervisor(work, plane="sts", instance="sts")
        orch = StsOrchestrator(
            supervisor,
            store=DbStatusStore(database.scope),
            publish=_publish,
            code_ref="test",
            argv_for=_argv(script, spawned),
        )
        try:
            await orch.boot()
            assert await start_handler(orch)(_start_message()) is not None
            await orch.converge("abc123")

            async def _running() -> bool:
                await orch.observe_all()
                view = await supervisor.status(session_worker_id("abc123"))
                return (
                    view is not None
                    and view.phase is WorkerPhase.RUNNING
                    and view.ready
                )

            await _until(_running)
            async with database.scope() as session:
                row = await StsSessionRepository(session).get_by_session_id("abc123")
            assert row is not None
            assert row.status == "live"
            assert row.conditions["phase"] == "running"
            snapshot = StsSessionStatus.model_validate(published[-1][1].payload)
            assert snapshot.status == "running"
            assert len(spawned) == 1

            reply = await end_handler(orch)(_end_message())
            assert reply is not None
            async with database.scope() as session:
                done = await StsSessionRepository(session).get_by_session_id(
                    "abc123"
                )
            assert done is not None
            assert done.status == "done"
            assert done.finished_at is not None
            terminal = StsSessionStatus.model_validate(published[-1][1].payload)
            assert terminal.status == "done"
        finally:
            await supervisor.close(CloseMode.STOP)


async def test_a_new_controller_adopts_the_detached_worker(tmp_path: Path) -> None:
    script = _script(tmp_path)
    spawned: list[Path] = []
    work = tmp_path / "supervisor"
    first = Supervisor(work, plane="sts", instance="sts")
    second: Supervisor | None = None
    stage = "first"

    async with a_database() as database:
        async with database.scope() as session:
            await an_owner(session)
            await StsSessionRepository(session).create_live(
                session_id="abc123",
                created_by=1,
                type="noop",
                restart="never",
            )
        store = DbStatusStore(database.scope)
        orch = StsOrchestrator(
            first,
            store=store,
            code_ref="test",
            argv_for=_argv(script, spawned),
        )
        try:
            await orch.boot()
            assert await start_handler(orch)(_start_message()) is not None
            await orch.converge("abc123")

            async def _running() -> bool:
                await orch.observe_all()
                view = await first.status(session_worker_id("abc123"))
                return (
                    view is not None
                    and view.phase is WorkerPhase.RUNNING
                    and view.pid is not None
                )

            await _until(_running)
            held = await first.status(session_worker_id("abc123"))
            assert held is not None and held.pid is not None
            pid = held.pid
            await first.close(CloseMode.DETACH)
            stage = "detached"
            second = Supervisor(work, plane="sts", instance="sts")
            adopted = StsOrchestrator(
                second,
                store=store,
                code_ref="test",
                argv_for=_argv(script, spawned),
            )
            await adopted.boot()
            stage = "second"
            view = await second.status(session_worker_id("abc123"))
            assert view is not None
            assert view.pid == pid
            assert view.phase in (WorkerPhase.RUNNING, WorkerPhase.STARTING)
            assert len(spawned) == 1
        finally:
            if stage == "second" and second is not None:
                await second.close(CloseMode.STOP)
            elif stage == "detached":
                cleanup = Supervisor(work, plane="sts", instance="sts")
                await cleanup.start()
                await cleanup.close(CloseMode.STOP)
            else:
                await first.close(CloseMode.STOP)
