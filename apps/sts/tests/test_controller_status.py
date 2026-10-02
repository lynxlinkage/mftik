"""Status columns and the snapshot, on the database fixture.

sqlite is component. A postgres URL, when set, is integration. The
supervisor is a fake: this tier does not start a process.
"""

from __future__ import annotations

from pathlib import Path

from db_harness import a_database, an_owner
from mftik.clock import FakeClock
from mftik.protocol import (
    STS_REASON_OPERATOR_STOP,
    STS_SESSION_END,
    Envelope,
    StsSessionEndRequest,
    StsSessionStatus,
)
from mftik_db.repositories.session import StsSessionRepository
from mftik_sts.controller import StsOrchestrator, end_handler, start_handler
from mftik_sts.controller.status import DbStatusStore
from test_controller_flow import FakeSupervisor, _model, _request


async def test_running_then_done_is_written_on_the_row(
    database_url: str, tmp_path: Path
) -> None:
    clock = FakeClock()
    published: list[tuple[str, Envelope]] = []

    async def _publish(subject: str, envelope: Envelope) -> None:
        published.append((subject, envelope))

    async with a_database(database_url) as database:
        async with database.scope() as session:
            await an_owner(session)
            await StsSessionRepository(session).create_live(
                session_id="abc123",
                created_by=1,
                type="noop",
                restart="never",
                instance=None,
            )
        supervisor = FakeSupervisor(tmp_path)
        supervisor.ready_on_spawn = True
        orch = StsOrchestrator(
            supervisor,  # type: ignore[arg-type]
            clock=clock,
            store=DbStatusStore(database.scope),
            publish=_publish,
            code_ref="test",
            argv_for=lambda path: ("stand-in", str(path)),
        )
        assert await start_handler(orch)(_request()) is not None
        await orch.converge("abc123")

        async with database.scope() as session:
            row = await StsSessionRepository(session).get_by_session_id("abc123")
        assert row is not None
        assert row.status == "live"
        assert row.conditions["phase"] == "running"
        assert row.observed_generation == 1
        assert row.worker_incarnation == 1
        assert row.restart_count == 0
        assert _model(published[-1][1].payload, StsSessionStatus).status == "running"

        clock.advance(5)
        reply = await end_handler(orch)(
            Envelope[dict].wrap(
                StsSessionEndRequest(
                    session_id="abc123", reason=STS_REASON_OPERATOR_STOP
                ).model_dump(),
                type=STS_SESSION_END,
                source="api",
            )
        )
        assert reply is not None
        async with database.scope() as session:
            done = await StsSessionRepository(session).get_by_session_id("abc123")
        assert done is not None
        assert done.status == "done"
        assert done.conditions["phase"] == "done"
        assert done.reason == STS_REASON_OPERATOR_STOP
        assert done.finished_at is not None
        terminal = _model(published[-1][1].payload, StsSessionStatus)
        assert terminal.status == "done"
        assert terminal.finished_at == clock.now()
