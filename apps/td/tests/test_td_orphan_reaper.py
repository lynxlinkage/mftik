"""Closing attaches this instance owns and does not hold locally.

A live link's STS death is the session heartbeat. This scan is the
restart case: rows left in our name with no local link, after strikes
so a row between two attaches is not reaped on the first look.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    TdAttachRequest,
    Topics,
)
from mftik_td.session import PaperSessionFactory, SessionManager

API_ID = 3


@dataclass
class FakeStore:
    rows: dict[tuple[str, int], SimpleNamespace] = field(default_factory=dict)

    def seed_live(self, session_id: str, api_id: int = API_ID) -> SimpleNamespace:
        row = SimpleNamespace(
            session_id=session_id,
            created_by=1,
            created_at=datetime.now(UTC),
            finished_at=None,
            status="live",
            api_id=api_id,
        )
        self.rows[(session_id, api_id)] = row
        return row

    async def persist_live(
        self, *, session_id: str, created_by: int, api_id: int
    ) -> SimpleNamespace:
        row = self.rows.get((session_id, api_id))
        if row is not None:
            row.status = "live"
            row.finished_at = None
            return row
        return self.seed_live(session_id, api_id)

    async def mark_done(
        self, *, session_id: str, api_id: int
    ) -> SimpleNamespace | None:
        row = self.rows.get((session_id, api_id))
        if row is None or row.status != "live":
            return None
        row.status = "done"
        row.finished_at = datetime.now(UTC)
        return row

    async def list_sessions(
        self,
        *,
        status: str | None = "live",
        created_by: int | None = None,
        limit: int = 100,
    ) -> list[SimpleNamespace]:
        out = [
            row
            for row in self.rows.values()
            if (status is None or row.status == status)
            and (created_by is None or row.created_by == created_by)
        ]
        return out[:limit]

    def status(self, session_id: str, api_id: int = API_ID) -> str | None:
        row = self.rows.get((session_id, api_id))
        return None if row is None else row.status


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-td-reap") as client:
        yield client


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=5,
        volatility_bps=0,
    ) as ex:
        yield ex


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def sessions(
    broker: Broker, paper: PaperExchange, store: FakeStore
) -> SessionManager:
    factory = PaperSessionFactory(broker, paper)
    factory.bind_api(API_ID, api_key="key-3", api_secret="sec-3")
    return SessionManager(
        factory,
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        list_db_sessions=store.list_sessions,
        lease_grace=2.0,
    )


async def _lease_publisher(
    broker: Broker, session_id: str, stop: asyncio.Event
) -> None:
    token = 0
    topic = Topics.sts_td_session(session_id)
    while not stop.is_set():
        token += 1
        await broker.publish(
            topic,
            Envelope[LeaseHeartbeat].wrap(
                LeaseHeartbeat(session_id=session_id, token=token),
                type=STS_LEASE_HEARTBEAT,
                source="sts",
                session_id=session_id,
            ),
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.05)
        except TimeoutError:
            continue


async def _attached(
    broker: Broker, sessions: SessionManager, session_id: str
) -> tuple[asyncio.Task, asyncio.Event]:
    """Attach ``session_id`` the way a running STS session would."""
    stop = asyncio.Event()
    task = asyncio.create_task(_lease_publisher(broker, session_id, stop))
    await sessions.attach(
        TdAttachRequest(
            api_id=API_ID, session_id=session_id, created_by=1, timeout=5.0
        )
    )
    return task, stop


@pytest.mark.asyncio
async def test_a_link_whose_lease_loop_died_is_detached(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    task, stop = await _attached(broker, sessions, "gone-1")
    link = sessions._accounts[API_ID].links["gone-1"]  # noqa: SLF001
    link.stop.set()
    await asyncio.gather(*link.tasks, return_exceptions=True)

    assert await sessions.reap_orphans() == []
    assert store.status("gone-1") == "live"

    assert await sessions.reap_orphans() == [("gone-1", API_ID)]
    assert store.status("gone-1") == "done"
    assert sessions.get(API_ID) is None

    stop.set()
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_a_running_strategy_keeps_its_link(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    task, stop = await _attached(broker, sessions, "live-1")

    assert await sessions.reap_orphans() == []
    assert await sessions.reap_orphans() == []
    assert store.status("live-1") == "live"
    assert sessions.get(API_ID) is not None

    stop.set()
    await sessions.detach(session_id="live-1", api_id=API_ID, reason="test")
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_a_revived_lease_loop_clears_the_strikes(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """One dead-task scan is a suspicion. A live task afterwards starts over."""
    task, stop = await _attached(broker, sessions, "blip-1")
    link = sessions._accounts[API_ID].links["blip-1"]  # noqa: SLF001
    link.stop.set()
    await asyncio.gather(*link.tasks, return_exceptions=True)

    assert await sessions.reap_orphans() == []
    link.tasks = [asyncio.create_task(asyncio.sleep(60))]
    assert await sessions.reap_orphans() == []
    link.stop.set()
    await asyncio.gather(*link.tasks, return_exceptions=True)
    assert await sessions.reap_orphans() == []

    assert store.status("blip-1") == "live"
    assert sessions.get(API_ID) is not None

    stop.set()
    await sessions.detach(session_id="blip-1", api_id=API_ID, reason="test")
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_a_row_left_by_a_previous_process_is_closed_after_two_scans(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """A restart clears the links but not the table. Strikes still apply."""
    store.seed_live("ghost-1")

    assert await sessions.reap_orphans() == []
    assert await sessions.reap_orphans() == [("ghost-1", API_ID)]
    assert store.status("ghost-1") == "done"


@pytest.mark.asyncio
async def test_a_row_for_another_td_is_left_alone(
    broker: Broker, paper: PaperExchange, store: FakeStore
) -> None:
    factory = PaperSessionFactory(broker, paper)
    factory.bind_api(API_ID, api_key="key-3", api_secret="sec-3")

    async def other_td(_api_id: int) -> str:
        return "td-jp"

    sessions = SessionManager(
        factory,
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        list_db_sessions=store.list_sessions,
        lease_grace=2.0,
        instance="td-tw",
        td_instance=other_td,
    )
    store.seed_live("theirs-1")

    assert await sessions.reap_orphans() == []
    assert await sessions.reap_orphans() == []
    assert store.status("theirs-1") == "live"


@pytest.mark.asyncio
async def test_a_failing_list_reaps_nothing(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    store.seed_live("dbdown-1")

    async def broken_list(**_kwargs):
        raise RuntimeError("db down")

    sessions._list_db_sessions = broken_list  # noqa: SLF001
    assert await sessions.reap_orphans() == []
    assert store.status("dbdown-1") == "live"
