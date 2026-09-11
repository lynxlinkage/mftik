"""Orphan reaper: named-instance rows that have no live link locally.

A row is an orphan when it names this instance and this process does not
have the link — or the lease tasks have died. Another instance's rows are
left for that instance. A first scan only strikes; a second scan closes.
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
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    MdAttachRequest,
    Topics,
)
from mftik_md.session import PaperPublicFactory, SessionManager

FEED = Topics.md_feed("orderbook", UniversalTicker.parse("Paper_Spot_BTCUSDT"))


@dataclass
class FakeMdStore:
    rows: dict[tuple[str, str], SimpleNamespace] = field(default_factory=dict)

    def seed_live(
        self,
        session_id: str,
        venue: str = "Bybit",
        *,
        instance: str = "md",
    ) -> SimpleNamespace:
        row = SimpleNamespace(
            venue=venue,
            session_id=session_id,
            created_by=1,
            created_at=datetime.now(UTC),
            finished_at=None,
            status="live",
            instance=instance,
        )
        self.rows[(venue, session_id)] = row
        return row

    async def persist_live(
        self,
        *,
        session_id: str,
        created_by: int,
        venues: list[str] | None = None,
        instance: str = "md",
    ) -> list[SimpleNamespace]:
        return [
            self.seed_live(session_id, venue, instance=instance)
            for venue in venues or []
        ]

    async def mark_done(
        self, *, session_id: str, instance: str = "md"
    ) -> list[SimpleNamespace]:
        done: list[SimpleNamespace] = []
        for row in self.rows.values():
            if row.session_id != session_id or row.status != "live":
                continue
            if getattr(row, "instance", instance) != instance:
                continue
            row.status = "done"
            row.finished_at = datetime.now(UTC)
            done.append(row)
        return done

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


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-md") as client:
        yield client


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=1,
        volatility_bps=0,
    ) as ex:
        yield ex


def _manager(
    broker: Broker, paper: PaperExchange, store: FakeMdStore
) -> SessionManager:
    return SessionManager(
        PaperPublicFactory(broker, paper),
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
    topic = Topics.sts_md_session(session_id)
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
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except TimeoutError:
            continue


async def _attached(
    broker: Broker, sessions: SessionManager, session_id: str
) -> tuple[asyncio.Task, asyncio.Event]:
    """Attach ``session_id`` with an STS lease running behind it."""
    stop = asyncio.Event()
    task = asyncio.create_task(_lease_publisher(broker, session_id, stop))
    await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[FEED],
            timeout=3.0,
        )
    )
    return task, stop


@pytest.mark.asyncio
async def test_a_ghost_row_is_closed_after_two_scans(
    broker: Broker, paper: PaperExchange
) -> None:
    """`done`, not `interrupted`: an md row follows its strategy session
    rather than carrying an outcome of its own."""
    store = FakeMdStore()
    store.seed_live("ghost-1")
    sessions = _manager(broker, paper, store)

    assert await sessions.reap_orphans() == []
    assert await sessions.reap_orphans() == ["ghost-1"]
    row = store.rows[("Bybit", "ghost-1")]
    assert row.status == "done"
    assert row.finished_at is not None


@pytest.mark.asyncio
async def test_every_venue_row_of_a_reaped_session_is_closed(
    broker: Broker, paper: PaperExchange
) -> None:
    """A session holds one row per venue, and the id is reported once."""
    store = FakeMdStore()
    store.seed_live("ghost-2", "Bybit")
    store.seed_live("ghost-2", "Gate")
    sessions = _manager(broker, paper, store)

    assert await sessions.reap_orphans() == []
    assert await sessions.reap_orphans() == ["ghost-2"]
    assert {row.status for row in store.rows.values()} == {"done"}


@pytest.mark.asyncio
async def test_an_attach_this_process_holds_is_left_alone(
    broker: Broker, paper: PaperExchange
) -> None:
    store = FakeMdStore()
    sessions = _manager(broker, paper, store)
    task, stop = await _attached(broker, sessions, "mine-1")

    assert await sessions.reap_orphans() == []
    assert await sessions.reap_orphans() == []
    assert store.rows[("Paper", "mine-1")].status == "live"

    stop.set()
    await sessions.detach(session_id="mine-1", reason="test")
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_a_link_whose_lease_loop_stopped_is_torn_down(
    broker: Broker, paper: PaperExchange
) -> None:
    """A link this process holds is not exempt from the scan.

    The lease tasks are the only sign the attach is still fenced. A link
    whose loop has stopped is holding a venue feed open for a session
    nobody is leasing.
    """
    store = FakeMdStore()
    sessions = _manager(broker, paper, store)
    task, stop = await _attached(broker, sessions, "dead-loop-1")

    link = sessions._links["dead-loop-1"]  # noqa: SLF001
    link.stop.set()
    await asyncio.gather(*link.tasks, return_exceptions=True)

    assert await sessions.reap_orphans() == []
    assert await sessions.reap_orphans() == ["dead-loop-1"]

    assert store.rows[("Paper", "dead-loop-1")].status == "done"
    assert "dead-loop-1" not in sessions._links  # noqa: SLF001
    assert sessions.feed_refcount(FEED) == 0

    stop.set()
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_a_healthy_link_survives_a_scan(
    broker: Broker, paper: PaperExchange
) -> None:
    store = FakeMdStore()
    sessions = _manager(broker, paper, store)
    task, stop = await _attached(broker, sessions, "blip-1")

    assert await sessions.reap_orphans() == []
    assert await sessions.reap_orphans() == []
    assert store.rows[("Paper", "blip-1")].status == "live"

    stop.set()
    await sessions.detach(session_id="blip-1", reason="test")
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_a_row_for_another_instance_is_left_alone(
    broker: Broker, paper: PaperExchange
) -> None:
    store = FakeMdStore()
    store.seed_live("theirs-1", instance="md-jp")
    sessions = _manager(broker, paper, store)

    assert await sessions.reap_orphans() == []
    assert await sessions.reap_orphans() == []
    assert store.rows[("Bybit", "theirs-1")].status == "live"


@pytest.mark.asyncio
async def test_a_live_sts_session_does_not_protect_a_dead_attach(
    broker: Broker, paper: PaperExchange
) -> None:
    """MD is reaped on its own local map, not on the strategy still running."""
    store = FakeMdStore()
    store.seed_live("sts-alive-1")
    sessions = _manager(broker, paper, store)

    assert await sessions.reap_orphans() == []
    assert await sessions.reap_orphans() == ["sts-alive-1"]
    assert store.rows[("Bybit", "sts-alive-1")].status == "done"


@pytest.mark.asyncio
async def test_the_link_is_registered_before_the_row_exists(
    broker: Broker, paper: PaperExchange
) -> None:
    """Otherwise a reaper could see a live row with no local link."""
    store = FakeMdStore()
    sessions = _manager(broker, paper, store)
    seen: list[bool] = []

    async def watching_persist(**kwargs):
        seen.append(kwargs["session_id"] in sessions._links)  # noqa: SLF001
        return await store.persist_live(**kwargs)

    sessions._persist_live = watching_persist  # noqa: SLF001
    task, stop = await _attached(broker, sessions, "order-1")

    assert seen == [True]

    stop.set()
    await sessions.detach(session_id="order-1", reason="test")
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_a_failing_list_reaps_nothing(
    broker: Broker, paper: PaperExchange
) -> None:
    store = FakeMdStore()
    store.seed_live("dbdown-1")
    sessions = _manager(broker, paper, store)

    async def broken_list(**_kwargs):
        raise RuntimeError("db down")

    sessions._list_db_sessions = broken_list  # noqa: SLF001
    assert await sessions.reap_orphans() == []
    assert store.rows[("Bybit", "dbdown-1")].status == "live"
