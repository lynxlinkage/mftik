"""Two MDs holding one session must not corrupt each other's rows.

The scan is still global so a process can *see* a peer's rows, but it
decides only the rows that name this instance. A peer that died is
reaped by the next process of that same instance name, not by a neighbour.
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
from mftik.exchange.paper import PaperExchange
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    MdAttachRequest,
    Topics,
)
from mftik_md.session import PaperPublicFactory, SessionManager

ONE = "md-jp-1"
TWO = "md-jp-2"
SESSION = "shared-sts"
FEED = Topics.md_feed("orderbook", UniversalTicker.parse("Paper_Spot_BTCUSDT"))


@dataclass
class InstanceAwareStore:
    """What ``mftik_db`` does, in memory.

    The key is the triple, and ``mark_done`` takes an instance — the two facts
    the real repository now carries and the two this file exists to exercise.
    """

    rows: dict[tuple[str, str, str], SimpleNamespace] = field(
        default_factory=dict
    )

    def seed(
        self, instance: str, session_id: str, venue: str = "Paper"
    ) -> SimpleNamespace:
        row = SimpleNamespace(
            instance=instance,
            venue=venue,
            session_id=session_id,
            created_by=1,
            created_at=datetime.now(UTC),
            finished_at=None,
            status="live",
        )
        self.rows[(instance, venue, session_id)] = row
        return row

    async def persist_live(
        self,
        *,
        instance: str,
        session_id: str,
        created_by: int,
        venues: list[str] | None = None,
    ) -> list[SimpleNamespace]:
        return [self.seed(instance, session_id, v) for v in venues or []]

    async def mark_done(
        self, *, session_id: str, instance: str
    ) -> list[SimpleNamespace]:
        done: list[SimpleNamespace] = []
        for row in self.rows.values():
            if row.session_id != session_id or row.instance != instance:
                continue
            if row.status != "live":
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
        return [
            row
            for row in self.rows.values()
            if status is None or row.status == status
        ][:limit]

    def live(self, instance: str) -> list[str]:
        return sorted(
            row.session_id
            for row in self.rows.values()
            if row.instance == instance and row.status == "live"
        )


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("md-two") as client:
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


@pytest.fixture
def store() -> InstanceAwareStore:
    return InstanceAwareStore()


def _manager(
    broker: Broker,
    paper: PaperExchange,
    store: InstanceAwareStore,
    instance: str,
) -> SessionManager:
    return SessionManager(
        PaperPublicFactory(broker, paper),
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        list_db_sessions=store.list_sessions,
        lease_grace=5.0,
        instance=instance,
    )


async def _lease(broker: Broker, session_id: str, stop: asyncio.Event) -> None:
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
        await asyncio.sleep(0.05)


async def _both_attached(broker, paper, store):
    """Two MDs holding one STS session, as PI-3 allows."""
    one = _manager(broker, paper, store, ONE)
    two = _manager(broker, paper, store, TWO)
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease(broker, SESSION, stop))
    request = MdAttachRequest(
        session_id=SESSION, created_by=1, subscriptions=[FEED], timeout=5.0
    )
    await one.attach(request)
    await two.attach(request)
    return one, two, stop, pub


@pytest.mark.asyncio
async def test_each_instance_keeps_its_own_link_and_rows(
    broker, paper, store
) -> None:
    one, two, stop, pub = await _both_attached(broker, paper, store)
    try:
        assert SESSION in one._links  # noqa: SLF001
        assert SESSION in two._links  # noqa: SLF001
        assert store.live(ONE) == [SESSION]
        assert store.live(TWO) == [SESSION]
    finally:
        stop.set()
        await pub
        await one.close_all()
        await two.close_all()


@pytest.mark.asyncio
async def test_one_detaching_leaves_the_other_running(
    broker, paper, store
) -> None:
    """PI-4. The failure this replaces was silent in three separate ways."""
    one, two, stop, pub = await _both_attached(broker, paper, store)
    try:
        await one.detach(session_id=SESSION, reason="sts_stop")

        assert SESSION not in one._links  # noqa: SLF001
        assert SESSION in two._links, "the peer's link survived"  # noqa: SLF001
        assert store.live(ONE) == []
        assert store.live(TWO) == [SESSION], "the peer's rows stayed live"
    finally:
        stop.set()
        await pub
        await one.close_all()
        await two.close_all()


@pytest.mark.asyncio
async def test_a_reap_scan_does_not_close_a_session_only_the_peer_holds(
    broker, paper, store
) -> None:
    """A neighbour's live row is never decided against this process's map."""
    two = _manager(broker, paper, store, TWO)
    one = _manager(broker, paper, store, ONE)
    theirs = "only-theirs"
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease(broker, theirs, stop))
    try:
        await two.attach(
            MdAttachRequest(
                session_id=theirs,
                created_by=1,
                subscriptions=[FEED],
                timeout=5.0,
            )
        )

        assert await one.reap_orphans() == []
        assert await one.reap_orphans() == []
        assert store.live(TWO) == [theirs]
        assert theirs in two._links  # noqa: SLF001
    finally:
        stop.set()
        await pub
        await one.close_all()
        await two.close_all()


@pytest.mark.asyncio
async def test_a_peer_that_died_is_left_for_that_instance(
    broker, paper, store
) -> None:
    """A neighbour does not close another instance's leftover rows.

    The next process of that instance name is the one that reaps them.
    """
    one = _manager(broker, paper, store, ONE)
    store.seed(TWO, "orphaned-sts")

    assert await one.reap_orphans() == []
    assert await one.reap_orphans() == []
    assert store.live(TWO) == ["orphaned-sts"]
    await one.close_all()


@pytest.mark.asyncio
async def test_this_instance_reaps_its_own_ghost_rows(
    broker, paper, store
) -> None:
    one = _manager(broker, paper, store, ONE)
    store.seed(ONE, "orphaned-sts")

    assert await one.reap_orphans() == []
    assert await one.reap_orphans() == ["orphaned-sts"]
    assert store.live(ONE) == []
    await one.close_all()


@pytest.mark.asyncio
async def test_two_instances_may_hold_one_venue_for_one_session(
    broker, paper, store
) -> None:
    """Which the old ``(venue, session_id)`` uniqueness refused outright."""
    one, two, stop, pub = await _both_attached(broker, paper, store)
    try:
        held = [
            key for key in store.rows if key[2] == SESSION and key[1] == "Paper"
        ]
        assert sorted(k[0] for k in held) == [ONE, TWO]
    finally:
        stop.set()
        await pub
        await one.close_all()
        await two.close_all()
