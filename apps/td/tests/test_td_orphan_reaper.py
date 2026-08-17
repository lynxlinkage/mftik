"""Closing attaches whose strategy is gone, in memory and in the table.

The lease is the primary signal, and it has one blind spot: it can only
expire while something is reading it, so a lease loop that stops takes the
expiry with it. This scan is the check that does not run inside the thing it
is checking — it asks whether the STS session behind an attach still exists,
which is answered by a key no TD loop has to be alive to read.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.exchange import PaperExchange
from mftik.liveness import clear_alive, mark_alive
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
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-td-reap"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


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
    await mark_alive(broker, session_id, domain="sts")
    stop = asyncio.Event()
    task = asyncio.create_task(_lease_publisher(broker, session_id, stop))
    await sessions.attach(
        TdAttachRequest(
            api_id=API_ID, session_id=session_id, created_by=1, timeout=5.0
        )
    )
    return task, stop


@pytest.mark.asyncio
async def test_a_link_whose_strategy_is_gone_is_detached(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """The case the lease cannot reach, because its reader is what died."""
    task, stop = await _attached(broker, sessions, "gone-1")
    await clear_alive(broker, "gone-1", domain="sts")

    # One scan is a suspicion, not a verdict.
    assert await sessions.reap_orphans() == []
    assert store.status("gone-1") == "live"

    assert await sessions.reap_orphans() == [("gone-1", API_ID)]
    assert store.status("gone-1") == "done"
    # The point of detaching rather than only closing the row: the venue
    # session behind the last link goes too.
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
async def test_a_key_that_comes_back_clears_the_strikes(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """A Redis outage past the TTL looks exactly like a stopped strategy for
    one scan. Two in a row, with the key absent throughout, is what separates
    them — so a key that returns must start the count over."""
    task, stop = await _attached(broker, sessions, "blip-1")

    await clear_alive(broker, "blip-1", domain="sts")
    assert await sessions.reap_orphans() == []
    await mark_alive(broker, "blip-1", domain="sts")
    assert await sessions.reap_orphans() == []
    await clear_alive(broker, "blip-1", domain="sts")
    assert await sessions.reap_orphans() == []

    assert store.status("blip-1") == "live"
    assert sessions.get(API_ID) is not None

    stop.set()
    await sessions.detach(session_id="blip-1", api_id=API_ID, reason="test")
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_an_unreadable_liveness_check_detaches_nothing(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """Redis being unreachable is not evidence that a strategy stopped.

    A stale link survives to the next scan; one detached by mistake takes the
    attach out from under a strategy that is still trading.
    """
    task, stop = await _attached(broker, sessions, "unreadable-1")
    await clear_alive(broker, "unreadable-1", domain="sts")
    original = broker.redis.exists

    async def exploding_exists(*args, **kwargs):
        raise RuntimeError("redis gone")

    broker.redis.exists = exploding_exists  # type: ignore[method-assign]
    try:
        assert await sessions.reap_orphans() == []
        assert await sessions.reap_orphans() == []
    finally:
        broker.redis.exists = original  # type: ignore[method-assign]

    assert store.status("unreadable-1") == "live"
    assert sessions.get(API_ID) is not None

    stop.set()
    await sessions.detach(
        session_id="unreadable-1", api_id=API_ID, reason="test"
    )
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_a_row_left_by_a_previous_process_is_closed(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """A restart clears the links but not the table.

    Nothing is running behind this row to be wrong about, so it goes on the
    first scan — the strike count guards live attaches, not dead rows.
    """
    store.seed_live("ghost-1")

    assert await sessions.reap_orphans() == [("ghost-1", API_ID)]
    assert store.status("ghost-1") == "done"


@pytest.mark.asyncio
async def test_a_row_whose_strategy_still_runs_is_left_alone(
    broker: Broker, sessions: SessionManager, store: FakeStore
) -> None:
    """Not this process's link is not the same as nobody's.

    Several TD processes serve the same subject; the key is what tells a row
    another one is running from a row with no owner at all.
    """
    store.seed_live("theirs-1")
    await mark_alive(broker, "theirs-1", domain="sts")

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
