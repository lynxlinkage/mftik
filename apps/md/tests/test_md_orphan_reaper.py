"""Reaping MD attach rows whose process died without writing anything.

`detach` closes a row, and every path into it needs the MD process running.
This covers the ending that has none — SIGKILL, OOM, the machine going away
— where the row would otherwise go on claiming a venue feed forever, and the
API, which answers from the table, would go on reporting it as live.
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
from mftik.exchange.tickers import UniversalTicker
from mftik.liveness import alive_key, clear_alive, is_alive, mark_alive
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
        self, session_id: str, venue: str = "Bybit"
    ) -> SimpleNamespace:
        row = SimpleNamespace(
            venue=venue,
            session_id=session_id,
            created_by=1,
            created_at=datetime.now(UTC),
            finished_at=None,
            status="live",
        )
        self.rows[(venue, session_id)] = row
        return row

    async def persist_live(
        self,
        *,
        session_id: str,
        created_by: int,
        venues: list[str] | None = None,
    ) -> list[SimpleNamespace]:
        return [self.seed_live(session_id, venue) for venue in venues or []]

    async def mark_done(self, *, session_id: str) -> list[SimpleNamespace]:
        done: list[SimpleNamespace] = []
        for row in self.rows.values():
            if row.session_id != session_id or row.status != "live":
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
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-md"),
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
async def test_a_row_with_no_owner_is_closed(
    broker: Broker, paper: PaperExchange
) -> None:
    """`done`, not `interrupted`: an md row follows its strategy session
    rather than carrying an outcome of its own."""
    store = FakeMdStore()
    store.seed_live("ghost-1")
    sessions = _manager(broker, paper, store)

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

    The key is renewed by the lease loop and by nothing else, so a link whose
    key has gone is one whose loop has stopped — an attach holding a venue
    feed open for a session nobody is leasing. Skipping local links, as this
    scan used to, is what let exactly that state last for hours.
    """
    store = FakeMdStore()
    sessions = _manager(broker, paper, store)
    task, stop = await _attached(broker, sessions, "dead-loop-1")

    # Stop the loop the way a transport failure leaves it — gone, with the
    # link still registered — then let its key lapse.
    link = sessions._links["dead-loop-1"]  # noqa: SLF001
    link.stop.set()
    await asyncio.gather(*link.tasks, return_exceptions=True)
    await clear_alive(broker, "dead-loop-1", domain="md")

    assert await sessions.reap_orphans() == []
    assert await sessions.reap_orphans() == ["dead-loop-1"]

    assert store.rows[("Paper", "dead-loop-1")].status == "done"
    # Torn down, not merely marked done: the feed must stop pumping too.
    assert "dead-loop-1" not in sessions._links  # noqa: SLF001
    assert sessions.feed_refcount(FEED) == 0

    stop.set()
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_a_healthy_link_survives_a_missing_key(
    broker: Broker, paper: PaperExchange
) -> None:
    """A Redis outage past the TTL looks like a dead loop for one scan.

    The lease loop puts the key back as soon as it can heartbeat again, so
    one absent reading must not cost a running session its feeds.
    """
    store = FakeMdStore()
    sessions = _manager(broker, paper, store)
    task, stop = await _attached(broker, sessions, "blip-1")

    await clear_alive(broker, "blip-1", domain="md")
    assert await sessions.reap_orphans() == []

    # The loop is alive, so the key is back before the next scan.
    for _ in range(50):
        if await is_alive(broker, "blip-1", domain="md"):
            break
        await asyncio.sleep(0.02)
    assert await sessions.reap_orphans() == []
    assert store.rows[("Paper", "blip-1")].status == "live"

    stop.set()
    await sessions.detach(session_id="blip-1", reason="test")
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_an_attach_another_process_holds_is_left_alone(
    broker: Broker, paper: PaperExchange
) -> None:
    """The point of the key: "not mine" is not the same as "nobody's".

    Several MD processes serve the same RPC subject, so reaping on local
    links alone would have each one closing the others' rows.
    """
    store = FakeMdStore()
    store.seed_live("theirs-1")
    # Stands in for the peer that holds the attach holding its key.
    await mark_alive(broker, "theirs-1", domain="md")
    sessions = _manager(broker, paper, store)

    assert await sessions.reap_orphans() == []
    assert store.rows[("Bybit", "theirs-1")].status == "live"


@pytest.mark.asyncio
async def test_a_live_sts_session_does_not_protect_a_dead_attach(
    broker: Broker, paper: PaperExchange
) -> None:
    """MD is reaped on MD's key, not on the strategy's.

    MD dying under a strategy that keeps running is the case a check on the
    STS key would call healthy — and the one where the row is most wrong,
    because the session goes on claiming a feed nobody is pumping.
    """
    store = FakeMdStore()
    store.seed_live("sts-alive-1")
    await mark_alive(broker, "sts-alive-1", domain="sts")
    sessions = _manager(broker, paper, store)

    assert await sessions.reap_orphans() == ["sts-alive-1"]
    assert store.rows[("Bybit", "sts-alive-1")].status == "done"


@pytest.mark.asyncio
async def test_the_key_is_claimed_before_the_row_exists(
    broker: Broker, paper: PaperExchange
) -> None:
    """Otherwise a reaper could see a live row with no key and close an
    attach that is a moment old."""
    store = FakeMdStore()
    sessions = _manager(broker, paper, store)
    seen: list[bool] = []

    async def watching_persist(**kwargs):
        seen.append(await is_alive(broker, kwargs["session_id"], domain="md"))
        return await store.persist_live(**kwargs)

    sessions._persist_live = watching_persist  # noqa: SLF001
    task, stop = await _attached(broker, sessions, "order-1")

    assert seen == [True]

    stop.set()
    await sessions.detach(session_id="order-1", reason="test")
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_detaching_releases_the_key(
    broker: Broker, paper: PaperExchange
) -> None:
    store = FakeMdStore()
    sessions = _manager(broker, paper, store)
    task, stop = await _attached(broker, sessions, "rel-1")
    assert await is_alive(broker, "rel-1", domain="md")

    stop.set()
    await sessions.detach(session_id="rel-1", reason="test")
    assert not await is_alive(broker, "rel-1", domain="md")

    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_the_key_is_renewed_while_the_attach_lives(
    broker: Broker, paper: PaperExchange
) -> None:
    """A long-lived attach must not be reaped when its key would expire."""
    store = FakeMdStore()
    sessions = _manager(broker, paper, store)
    task, stop = await _attached(broker, sessions, "renew-1")

    # Expire it out from under the attach; the lease heartbeat must put it
    # back without STS having to re-attach.
    await broker.redis.delete(
        alive_key(broker.config.key_prefix, "renew-1", domain="md")
    )
    for _ in range(50):
        if await is_alive(broker, "renew-1", domain="md"):
            break
        await asyncio.sleep(0.02)

    assert await is_alive(broker, "renew-1", domain="md")

    stop.set()
    await sessions.detach(session_id="renew-1", reason="test")
    await asyncio.gather(task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_an_unreadable_liveness_check_reaps_nothing(
    broker: Broker, paper: PaperExchange
) -> None:
    """Redis being unreachable is not evidence that MD died.

    A stale row survives to the next scan; a row wrongly closed hides a feed
    that is still running.
    """
    store = FakeMdStore()
    store.seed_live("unreadable-1")
    sessions = _manager(broker, paper, store)

    original = broker.redis.exists

    async def exploding_exists(*args, **kwargs):
        raise RuntimeError("redis gone")

    broker.redis.exists = exploding_exists  # type: ignore[method-assign]
    try:
        assert await sessions.reap_orphans() == []
    finally:
        broker.redis.exists = original  # type: ignore[method-assign]
    assert store.rows[("Bybit", "unreadable-1")].status == "live"


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


@pytest.mark.asyncio
async def test_clearing_a_key_that_was_never_claimed_is_fine(
    broker: Broker,
) -> None:
    await clear_alive(broker, "never-existed", domain="md")
