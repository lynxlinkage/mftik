"""Failed as a terminal status — Strategy.fail and infrastructure death.

A session that stops because something went wrong must not be recorded the
same way as one that finished its job: the row lands in ``failed`` and keeps
the reason, so the UI can say *why* without anyone reading the logs.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.protocol import (
    MD_SESSION_DETACH,
    ListSessionsRequest,
    MdDetachRequest,
    MdDetachResult,
    MdDetachResultEnvelope,
    StsCreateSessionRequest,
    Topics,
)
from mft_sts.impl import register
from mft_sts.session import SessionManager
from mft_sts.strategy import Strategy


@dataclass
class FakeStsStore:
    """Stand-in for ``mft_sts.db`` — same keyword contract as the real one."""

    rows: dict[str, SimpleNamespace] = field(default_factory=dict)

    async def persist_live(
        self,
        *,
        session_id: str,
        created_by: int,
        strategy: str | None = None,
        td_api_ids: list[int] | None = None,
        md_ids: list[str] | None = None,
        st_paras: dict | None = None,
        cid_slot: int | None = None,
        restart: str = "always",
    ) -> SimpleNamespace:
        row = SimpleNamespace(
            session_id=session_id,
            created_by=created_by,
            created_at=datetime.now(UTC),
            finished_at=None,
            status="live",
            strategy=strategy,
            cid_slot=cid_slot,
            restart=restart,
            rebuild_count=0,
            reason=None,
        )
        self.rows[session_id] = row
        return row

    async def mark_finished(
        self,
        session_id: str,
        *,
        status: str = "done",
        reason: str | None = None,
    ) -> SimpleNamespace | None:
        row = self.rows.get(session_id)
        if row is None:
            return None
        row.status = status
        row.reason = reason
        row.finished_at = datetime.now(UTC)
        return row

    async def list_sessions(
        self,
        *,
        status: str | None = "live",
        created_by: int | None = None,
    ) -> list[SimpleNamespace]:
        return [
            row
            for row in self.rows.values()
            if status is None or row.status == status
        ]


class FailingStrategy(Strategy):
    """Fails on ready, the way a strategy rejects its own configuration."""

    name = "failing"

    def __init__(self) -> None:
        super().__init__()
        self.events: list[str] = []

    async def on_ready(self) -> None:
        self.events.append("on_ready")
        self.fail("no tradable account attached")

    async def on_stop(self) -> None:
        self.events.append("on_stop")


class ExitingStrategy(Strategy):
    """Ends naturally — the control case for the failing one."""

    name = "exiting"

    async def on_ready(self) -> None:
        self.exit("work_done")


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


def _manager(broker: Broker, store: FakeStsStore, strategy: type[Strategy]):
    register(strategy)
    instances: list[Strategy] = []

    def factory(name: str | None) -> Strategy:
        s = strategy()
        instances.append(s)
        return s

    manager = SessionManager(
        broker,
        heartbeat_interval=0.1,
        strategy_factory=factory,
        persist_live=store.persist_live,
        mark_done=store.mark_finished,
        list_db_sessions=store.list_sessions,
    )
    return manager, instances


async def _until_closed(
    manager: SessionManager, store: FakeStsStore, session_id: str
) -> SimpleNamespace:
    """Wait for the row to reach a terminal status, not just for the pop.

    ``close`` removes the session from the manager before it writes the row,
    so waiting on ``manager.get`` alone races the store update.
    """
    for _ in range(100):
        row = store.rows.get(session_id)
        if (
            manager.get(session_id) is None
            and row is not None
            and row.status != "live"
        ):
            return row
        await asyncio.sleep(0.02)
    raise AssertionError(f"session {session_id} never reached a terminal status")


@pytest.mark.asyncio
async def test_strategy_fail_marks_the_session_failed_with_its_reason(
    broker: Broker,
) -> None:
    store = FakeStsStore()
    manager, instances = _manager(broker, store, FailingStrategy)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="f-1", created_by=1, strategy="failing"
        )
    )
    row = await _until_closed(manager, store, "f-1")
    assert row.status == "failed"
    assert row.reason == "no tradable account attached"
    assert row.finished_at is not None
    # Teardown is the same as a natural exit — on_stop still runs.
    assert "on_stop" in instances[0].events


@pytest.mark.asyncio
async def test_a_natural_exit_keeps_the_reason_it_gave(broker: Broker) -> None:
    """`done` says a strategy reached its own end, not which end.

    `oco_filled` and `chase_expired` are both done and mean very different
    things, so the reason has to survive the trip to the row.
    """
    store = FakeStsStore()
    manager, _ = _manager(broker, store, ExitingStrategy)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="e-1", created_by=1, strategy="exiting"
        )
    )
    row = await _until_closed(manager, store, "e-1")
    assert row.status == "done"
    assert row.reason == "work_done"


@pytest.mark.asyncio
async def test_an_operator_stop_is_done_and_says_so(broker: Broker) -> None:
    """Stopping a healthy session is not a failure — but it is not the
    strategy finishing either, and the status alone cannot say which."""
    store = FakeStsStore()
    manager, _ = _manager(broker, store, ExitingStrategy)

    class Idle(Strategy):
        name = "idle_stop"

    register(Idle)
    manager._strategy_factory = lambda name: Idle()  # noqa: SLF001
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="s-1", created_by=1, strategy="idle_stop"
        )
    )
    result = await manager.stop_session("s-1")

    assert result.status == "done"
    assert result.reason == "operator_stop"
    assert store.rows["s-1"].status == "done"
    assert store.rows["s-1"].reason == "operator_stop"


@pytest.mark.asyncio
async def test_the_first_ending_wins(broker: Broker) -> None:
    """A fail after an exit must not relabel a session already on its way out."""

    class ExitThenFail(Strategy):
        name = "exit_then_fail"

        async def on_ready(self) -> None:
            self.exit("work_done")
            self.fail("too late")

    store = FakeStsStore()
    manager, _ = _manager(broker, store, ExitThenFail)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="r-1", created_by=1, strategy="exit_then_fail"
        )
    )
    row = await _until_closed(manager, store, "r-1")

    assert row.status == "done"
    assert row.reason == "work_done"


async def _serve_md_detach(broker: Broker, stop: asyncio.Event) -> None:
    """Answer detaches so a stop does not wait out an absent MD.

    The session under test has an md attach, and detaching is a request now:
    unanswered, it is retried and then reported, which this test has no
    reason to sit through.
    """
    async for req in broker.serve(Topics.MD, stop=stop):
        if req.envelope.type != MD_SESSION_DETACH:
            continue
        payload = MdDetachRequest.model_validate(req.envelope.payload)
        await req.reply(
            MdDetachResultEnvelope.wrap(
                MdDetachResult(session_id=payload.session_id),
                type=MD_SESSION_DETACH,
                source="md",
                session_id=payload.session_id,
            )
        )


@pytest.mark.asyncio
async def test_a_dead_feed_fails_the_session_instead_of_leaving_it_live(
    broker: Broker,
) -> None:
    """A pump that raises never comes back — the row must not stay ``live``.

    Without this the session receives nothing, holds no lease, and still shows
    up in the UI as running.
    """

    class Quiet(Strategy):
        name = "quiet_md"

    store = FakeStsStore()
    manager, _ = _manager(broker, store, Quiet)

    original = broker.subscribe

    def exploding_subscribe(topics, **kwargs):
        if topics == "md.dead-1":
            raise RuntimeError("redis connection lost")
        return original(topics, **kwargs)

    broker.subscribe = exploding_subscribe  # type: ignore[method-assign]
    md_stop = asyncio.Event()
    md = asyncio.create_task(_serve_md_detach(broker, md_stop))
    try:
        await manager.create_session(
            StsCreateSessionRequest(
                session_id="dead-1",
                created_by=1,
                strategy="quiet_md",
                md=["ticker.Paper_Spot_BTCUSDT"],
            )
        )
        row = await _until_closed(manager, store, "dead-1")
    finally:
        broker.subscribe = original  # type: ignore[method-assign]
        md_stop.set()
        md.cancel()
        await asyncio.gather(md, return_exceptions=True)

    assert row.status == "failed"
    assert "md feed" in (row.reason or "")


@pytest.mark.asyncio
async def test_list_sessions_reports_the_reason(broker: Broker) -> None:
    store = FakeStsStore()
    manager, _ = _manager(broker, store, FailingStrategy)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="f-2", created_by=1, strategy="failing"
        )
    )
    await _until_closed(manager, store, "f-2")

    rows = await manager.list_sessions(
        ListSessionsRequest(domain="sts", status="failed")
    )
    assert [(r.session_id, r.status, r.reason) for r in rows] == [
        ("f-2", "failed", "no tradable account attached")
    ]
    # A closed session has no live strategy, so pause state is unknown.
    assert rows[0].paused is None


@pytest.mark.asyncio
async def test_shutdown_records_the_row_before_the_slow_teardown(
    broker: Broker,
) -> None:
    """A shutdown killed mid-teardown must not leave the row marked live.

    Teardown can block on TD acking cancels for longer than the SIGTERM grace
    period. If the row is only written afterwards, the process dies with the
    session stopped and the row still live — and no process is left to fix it.
    """

    class SlowStop(Strategy):
        name = "slow_stop"

        async def on_stop(self) -> None:
            # Stands in for waiting on a cancel ack that never comes.
            await asyncio.sleep(3600)

    store = FakeStsStore()
    manager, _ = _manager(broker, store, SlowStop)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="kill-1", created_by=1, strategy="slow_stop"
        )
    )

    # Shut down, then give up on it the way SIGKILL would.
    task = asyncio.create_task(manager.close_all())
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert store.rows["kill-1"].status == "interrupted"


@pytest.mark.asyncio
async def test_shutdown_is_interrupted_not_done(broker: Broker) -> None:
    """A session STS cut short did not reach its own end.

    Recording it as ``done`` would say the strategy finished, which is the one
    thing that did not happen — and would hide it from anything asking which
    sessions were still running when the process went away.
    """
    store = FakeStsStore()
    manager, _ = _manager(broker, store, ExitingStrategy)

    class Idle(Strategy):
        name = "idle_shutdown"

    register(Idle)
    manager._strategy_factory = lambda name: Idle()  # noqa: SLF001
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="sd-1", created_by=1, strategy="idle_shutdown"
        )
    )
    await manager.close_all()

    row = store.rows["sd-1"]
    assert row.status == "interrupted"
    assert row.reason
    assert row.finished_at is not None


@pytest.mark.asyncio
async def test_an_operator_stop_is_not_interrupted(broker: Broker) -> None:
    """Only STS going down is an interruption. A deliberate stop is a done."""
    store = FakeStsStore()
    manager, _ = _manager(broker, store, ExitingStrategy)

    class Idle(Strategy):
        name = "idle_operator"

    register(Idle)
    manager._strategy_factory = lambda name: Idle()  # noqa: SLF001
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="op-1", created_by=1, strategy="idle_operator"
        )
    )
    await manager.stop_session("op-1")

    assert store.rows["op-1"].status == "done"


@pytest.mark.asyncio
async def test_the_cid_slot_is_recorded_with_the_session(broker: Broker) -> None:
    """Without it on the row, a rebuilt session cannot keep its slot, and
    `owns()` would disown every order placed before the restart."""
    store = FakeStsStore()
    manager, _ = _manager(broker, store, ExitingStrategy)

    class Idle(Strategy):
        name = "idle_slot"

    register(Idle)
    manager._strategy_factory = lambda name: Idle()  # noqa: SLF001
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="slot-1", created_by=1, strategy="idle_slot"
        )
    )

    session = manager.get("slot-1")
    assert session is not None
    assert store.rows["slot-1"].cid_slot == session.cid_slot
    await manager.close_all()


@pytest.mark.asyncio
async def test_remember_reaches_the_store(broker: Broker) -> None:
    store = FakeStsStore()
    facts: list[tuple[str, str, str]] = []

    async def remember(session_id: str, key: str, value: str) -> None:
        facts.append((session_id, key, value))

    class Anchoring(Strategy):
        name = "anchoring"

        async def on_ready(self) -> None:
            await self.remember("ref_start", "50000")

    register(Anchoring)
    manager = SessionManager(
        broker,
        heartbeat_interval=0.1,
        strategy_factory=lambda name: Anchoring(),
        persist_live=store.persist_live,
        mark_done=store.mark_finished,
        list_db_sessions=store.list_sessions,
        remember_fact=remember,
    )
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="mem-1", created_by=1, strategy="anchoring"
        )
    )

    assert facts == [("mem-1", "ref_start", "50000")]
    await manager.close_all()


@pytest.mark.asyncio
async def test_a_failed_remember_does_not_stop_the_strategy(
    broker: Broker,
) -> None:
    """Everything remembered is a nicety for a rebuild that may never happen;
    the strategy calling it is in the middle of trading."""
    store = FakeStsStore()

    async def exploding_remember(session_id: str, key: str, value: str) -> None:
        raise RuntimeError("db gone")

    class Anchoring(Strategy):
        name = "anchoring_broken"

        async def on_ready(self) -> None:
            await self.remember("ref_start", "50000")

    register(Anchoring)
    manager = SessionManager(
        broker,
        heartbeat_interval=0.1,
        strategy_factory=lambda name: Anchoring(),
        persist_live=store.persist_live,
        mark_done=store.mark_finished,
        list_db_sessions=store.list_sessions,
        remember_fact=exploding_remember,
    )
    result = await manager.create_session(
        StsCreateSessionRequest(
            session_id="mem-2", created_by=1, strategy="anchoring_broken"
        )
    )

    assert result.session_id == "mem-2"
    assert manager.get("mem-2") is not None
    await manager.close_all()


@pytest.mark.asyncio
async def test_create_says_the_session_is_already_over(broker: Broker) -> None:
    """The reply carries the refusal, so a deploy need not go on to attach.

    A strategy rejects its configuration in ``on_start`` / ``on_ready``, which
    run inside ``create_session``. Without this the caller is told it created
    a live session, attaches feeds to it, and finds out half a minute later as
    a lease timeout naming nothing that went wrong.
    """
    store = FakeStsStore()
    manager, _ = _manager(broker, store, FailingStrategy)

    result = await manager.create_session(
        StsCreateSessionRequest(
            session_id="f-fast", created_by=1, strategy="failing"
        )
    )

    # Answered from the reply itself — no waiting on the teardown task.
    assert result.status == "failed"
    assert result.reason == "no tradable account attached"
    assert result.session_id == "f-fast"


@pytest.mark.asyncio
async def test_create_reports_a_natural_end_as_done(broker: Broker) -> None:
    """Not every early ending is a failure, and the deploy should see which."""
    store = FakeStsStore()
    manager, _ = _manager(broker, store, ExitingStrategy)

    result = await manager.create_session(
        StsCreateSessionRequest(
            session_id="e-fast", created_by=1, strategy="exiting"
        )
    )

    assert result.status == "done"
    assert result.reason == "work_done"


@pytest.mark.asyncio
async def test_create_of_a_healthy_session_still_says_live(
    broker: Broker,
) -> None:
    store = FakeStsStore()

    class Idle(Strategy):
        name = "idle_create"

    manager, _ = _manager(broker, store, Idle)
    result = await manager.create_session(
        StsCreateSessionRequest(
            session_id="ok-1", created_by=1, strategy="idle_create"
        )
    )

    assert result.status == "live"
    assert result.reason is None
    await manager.close_all()
