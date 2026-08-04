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
from mft.protocol import ListSessionsRequest, StsCreateSessionRequest
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
    ) -> SimpleNamespace:
        row = SimpleNamespace(
            session_id=session_id,
            created_by=created_by,
            created_at=datetime.now(UTC),
            finished_at=None,
            status="live",
            strategy=strategy,
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
async def test_a_natural_exit_stays_done_and_carries_no_reason(
    broker: Broker,
) -> None:
    store = FakeStsStore()
    manager, _ = _manager(broker, store, ExitingStrategy)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="e-1", created_by=1, strategy="exiting"
        )
    )
    row = await _until_closed(manager, store, "e-1")
    assert row.status == "done"
    assert row.reason is None


@pytest.mark.asyncio
async def test_an_operator_stop_stays_done(broker: Broker) -> None:
    """Stopping a healthy session is not a failure, however it is spelled."""
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
    assert result.reason is None
    assert store.rows["s-1"].status == "done"


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
    assert row.reason is None


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
    try:
        await manager.create_session(
            StsCreateSessionRequest(
                session_id="dead-1",
                created_by=1,
                strategy="quiet_md",
                md=["paper.ticker.BTCUSDT"],
            )
        )
        row = await _until_closed(manager, store, "dead-1")
    finally:
        broker.subscribe = original  # type: ignore[method-assign]

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

    assert store.rows["kill-1"].status == "done"
