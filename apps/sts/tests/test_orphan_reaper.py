"""Reaping sessions whose process died without writing anything.

Every other ending records its own row. This covers the one that cannot —
SIGKILL, OOM, the machine going away — where the row would otherwise keep
claiming a session is running with no owner left to say otherwise.

A row is an orphan when it belongs to this instance and this process does
not have it locally. Two scans must agree.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.protocol import StsCreateSessionRequest
from mftik.strategy import Strategy
from mftik_sts.impl import register
from mftik_sts.session import SessionManager


@dataclass
class FakeStsStore:
    rows: dict[str, SimpleNamespace] = field(default_factory=dict)

    def seed_live(
        self,
        session_id: str,
        strategy: str = "oco",
        restart: str = "always",
        instance: str | None = "sts",
    ) -> SimpleNamespace:
        row = SimpleNamespace(
            session_id=session_id,
            created_by=1,
            created_at=datetime.now(UTC),
            finished_at=None,
            status="live",
            strategy=strategy,
            restart=restart,
            rebuild_count=0,
            reason=None,
            instance=instance,
            td={},
        )
        self.rows[session_id] = row
        return row

    async def persist_live(
        self,
        *,
        session_id: str,
        created_by: int,
        strategy: str | None = None,
        td_api_ids: list[int] | None = None,
        md_ids: list[str] | None = None,
        st_paras: dict | None = None,
        restart: str = "always",
        **_extra: object,
    ) -> SimpleNamespace:
        return self.seed_live(
            session_id, strategy or "unknown", restart
        )

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
        self, *, status: str | None = "live", created_by: int | None = None
    ) -> list[SimpleNamespace]:
        return [
            r for r in self.rows.values() if status is None or r.status == status
        ]


class Idle(Strategy):
    name = "idle_reap"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


def _manager(broker: Broker, store: FakeStsStore) -> SessionManager:
    register(Idle)
    return SessionManager(
        broker,
        heartbeat_interval=0.1,
        strategy_factory=lambda name: Idle(),
        persist_live=store.persist_live,
        mark_done=store.mark_finished,
        list_db_sessions=store.list_sessions,
    )


async def _reap_twice(manager: SessionManager) -> list[str]:
    first = await manager.reap_orphans()
    if first:
        return first
    return await manager.reap_orphans()


@pytest.mark.asyncio
async def test_a_row_with_no_owner_is_interrupted(broker: Broker) -> None:
    """Interrupted rather than failed: nothing was wrong with the strategy
    and it did not choose to stop, which is the same category as a shutdown.

    It is also what makes the rebuild candidate set `status = interrupted`
    rather than a match on the reason string.
    """
    store = FakeStsStore()
    store.seed_live("ghost-1")
    manager = _manager(broker, store)

    assert await manager.reap_orphans() == []
    assert await manager.reap_orphans() == ["ghost-1"]
    row = store.rows["ghost-1"]
    assert row.status == "interrupted"
    assert row.reason == "process died: no session heartbeat"
    assert row.finished_at is not None


@pytest.mark.asyncio
async def test_a_session_this_process_runs_is_left_alone(broker: Broker) -> None:
    store = FakeStsStore()
    manager = _manager(broker, store)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="mine-1", created_by=1, strategy="idle_reap"
        )
    )

    assert await _reap_twice(manager) == []
    assert store.rows["mine-1"].status == "live"
    await manager.close_all()


@pytest.mark.asyncio
async def test_a_session_another_instance_owns_is_left_alone(
    broker: Broker,
) -> None:
    store = FakeStsStore()
    store.seed_live("theirs-1", instance="sts-jp")
    manager = _manager(broker, store)

    assert await _reap_twice(manager) == []
    assert store.rows["theirs-1"].status == "live"


@pytest.mark.asyncio
async def test_an_unpinned_row_without_a_derivation_is_left_alone(
    broker: Broker,
) -> None:
    store = FakeStsStore()
    store.seed_live("cross-1", instance=None)
    manager = _manager(broker, store)

    assert await _reap_twice(manager) == []
    assert store.rows["cross-1"].status == "live"


@pytest.mark.asyncio
async def test_the_session_is_registered_before_the_row_exists(
    broker: Broker,
) -> None:
    """Otherwise a reaper could see a live row with no local session."""
    store = FakeStsStore()
    seen: list[bool] = []

    async def watching_persist(**kwargs):
        seen.append(kwargs["session_id"] in manager._sessions)  # noqa: SLF001
        return await store.persist_live(**kwargs)

    manager = _manager(broker, store)
    manager._persist_live = watching_persist  # noqa: SLF001
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="order-1", created_by=1, strategy="idle_reap"
        )
    )

    assert seen == [True]
    await manager.close_all()


@pytest.mark.asyncio
async def test_reaping_is_safe_to_repeat(broker: Broker) -> None:
    store = FakeStsStore()
    store.seed_live("ghost-2")
    manager = _manager(broker, store)

    assert await _reap_twice(manager) == ["ghost-2"]
    assert await manager.reap_orphans() == []
    assert store.rows["ghost-2"].status == "interrupted"
