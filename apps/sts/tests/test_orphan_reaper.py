"""Reaping sessions whose process died without writing anything.

Every other ending records its own row. This covers the one that cannot —
SIGKILL, OOM, the machine going away — where the row would otherwise keep
claiming a session is running with no owner left to say otherwise.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.protocol import StsCreateSessionRequest
from mft_sts.impl import register
from mft_sts.liveness import alive_key, clear_alive, is_alive, mark_alive
from mft_sts.session import SessionManager
from mft_sts.strategy import Strategy


@dataclass
class FakeStsStore:
    rows: dict[str, SimpleNamespace] = field(default_factory=dict)

    def seed_live(
        self,
        session_id: str,
        strategy: str = "oco",
        cid_slot: int | None = None,
        restart: str = "always",
    ) -> SimpleNamespace:
        row = SimpleNamespace(
            session_id=session_id,
            created_by=1,
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
        return self.seed_live(
            session_id, strategy or "unknown", cid_slot, restart
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
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


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

    assert await manager.reap_orphans() == []
    assert store.rows["mine-1"].status == "live"
    await manager.close_all()


@pytest.mark.asyncio
async def test_a_session_another_process_runs_is_left_alone(
    broker: Broker,
) -> None:
    """The point of the key: "not mine" is not the same as "nobody's".

    Several STS processes serve the same RPC subject, so reaping on ownership
    alone would have each one killing the others' sessions.
    """
    store = FakeStsStore()
    store.seed_live("theirs-1")
    # Stands in for the peer that owns it holding its key.
    await mark_alive(broker, "theirs-1")
    manager = _manager(broker, store)

    assert await manager.reap_orphans() == []
    assert store.rows["theirs-1"].status == "live"


@pytest.mark.asyncio
async def test_the_key_is_claimed_before_the_row_exists(broker: Broker) -> None:
    """Otherwise a reaper could see a live row with no key and fail a
    session that is a moment old."""
    store = FakeStsStore()
    seen: list[bool] = []

    async def watching_persist(**kwargs):
        seen.append(await is_alive(broker, kwargs["session_id"]))
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
async def test_closing_releases_the_key(broker: Broker) -> None:
    store = FakeStsStore()
    manager = _manager(broker, store)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="rel-1", created_by=1, strategy="idle_reap"
        )
    )
    assert await is_alive(broker, "rel-1")

    await manager.close("rel-1")
    assert not await is_alive(broker, "rel-1")


@pytest.mark.asyncio
async def test_the_key_is_renewed_while_the_session_runs(
    broker: Broker,
) -> None:
    """A long-running session must not be reaped when its key would expire."""
    store = FakeStsStore()
    manager = _manager(broker, store)
    await manager.create_session(
        StsCreateSessionRequest(
            session_id="renew-1", created_by=1, strategy="idle_reap"
        )
    )
    key = alive_key(broker.config.key_prefix, "renew-1")
    # Expire it out from under the session; the heartbeat must put it back.
    await broker.redis.delete(key)
    for _ in range(50):
        if await is_alive(broker, "renew-1"):
            break
        await asyncio.sleep(0.02)

    assert await is_alive(broker, "renew-1")
    await manager.close_all()


@pytest.mark.asyncio
async def test_an_unreadable_liveness_check_reaps_nothing(
    broker: Broker,
) -> None:
    """Redis being unreachable is not evidence that a session died.

    Leaving a stale row is recoverable on the next scan; failing a strategy
    that is still trading is not.
    """
    store = FakeStsStore()
    store.seed_live("unknown-1")
    manager = _manager(broker, store)

    original = broker.redis.exists

    async def exploding_exists(*args, **kwargs):
        raise RuntimeError("redis gone")

    broker.redis.exists = exploding_exists  # type: ignore[method-assign]
    try:
        assert await manager.reap_orphans() == []
    finally:
        broker.redis.exists = original  # type: ignore[method-assign]
    assert store.rows["unknown-1"].status == "live"


@pytest.mark.asyncio
async def test_reaping_is_safe_to_repeat(broker: Broker) -> None:
    store = FakeStsStore()
    store.seed_live("ghost-2")
    manager = _manager(broker, store)

    assert await manager.reap_orphans() == ["ghost-2"]
    # Already terminal, so it is no longer in the live listing.
    assert await manager.reap_orphans() == []
    assert store.rows["ghost-2"].status == "interrupted"


@pytest.mark.asyncio
async def test_clearing_a_key_that_was_never_claimed_is_fine(
    broker: Broker,
) -> None:
    await clear_alive(broker, "never-existed")
