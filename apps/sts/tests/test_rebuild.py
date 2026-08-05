"""Restoring sessions that were running when STS last went away.

Candidates are exactly ``status = interrupted``. What makes a rebuild correct
rather than merely possible: it keeps the cid slot, so the strategy still
recognises its own orders; it hands back what the strategy remembered; and it
attaches in the order a deploy uses, because TD will not attach to a session
it cannot hear heartbeating.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import fakeredis.aioredis
import mft_sts.session.manager as manager_mod
import pytest
from mft.broker import Broker, BrokerConfig
from mft.protocol import (
    MD_SESSION_ATTACH,
    TD_SESSION_ATTACH,
    Envelope,
    MdAttachResult,
    RpcError,
    TdAttachResult,
    Topics,
)
from mft_sts.impl import register
from mft_sts.liveness import is_alive, mark_alive
from mft_sts.session import SessionManager
from mft_sts.strategy import Strategy


@dataclass
class FakeStsStore:
    rows: dict[str, SimpleNamespace] = field(default_factory=dict)

    def seed(
        self,
        session_id: str,
        *,
        status: str = "interrupted",
        strategy: str = "rebuildable",
        cid_slot: int | None = 7,
        td_api_ids: list[int] | None = None,
        md_ids: list[str] | None = None,
        st_facts: dict[str, str] | None = None,
        finished_ago_s: float = 0.0,
    ) -> SimpleNamespace:
        row = SimpleNamespace(
            session_id=session_id,
            created_by=1,
            created_at=datetime.now(UTC),
            finished_at=datetime.now(UTC) - timedelta(seconds=finished_ago_s),
            status=status,
            reason="STS shut down while this was running",
            strategy=strategy,
            cid_slot=cid_slot,
            td_api_ids=list(td_api_ids or []),
            md_ids=list(md_ids or []),
            st_paras={},
            st_facts=dict(st_facts or {}),
        )
        self.rows[session_id] = row
        return row

    async def persist_live(self, **kwargs: Any) -> SimpleNamespace:
        return self.seed(kwargs["session_id"], status="live")

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

    async def mark_live(self, session_id: str) -> SimpleNamespace | None:
        row = self.rows.get(session_id)
        if row is None:
            return None
        row.status = "live"
        row.finished_at = None
        row.reason = None
        return row

    async def list_sessions(
        self, *, status: str | None = "live", created_by: int | None = None
    ) -> list[SimpleNamespace]:
        return [
            r for r in self.rows.values() if status is None or r.status == status
        ]


class Rebuildable(Strategy):
    name = "rebuildable"

    def __init__(self) -> None:
        super().__init__()
        self.rebuilt_with: dict[str, str] | None = None
        self.events: list[str] = []

    async def on_rebuild(self, remembered: dict[str, str]) -> None:
        self.rebuilt_with = dict(remembered)
        self.events.append("on_rebuild")

    async def on_start(self) -> None:
        self.events.append("on_start")

    async def on_ready(self) -> None:
        self.events.append("on_ready")


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


def _manager(
    broker: Broker, store: FakeStsStore, instances: list[Rebuildable]
) -> SessionManager:
    register(Rebuildable)

    def factory(name: str | None) -> Strategy:
        s = Rebuildable()
        instances.append(s)
        return s

    return SessionManager(
        broker,
        heartbeat_interval=0.05,
        strategy_factory=factory,
        persist_live=store.persist_live,
        mark_done=store.mark_finished,
        mark_live=store.mark_live,
        list_db_sessions=store.list_sessions,
    )


async def _serve_attaches(
    broker: Broker, stop: asyncio.Event, *, fail: bool = False
) -> None:
    """Stand in for TD and MD answering their attach subjects."""

    async def serve(subject: str, ok_type: str, err_type: str) -> None:
        async for req in broker.serve(subject, stop=stop):
            payload = req.envelope.payload
            session_id = payload.get("session_id", "")
            if fail:
                await req.reply(
                    Envelope[RpcError].wrap(
                        RpcError(code="nope", message="not today"),
                        type=err_type,
                        source="fake",
                        session_id=session_id,
                    )
                )
                continue
            if ok_type == TD_SESSION_ATTACH:
                result: Any = TdAttachResult(
                    session_id=session_id,
                    api_id=int(payload.get("api_id", 0)),
                    refcount=1,
                )
                env: Any = Envelope[TdAttachResult].wrap(
                    result, type=ok_type, source="td", session_id=session_id
                )
            else:
                env = Envelope[MdAttachResult].wrap(
                    MdAttachResult(
                        session_id=session_id,
                        subscriptions=list(payload.get("subscriptions", [])),
                    ),
                    type=ok_type,
                    source="md",
                    session_id=session_id,
                )
            await req.reply(env)

    await asyncio.gather(
        serve(Topics.TD, TD_SESSION_ATTACH, "td.error"),
        serve(Topics.MD, MD_SESSION_ATTACH, "md.error"),
        return_exceptions=True,
    )


@pytest.mark.asyncio
async def test_an_interrupted_session_comes_back(broker: Broker) -> None:
    store = FakeStsStore()
    store.seed("r-1", td_api_ids=[3], md_ids=["paper.bestquote.BTCUSDT"])
    instances: list[Rebuildable] = []
    manager = _manager(broker, store, instances)

    stop = asyncio.Event()
    serving = asyncio.create_task(_serve_attaches(broker, stop))
    await asyncio.sleep(0.1)
    try:
        assert await manager.rebuild_interrupted() == ["r-1"]
    finally:
        stop.set()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)

    row = store.rows["r-1"]
    assert row.status == "live"
    # A session that is running again has no end and no reason for one.
    assert row.finished_at is None
    assert row.reason is None
    assert manager.get("r-1") is not None
    await manager.close_all()


@pytest.mark.asyncio
async def test_the_cid_slot_survives(broker: Broker) -> None:
    """The point of the whole thing: the strategy still owns its old orders."""
    store = FakeStsStore()
    store.seed("r-2", cid_slot=4242)
    instances: list[Rebuildable] = []
    manager = _manager(broker, store, instances)

    await manager.rebuild_interrupted()

    session = manager.get("r-2")
    assert session is not None
    assert session.cid_slot == 4242
    await manager.close_all()


@pytest.mark.asyncio
async def test_remembered_facts_arrive_before_any_other_hook(
    broker: Broker,
) -> None:
    """`on_rebuild` runs first, so every later hook sees restored state."""
    store = FakeStsStore()
    store.seed("r-3", st_facts={"ref_start": "50000"})
    instances: list[Rebuildable] = []
    manager = _manager(broker, store, instances)

    await manager.rebuild_interrupted()

    strat = instances[0]
    assert strat.rebuilt_with == {"ref_start": "50000"}
    assert strat.events == ["on_rebuild", "on_start", "on_ready"]
    await manager.close_all()


@pytest.mark.asyncio
async def test_a_session_without_a_slot_is_left_alone(broker: Broker) -> None:
    """Rebuilding it would mint ids in a different slot, and the strategy
    would disown every order it placed before the restart."""
    store = FakeStsStore()
    store.seed("r-old", cid_slot=None)
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []
    assert store.rows["r-old"].status == "interrupted"


@pytest.mark.asyncio
async def test_only_one_process_rebuilds_a_session(broker: Broker) -> None:
    """Several STS processes boot at once. Without the atomic claim they
    would each restore the same session and run two of it."""
    store = FakeStsStore()
    store.seed("r-4")
    # Stands in for the peer that got there first.
    await mark_alive(broker, "r-4")
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []
    assert store.rows["r-4"].status == "interrupted"


@pytest.mark.asyncio
async def test_a_live_session_is_not_rebuilt(broker: Broker) -> None:
    store = FakeStsStore()
    store.seed("r-live", status="live")
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []


@pytest.mark.asyncio
async def test_a_failed_attach_puts_the_session_back(
    broker: Broker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Half-attached is worse than interrupted: it heartbeats and looks alive
    while blind to a feed or an account."""
    # The real backoff is sized for a TD that is still starting; here every
    # attempt is refused outright, so waiting it out only slows the suite.
    monkeypatch.setattr(manager_mod, "_ATTACH_ATTEMPTS", 2)
    monkeypatch.setattr(manager_mod, "_ATTACH_BACKOFF_S", 0.01)
    store = FakeStsStore()
    store.seed("r-5", td_api_ids=[3])
    manager = _manager(broker, store, [])

    stop = asyncio.Event()
    serving = asyncio.create_task(_serve_attaches(broker, stop, fail=True))
    await asyncio.sleep(0.1)
    try:
        assert await manager.rebuild_interrupted() == []
    finally:
        stop.set()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)

    assert store.rows["r-5"].status == "interrupted"
    assert manager.get("r-5") is None
    # The claim is released, so the next boot may try again.
    assert not await is_alive(broker, "r-5")


@pytest.mark.asyncio
async def test_a_stale_session_is_left_where_it_is(broker: Broker) -> None:
    """Restoring is for a restart, where the gap is seconds to minutes.

    A session interrupted long ago would come back to a market that moved on
    and to orders the venue may have expired. Not an error and not a status
    change — the row keeps saying what happened to it, and a person can still
    decide to do something about it.
    """
    store = FakeStsStore()
    store.seed("r-stale", finished_ago_s=4000.0)
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []
    row = store.rows["r-stale"]
    assert row.status == "interrupted"
    assert row.reason == "STS shut down while this was running"
    assert not await is_alive(broker, "r-stale")


@pytest.mark.asyncio
async def test_a_session_inside_the_window_still_comes_back(
    broker: Broker,
) -> None:
    store = FakeStsStore()
    store.seed("r-fresh", finished_ago_s=60.0)
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == ["r-fresh"]
    await manager.close_all()


@pytest.mark.asyncio
async def test_an_unknown_age_counts_as_too_old(broker: Broker) -> None:
    """A row that says it is interrupted without saying when is not evidence
    that it stopped recently."""
    store = FakeStsStore()
    row = store.seed("r-nodate")
    row.finished_at = None
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []
    assert store.rows["r-nodate"].status == "interrupted"


@pytest.mark.asyncio
async def test_the_window_is_configurable(broker: Broker) -> None:
    store = FakeStsStore()
    store.seed("r-wide", finished_ago_s=4000.0)
    manager = _manager(broker, store, [])
    manager._rebuild_max_age_s = 5000.0  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["r-wide"]
    await manager.close_all()
