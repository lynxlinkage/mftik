"""Restoring sessions that were running when STS last went away.

Candidates are exactly ``status = interrupted``. What makes a rebuild correct
rather than merely possible: it keeps the cid slot, so the strategy still
recognises its own orders; it hands back what the strategy remembered; and it
attaches in the order a deploy uses, because TD will not attach to a session
it cannot hear heartbeating.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import fakeredis.aioredis
import mftik_sts.session.manager as manager_mod
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.liveness import is_alive, mark_alive
from mftik.protocol import (
    MD_SESSION_ATTACH,
    TD_SESSION_ATTACH,
    Envelope,
    MdAttachResult,
    RpcError,
    TdAttachResult,
    Topics,
)
from mftik.strategy import Strategy
from mftik_sts.impl import register
from mftik_sts.session import SessionManager


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
        restart: str = "always",
        rebuild_count: int = 0,
        type: str | None = None,
    ) -> SimpleNamespace:
        row = SimpleNamespace(
            session_id=session_id,
            created_by=1,
            created_at=datetime.now(UTC),
            finished_at=datetime.now(UTC) - timedelta(seconds=finished_ago_s),
            status=status,
            reason="STS shut down while this was running",
            strategy=strategy,
            type=type,
            cid_slot=cid_slot,
            restart=restart,
            rebuild_count=rebuild_count,
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

    async def bump_rebuild_count(self, session_id: str) -> int:
        row = self.rows.get(session_id)
        if row is None:
            return 0
        row.rebuild_count = int(row.rebuild_count or 0) + 1
        return row.rebuild_count

    async def reset_rebuild_count(self, session_id: str) -> SimpleNamespace | None:
        row = self.rows.get(session_id)
        if row is None:
            return None
        row.rebuild_count = 0
        return row

    async def list_sessions(
        self,
        *,
        status: str | None = "live",
        created_by: int | None = None,
        limit: int = 100,
    ) -> list[SimpleNamespace]:
        return [
            r for r in self.rows.values() if status is None or r.status == status
        ]


class Rebuildable(Strategy):
    name = "rebuildable"
    rebuildable = True

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
        bump_rebuild_count=store.bump_rebuild_count,
        reset_rebuild_count=store.reset_rebuild_count,
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
    store.seed("r-1", td_api_ids=[3], md_ids=["bestquote.Paper_Spot_BTCUSDT"])
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
    await mark_alive(broker, "r-4", domain="sts")
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
    assert not await is_alive(broker, "r-5", domain="sts")


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
    assert not await is_alive(broker, "r-stale", domain="sts")


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


@pytest.mark.asyncio
async def test_a_strategy_that_cannot_be_rebuilt_is_left_alone(
    broker: Broker,
) -> None:
    """Readiness belongs to the strategy, not to whoever set the env var.

    A class without `on_rebuild` reads recon as a clean account and starts
    over, placing orders beside the ones the session left resting.
    """

    class NotReady(Strategy):
        name = "not_ready"

    register(NotReady)
    store = FakeStsStore()
    store.seed("r-noimpl", strategy="not_ready")
    manager = _manager(broker, store, [])
    manager._strategy_factory = lambda name: NotReady()  # noqa: SLF001

    assert await manager.rebuild_interrupted() == []
    assert store.rows["r-noimpl"].status == "interrupted"
    # No claim taken, so nothing has to release one.
    assert not await is_alive(broker, "r-noimpl", domain="sts")


@pytest.mark.asyncio
async def test_a_run_that_asked_not_to_come_back_stays_ended(
    broker: Broker,
) -> None:
    """Two gates already stand in front of a rebuild — the operator enabling
    it and the class supporting it. This is the third, and the only one the
    person who deployed the run controls."""
    store = FakeStsStore()
    store.seed("r-oneshot", restart="never")
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []
    assert store.rows["r-oneshot"].status == "interrupted"
    assert not await is_alive(broker, "r-oneshot", domain="sts")


@pytest.mark.asyncio
async def test_a_session_rebuilt_too_often_is_left_alone(broker: Broker) -> None:
    """A strategy that takes the process down with it would otherwise be
    restored into the same crash on every boot."""
    store = FakeStsStore()
    store.seed("r-loop", rebuild_count=3)
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []
    assert store.rows["r-loop"].status == "interrupted"


@pytest.mark.asyncio
async def test_the_attempt_is_counted_before_it_is_made(broker: Broker) -> None:
    """Counted first, because a rebuild that never returns still has to
    count — that is the loop the cap exists to break."""
    store = FakeStsStore()
    store.seed("r-count")
    counted: list[str] = []

    async def bump(session_id: str) -> int:
        counted.append(session_id)
        return len(counted)

    manager = _manager(broker, store, [])
    manager._bump_rebuild_count = bump  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["r-count"]
    assert counted == ["r-count"]
    await manager.close_all()


def _manager_with_factory(
    broker: Broker, store: FakeStsStore, factory
) -> SessionManager:  # noqa: ANN001
    return SessionManager(
        broker,
        heartbeat_interval=0.05,
        strategy_factory=factory,
        persist_live=store.persist_live,
        mark_done=store.mark_finished,
        mark_live=store.mark_live,
        list_db_sessions=store.list_sessions,
        bump_rebuild_count=store.bump_rebuild_count,
        reset_rebuild_count=store.reset_rebuild_count,
    )


@pytest.mark.asyncio
async def test_a_strategy_this_build_lacks_is_skipped_without_a_traceback(
    broker: Broker, caplog
) -> None:
    """A renamed or withdrawn strategy leaves rows that name it.

    Expected, and permanent for that row: no build will ever resolve it. A
    stack trace on every boot for a condition nothing can act on teaches an
    operator that STS tracebacks are noise, which is how a real one gets
    missed.
    """
    store = FakeStsStore()
    store.seed("r-gone", strategy="macd_volume")

    def factory(name: str | None) -> Strategy:
        raise KeyError(f"unknown strategy {name!r}")

    manager = _manager_with_factory(broker, store, factory)

    with caplog.at_level(logging.WARNING, logger=manager_mod.__name__):
        assert await manager.rebuild_interrupted() == []

    assert store.rows["r-gone"].status == "interrupted"
    records = [r for r in caplog.records if "r-gone" in r.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert records[0].exc_info is None
    # The name is in the line, since finding the row is the next thing anyone
    # reading this will want to do.
    assert "macd_volume" in records[0].getMessage()


@pytest.mark.asyncio
async def test_a_strategy_that_will_not_construct_keeps_its_traceback(
    broker: Broker, caplog
) -> None:
    """The other branch: this one is a fault, and the trace is the point."""
    store = FakeStsStore()
    store.seed("r-broken", strategy="explodes")

    def factory(name: str | None) -> Strategy:
        raise RuntimeError("__init__ blew up")

    manager = _manager_with_factory(broker, store, factory)

    with caplog.at_level(logging.WARNING, logger=manager_mod.__name__):
        assert await manager.rebuild_interrupted() == []

    records = [r for r in caplog.records if "r-broken" in r.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert records[0].exc_info is not None


@pytest.mark.asyncio
async def test_one_unresolvable_row_does_not_stop_the_scan(
    broker: Broker
) -> None:
    """The rows are independent; one stale name must not strand the rest."""
    store = FakeStsStore()
    store.seed("r-gone", strategy="macd_volume")
    store.seed("r-good")
    instances: list[Rebuildable] = []

    def factory(name: str | None) -> Strategy:
        if name == "macd_volume":
            raise KeyError(name)
        s = Rebuildable()
        instances.append(s)
        return s

    manager = _manager_with_factory(broker, store, factory)

    assert await manager.rebuild_interrupted() == ["r-good"]
    assert store.rows["r-gone"].status == "interrupted"
    await manager.close_all()


@pytest.mark.asyncio
async def test_a_rebuild_that_keeps_running_forgives_its_attempts(
    broker: Broker,
) -> None:
    """The cap counts attempts, not the deploys a healthy session lives through.

    Without this, a session that comes back and then trades all day still
    carries the attempt into the next restart, and the fourth one retires it
    for no reason anybody could see from the row.
    """
    store = FakeStsStore()
    store.seed("r-settle", rebuild_count=2)
    manager = _manager(broker, store, [])
    manager._rebuild_settle_s = 0.05  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["r-settle"]
    # Counted on the way in — the reset is what takes it back down.
    assert store.rows["r-settle"].rebuild_count == 3
    await asyncio.sleep(0.15)

    assert store.rows["r-settle"].rebuild_count == 0
    await manager.close_all()


@pytest.mark.asyncio
async def test_a_rebuild_that_does_not_hold_keeps_its_attempts(
    broker: Broker,
) -> None:
    """The loop the cap exists to break: restored, dead, restored again.

    A rebuild returning successfully says nothing about whether the strategy
    can survive being back — only running for a while does.
    """
    store = FakeStsStore()
    store.seed("r-nohold", rebuild_count=2)
    manager = _manager(broker, store, [])
    manager._rebuild_settle_s = 0.05  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["r-nohold"]
    await manager.close(
        "r-nohold", status="failed", reason="took the process down"
    )
    await asyncio.sleep(0.15)

    assert store.rows["r-nohold"].rebuild_count == 3


@pytest.mark.asyncio
async def test_a_later_run_under_the_same_id_is_not_credited(
    broker: Broker,
) -> None:
    """Identity, not id: a session that stopped and was deployed again is a
    different run, and clearing the count on its behalf would credit it for
    surviving something it was never part of."""
    store = FakeStsStore()
    store.seed("r-reused", rebuild_count=1)
    manager = _manager(broker, store, [])
    manager._rebuild_settle_s = 0.05  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["r-reused"]
    rebuilt = manager.get("r-reused")
    assert rebuilt is not None
    # Stands in for the operator stopping it and deploying it again.
    manager._sessions["r-reused"] = SimpleNamespace()  # noqa: SLF001
    await asyncio.sleep(0.15)

    assert store.rows["r-reused"].rebuild_count == 2
    manager._sessions["r-reused"] = rebuilt  # noqa: SLF001
    await manager.close_all()


@pytest.mark.asyncio
async def test_a_shutdown_mid_settle_leaves_the_count_alone(
    broker: Broker,
) -> None:
    """`close_all` is interrupting these sessions, not blessing them. A timer
    that fired during teardown would clear the count of a session STS is in
    the middle of taking away."""
    store = FakeStsStore()
    store.seed("r-shutdown", rebuild_count=2)
    manager = _manager(broker, store, [])
    manager._rebuild_settle_s = 0.05  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["r-shutdown"]
    await manager.close_all()
    await asyncio.sleep(0.15)

    assert store.rows["r-shutdown"].rebuild_count == 3
    assert store.rows["r-shutdown"].status == "interrupted"


@pytest.mark.asyncio
async def test_incompatible_environment_is_not_rebuilt_and_counts(
    broker: Broker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from mftik.registry import RegistryStore
    from mftik_sts.runtime_env import attach_overlay, reset_for_tests

    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    reset_for_tests()
    attach_overlay(tmp_path)
    registry = RegistryStore(tmp_path)
    registry.put_remote("peer", "http://peer:8000")
    added = registry.add(
        {
            "strategy.py": (
                "from mftik.strategy import Strategy\n\n"
                "class UsesNumpy(Strategy):\n"
                '    name = "uses_numpy"\n'
                '    requires = ("numpy",)\n'
            )
        },
        origin="peer",
        applied_extras={},
    )
    key = f"{added.origin}::{added.type}"
    store = FakeStsStore()
    store.seed("r-env", strategy="uses_numpy", type=key)
    instances: list[Rebuildable] = []
    manager = _manager(broker, store, instances)

    assert await manager.rebuild_interrupted() == []
    assert instances == []
    assert store.rows["r-env"].rebuild_count == 1
    assert store.rows["r-env"].status == "interrupted"
    reset_for_tests()
