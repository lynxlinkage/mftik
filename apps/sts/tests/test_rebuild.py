"""Restoring sessions that were running when STS last went away.

Candidates are exactly ``status = interrupted``. What makes a rebuild correct
rather than merely possible: it keeps the same session_id, so the strategy still
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

import mftik_sts.session.manager as manager_mod
import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.protocol import (
    MD_SESSION_ATTACH,
    TD_SESSION_ATTACH,
    Envelope,
    MdAttachResult,
    MdAttachResultEnvelope,
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
        td: dict[str, Any] | None = None,
        td_api_ids: list[int] | None = None,
        md_ids: list[str] | dict[str, list[str]] | None = None,
        st_facts: dict[str, str] | None = None,
        finished_ago_s: float = 0.0,
        restart: str = "always",
        rebuild_count: int = 0,
        type: str | None = None,
        instance: str | None = "sts",
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
            instance=instance,
            restart=restart,
            rebuild_count=rebuild_count,
            td=td
            if td is not None
            else {
                f"account-{int(i)}": {"api_id": int(i)}
                for i in (td_api_ids or [])
            },
            # Either shape: a row written before instances holds a flat list,
            # and the compat shim has to read it. Passed through unchanged so
            # a test can seed exactly what was on disk.
            md_ids=md_ids if md_ids is not None else [],
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
    async with a_broker() as client:
        yield client


def _manager(
    broker: Broker,
    store: FakeStsStore,
    instances: list[Rebuildable],
    *,
    instance: str = "sts",
    derive_sts=None,
) -> SessionManager:
    register(Rebuildable)

    def factory(name: str | None) -> Strategy:
        s = Rebuildable()
        instances.append(s)
        return s

    return SessionManager(
        broker,
        instance=instance,
        heartbeat_interval=0.05,
        strategy_factory=factory,
        persist_live=store.persist_live,
        mark_done=store.mark_finished,
        mark_live=store.mark_live,
        list_db_sessions=store.list_sessions,
        bump_rebuild_count=store.bump_rebuild_count,
        reset_rebuild_count=store.reset_rebuild_count,
        derive_sts=derive_sts,
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
        serve(Topics.td("td"), TD_SESSION_ATTACH, "td.error"),
        serve(Topics.MD, MD_SESSION_ATTACH, "md.error"),
        return_exceptions=True,
    )


@pytest.mark.asyncio
async def test_an_interrupted_session_comes_back(broker: Broker) -> None:
    store = FakeStsStore()
    store.seed(
        "aa0001",
        td={"paper trader": {"api_id": 3}},
        md_ids=["bestquote.Paper_Spot_BTCUSDT"],
    )
    instances: list[Rebuildable] = []
    manager = _manager(broker, store, instances)

    stop = asyncio.Event()
    serving = asyncio.create_task(_serve_attaches(broker, stop))
    await asyncio.sleep(0.1)
    try:
        assert await manager.rebuild_interrupted() == ["aa0001"]
    finally:
        stop.set()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)

    row = store.rows["aa0001"]
    assert row.status == "live"
    # A session that is running again has no end and no reason for one.
    assert row.finished_at is None
    assert row.reason is None
    session = manager.get("aa0001")
    assert session is not None
    assert session.td["paper trader"].api_id == 3
    await manager.close_all()


@pytest.mark.asyncio
async def test_the_session_id_survives(broker: Broker) -> None:
    """The point of the whole thing: the strategy still owns its old orders."""
    store = FakeStsStore()
    store.seed("aa0002")
    instances: list[Rebuildable] = []
    manager = _manager(broker, store, instances)

    await manager.rebuild_interrupted()

    session = manager.get("aa0002")
    assert session is not None
    assert session.session_id == "aa0002"
    await manager.close_all()


@pytest.mark.asyncio
async def test_remembered_facts_arrive_before_any_other_hook(
    broker: Broker,
) -> None:
    """`on_rebuild` runs first, so every later hook sees restored state."""
    store = FakeStsStore()
    store.seed("aa0003", st_facts={"ref_start": "50000"})
    instances: list[Rebuildable] = []
    manager = _manager(broker, store, instances)

    await manager.rebuild_interrupted()

    strat = instances[0]
    assert strat.rebuilt_with == {"ref_start": "50000"}
    assert strat.events == ["on_rebuild", "on_start", "on_ready"]
    await manager.close_all()


@pytest.mark.asyncio
async def test_a_pre_v1_session_id_is_left_alone(broker: Broker) -> None:
    """A leftover uuid must not reach the client_order_id factory."""
    store = FakeStsStore()
    store.seed("112a28a60a0240d288641807d77a2da0")
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []
    assert store.rows["112a28a60a0240d288641807d77a2da0"].status == "interrupted"


@pytest.mark.asyncio
async def test_only_the_derived_instance_rebuilds_an_unpinned_row(
    broker: Broker,
) -> None:
    """Placement, not a claim race. Two STS boot; only the derived one takes it."""
    store = FakeStsStore()
    store.seed("aa0004", instance=None)

    async def derive(_api_ids: list[int]) -> str:
        return "sts-tw"

    tw = _manager(broker, store, [], instance="sts-tw", derive_sts=derive)
    jp = _manager(broker, store, [], instance="sts-jp", derive_sts=derive)
    try:
        assert await jp.rebuild_interrupted() == []
        assert await tw.rebuild_interrupted() == ["aa0004"]
    finally:
        await tw.close_all()
        await jp.close_all()


@pytest.mark.asyncio
async def test_a_live_session_is_not_rebuilt(broker: Broker) -> None:
    store = FakeStsStore()
    store.seed("aa0006", status="live")
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
    monkeypatch.setattr(manager_mod, "_ATTACH_BUDGET_S", 0.02)
    monkeypatch.setattr(manager_mod, "_ATTACH_BACKOFF_S", 0.01)
    store = FakeStsStore()
    store.seed("aa0005", td_api_ids=[3])
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

    assert store.rows["aa0005"].status == "interrupted"
    assert manager.get("aa0005") is None


@pytest.mark.asyncio
async def test_a_stale_session_is_left_where_it_is(broker: Broker) -> None:
    """Restoring is for a restart, where the gap is seconds to minutes.

    A session interrupted long ago would come back to a market that moved on
    and to orders the venue may have expired. Not an error and not a status
    change — the row keeps saying what happened to it, and a person can still
    decide to do something about it.
    """
    store = FakeStsStore()
    store.seed("aa0007", finished_ago_s=4000.0)
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []
    row = store.rows["aa0007"]
    assert row.status == "interrupted"
    assert row.reason == "STS shut down while this was running"


@pytest.mark.asyncio
async def test_a_session_inside_the_window_still_comes_back(
    broker: Broker,
) -> None:
    store = FakeStsStore()
    store.seed("aa0008", finished_ago_s=60.0)
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == ["aa0008"]
    await manager.close_all()


@pytest.mark.asyncio
async def test_an_unknown_age_counts_as_too_old(broker: Broker) -> None:
    """A row that says it is interrupted without saying when is not evidence
    that it stopped recently."""
    store = FakeStsStore()
    row = store.seed("aa0009")
    row.finished_at = None
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []
    assert store.rows["aa0009"].status == "interrupted"


@pytest.mark.asyncio
async def test_the_window_is_configurable(broker: Broker) -> None:
    store = FakeStsStore()
    store.seed("aa000a", finished_ago_s=4000.0)
    manager = _manager(broker, store, [])
    manager._rebuild_max_age_s = 5000.0  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["aa000a"]
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
    store.seed("aa000b", strategy="not_ready")
    manager = _manager(broker, store, [])
    manager._strategy_factory = lambda name: NotReady()  # noqa: SLF001

    assert await manager.rebuild_interrupted() == []
    assert store.rows["aa000b"].status == "interrupted"


@pytest.mark.asyncio
async def test_a_run_that_asked_not_to_come_back_stays_ended(
    broker: Broker,
) -> None:
    """Two gates already stand in front of a rebuild — the operator enabling
    it and the class supporting it. This is the third, and the only one the
    person who deployed the run controls."""
    store = FakeStsStore()
    store.seed("aa000c", restart="never")
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []
    assert store.rows["aa000c"].status == "interrupted"


@pytest.mark.asyncio
async def test_a_session_rebuilt_too_often_is_left_alone(broker: Broker) -> None:
    """A strategy that takes the process down with it would otherwise be
    restored into the same crash on every boot."""
    store = FakeStsStore()
    store.seed("aa000d", rebuild_count=3)
    manager = _manager(broker, store, [])

    assert await manager.rebuild_interrupted() == []
    assert store.rows["aa000d"].status == "interrupted"


@pytest.mark.asyncio
async def test_the_attempt_is_counted_before_it_is_made(broker: Broker) -> None:
    """Counted first, because a rebuild that never returns still has to
    count — that is the loop the cap exists to break."""
    store = FakeStsStore()
    store.seed("aa000e")
    counted: list[str] = []

    async def bump(session_id: str) -> int:
        counted.append(session_id)
        return len(counted)

    manager = _manager(broker, store, [])
    manager._bump_rebuild_count = bump  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["aa000e"]
    assert counted == ["aa000e"]
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
    store.seed("aa000f", strategy="macd_volume")

    def factory(name: str | None) -> Strategy:
        raise KeyError(f"unknown strategy {name!r}")

    manager = _manager_with_factory(broker, store, factory)

    with caplog.at_level(logging.WARNING, logger=manager_mod.__name__):
        assert await manager.rebuild_interrupted() == []

    assert store.rows["aa000f"].status == "interrupted"
    records = [r for r in caplog.records if "aa000f" in r.getMessage()]
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
    store.seed("aa0010", strategy="explodes")

    def factory(name: str | None) -> Strategy:
        raise RuntimeError("__init__ blew up")

    manager = _manager_with_factory(broker, store, factory)

    with caplog.at_level(logging.WARNING, logger=manager_mod.__name__):
        assert await manager.rebuild_interrupted() == []

    records = [r for r in caplog.records if "aa0010" in r.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert records[0].exc_info is not None


@pytest.mark.asyncio
async def test_one_unresolvable_row_does_not_stop_the_scan(
    broker: Broker
) -> None:
    """The rows are independent; one stale name must not strand the rest."""
    store = FakeStsStore()
    store.seed("aa000f", strategy="macd_volume")
    store.seed("aa0011")
    instances: list[Rebuildable] = []

    def factory(name: str | None) -> Strategy:
        if name == "macd_volume":
            raise KeyError(name)
        s = Rebuildable()
        instances.append(s)
        return s

    manager = _manager_with_factory(broker, store, factory)

    assert await manager.rebuild_interrupted() == ["aa0011"]
    assert store.rows["aa000f"].status == "interrupted"
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
    store.seed("aa0012", rebuild_count=2)
    manager = _manager(broker, store, [])
    manager._rebuild_settle_s = 0.05  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["aa0012"]
    # Counted on the way in — the reset is what takes it back down.
    assert store.rows["aa0012"].rebuild_count == 3
    await asyncio.sleep(0.15)

    assert store.rows["aa0012"].rebuild_count == 0
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
    store.seed("aa0013", rebuild_count=2)
    manager = _manager(broker, store, [])
    manager._rebuild_settle_s = 0.05  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["aa0013"]
    await manager.close(
        "aa0013", status="failed", reason="took the process down"
    )
    await asyncio.sleep(0.15)

    assert store.rows["aa0013"].rebuild_count == 3


@pytest.mark.asyncio
async def test_a_later_run_under_the_same_id_is_not_credited(
    broker: Broker,
) -> None:
    """Identity, not id: a session that stopped and was deployed again is a
    different run, and clearing the count on its behalf would credit it for
    surviving something it was never part of."""
    store = FakeStsStore()
    store.seed("aa0014", rebuild_count=1)
    manager = _manager(broker, store, [])
    manager._rebuild_settle_s = 0.05  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["aa0014"]
    rebuilt = manager.get("aa0014")
    assert rebuilt is not None
    # Stands in for the operator stopping it and deploying it again.
    manager._sessions["aa0014"] = SimpleNamespace()  # noqa: SLF001
    await asyncio.sleep(0.15)

    assert store.rows["aa0014"].rebuild_count == 2
    manager._sessions["aa0014"] = rebuilt  # noqa: SLF001
    await manager.close_all()


@pytest.mark.asyncio
async def test_a_shutdown_mid_settle_leaves_the_count_alone(
    broker: Broker,
) -> None:
    """`close_all` is interrupting these sessions, not blessing them. A timer
    that fired during teardown would clear the count of a session STS is in
    the middle of taking away."""
    store = FakeStsStore()
    store.seed("aa0015", rebuild_count=2)
    manager = _manager(broker, store, [])
    manager._rebuild_settle_s = 0.05  # noqa: SLF001

    assert await manager.rebuild_interrupted() == ["aa0015"]
    await manager.close_all()
    await asyncio.sleep(0.15)

    assert store.rows["aa0015"].rebuild_count == 3
    assert store.rows["aa0015"].status == "interrupted"


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
    store.seed("aa0016", strategy="uses_numpy", type=key)
    instances: list[Rebuildable] = []
    manager = _manager(broker, store, instances)

    assert await manager.rebuild_interrupted() == []
    assert instances == []
    assert store.rows["aa0016"].rebuild_count == 1
    assert store.rows["aa0016"].status == "interrupted"
    reset_for_tests()


@pytest.mark.asyncio
async def test_a_pinned_row_rebuilds_on_the_instance_it_names(
    broker: Broker,
) -> None:
    """A run pinned to an MD comes back on that MD, or not at all.

    The flat-list shape a row written before instances holds is covered by
    ``test_an_interrupted_session_comes_back`` above, which seeds exactly that
    and still passes — the compat shim is what keeps it passing. This is the
    other half: a row that *does* name an instance must not quietly fall back
    to the shared pool, or the pin would survive a deploy and not a restart.
    """
    store = FakeStsStore()
    store.seed("aa0017", md_ids={"md-jp-1": ["bestquote.Paper_Spot_BTCUSDT"]})
    instances: list[Rebuildable] = []
    manager = _manager(broker, store, instances)

    subjects: list[str] = []
    stop = asyncio.Event()

    async def serve(subject: str) -> None:
        async for req in broker.serve(subject, stop=stop):
            subjects.append(subject)
            await req.reply(
                MdAttachResultEnvelope.wrap(
                    MdAttachResult(
                        session_id="aa0017",
                        subscriptions=["bestquote.Paper_Spot_BTCUSDT"],
                        refcounts={},
                    ),
                    type=MD_SESSION_ATTACH,
                    source="md",
                )
            )
            return

    serving = asyncio.gather(
        serve(Topics.md("md-jp-1")),
        serve(Topics.MD),
        return_exceptions=True,
    )
    await asyncio.sleep(0.1)
    try:
        assert await manager.rebuild_interrupted() == ["aa0017"]
        assert subjects == [Topics.md("md-jp-1")], (
            "the shared pool was never asked"
        )
    finally:
        stop.set()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)
        await manager.close_all()


@pytest.mark.asyncio
async def test_a_pinned_row_is_rebuilt_only_by_the_instance_it_names(
    broker: Broker,
) -> None:
    """PI-8's placement half, and the determinism is the point.

    Every STS scans every interrupted row, because the table is shared.
    Without the filter the two below race for this session and whichever boots
    first takes it — so a run deployed to `sts-tw` comes back on `sts-jp`, and
    differently on the next restart. Placement is the whole guard.
    """
    store = FakeStsStore()
    store.seed("aa0018", instance="sts-tw", md_ids=[])
    tw = _manager(broker, store, [], instance="sts-tw")
    jp = _manager(broker, store, [], instance="sts-jp")
    try:
        assert await jp.rebuild_interrupted() == [], "not sts-jp's to take"
        assert await tw.rebuild_interrupted() == ["aa0018"]
    finally:
        await tw.close_all()
        await jp.close_all()


@pytest.mark.asyncio
async def test_a_row_pinned_to_an_instance_nobody_runs_stays_interrupted(
    broker: Broker,
) -> None:
    """It waits for a person rather than moving itself.

    The same rule as everywhere else here: the node reports the mismatch and
    does not quietly resolve it by carrying a session across a boundary
    somebody drew on purpose.
    """
    store = FakeStsStore()
    store.seed("aa0019", instance="sts-retired", md_ids=[])
    manager = _manager(broker, store, [], instance="sts-tw")
    try:
        assert await manager.rebuild_interrupted() == []
        assert store.rows["aa0019"].status == "interrupted"
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_an_unpinned_row_is_rebuilt_by_the_derived_instance(
    broker: Broker,
) -> None:
    """Null means derive, not race. The row still records what was asked."""
    store = FakeStsStore()
    store.seed("aa001a", instance=None, md_ids=[])

    async def derive(_api_ids: list[int]) -> str:
        return "sts-jp"

    manager = _manager(
        broker, store, [], instance="sts-jp", derive_sts=derive
    )
    try:
        assert await manager.rebuild_interrupted() == ["aa001a"]
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_an_unpinned_row_whose_derivation_is_not_unique_stays_interrupted(
    broker: Broker,
) -> None:
    store = FakeStsStore()
    store.seed("aa001b", instance=None, md_ids=[])

    async def nobody(_api_ids: list[int]) -> None:
        return None

    manager = _manager(
        broker, store, [], instance="sts-jp", derive_sts=nobody
    )
    try:
        assert await manager.rebuild_interrupted() == []
        assert store.rows["aa001b"].status == "interrupted"
    finally:
        await manager.close_all()
