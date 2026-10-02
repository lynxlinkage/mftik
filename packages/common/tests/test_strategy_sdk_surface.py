"""The SDK surface IF-06 defines, and that it only returns null data yet.

Two things are being pinned here. The first is that every name the plan's §3.4
SDK row promises is reachable and shaped the way a strategy will be written
against it — a hook that takes the wrong number of arguments, or an accessor
spelled ``self.mds`` when the docs say ``self.md``, is a contract broken before
anything is implemented.

The second is that none of it pretends to work. An interface ticket that
accidentally returns a plausible value is worse than one that raises: a
strategy written against ``self.md.state(feed)`` answering ``"live"`` for every
feed would look correct until the day a feed went down. So the stubs answer
``None``, an empty set, or ``NotImplementedError("IF-06")``, and that is checked
rather than assumed.

The behaviour these stand in for is in ``test_strategy_sdk_contract.py``, as
xfail.
"""

from __future__ import annotations

import inspect

import mftik.strategy as sdk
import pytest
from mftik.protocol.reject_codes import RejectCode, is_td_internal
from mftik.strategy import (
    AccountState,
    FeedState,
    HookSlow,
    NotReady,
    OffloadPool,
    OffloadWorkerLost,
    Ready,
    Strategy,
    StrategyHarness,
    UniverseChange,
)

#: Every hook §3.4 adds, and how many arguments the platform calls it with.
NEW_HOOKS = {
    "on_ready": 1,
    "on_md_update": 3,
    "on_td_update": 3,
    "on_resync": 3,
    "on_universe_change": 2,
}


@pytest.mark.parametrize("name", sorted(NEW_HOOKS))
def test_every_new_hook_is_an_async_no_op(name: str) -> None:
    """A strategy overrides what it cares about and inherits the rest."""
    hook = getattr(Strategy, name)
    assert inspect.iscoroutinefunction(hook)
    params = list(inspect.signature(hook).parameters)
    assert params[0] == "self"
    assert len(params) - 1 == NEW_HOOKS[name]


async def test_the_default_hooks_do_nothing() -> None:
    strategy = Strategy()
    assert await strategy.on_ready(Ready()) is None
    assert await strategy.on_md_update("ticker.Paper_Spot_BTCUSDT", "down", "x") is None
    assert await strategy.on_td_update(1, "unavailable", "x") is None
    assert await strategy.on_universe_change("btc_chain", UniverseChange()) is None


def test_on_ready_takes_the_report() -> None:
    """``on_ready(ready)``, not ``on_ready()`` — F12's missing-feed list has to
    reach the strategy that decides what to do about it."""
    params = inspect.signature(Strategy.on_ready).parameters
    assert "ready" in params


def test_ready_is_empty_by_default() -> None:
    assert Ready().missing_feeds == ()


def test_a_universe_change_is_empty_by_default() -> None:
    change = UniverseChange()
    assert change.added == ()
    assert change.removed == ()
    assert change.epoch == 0
    assert change.current is None


def test_the_availability_accessors_are_bound_and_separate() -> None:
    """``self.md`` is availability; ``self.mds`` is a venue query. Two names one
    letter apart, so this is worth a test rather than a comment."""
    strategy = Strategy()
    assert isinstance(strategy.md, sdk.md.StrategyMd)
    assert isinstance(strategy.td, sdk.td.StrategyTd)
    assert isinstance(strategy.mds, sdk.mds.StrategyMds)
    assert strategy.md is not strategy.mds


def test_md_reads_are_null_data() -> None:
    md = sdk.md.StrategyMd()
    assert md.state("ticker.Paper_Spot_BTCUSDT") is None
    assert md.universe("btc_chain") == frozenset()
    assert md.current("btc_q") is None


async def test_md_subscribe_is_not_implemented_yet() -> None:
    with pytest.raises(NotImplementedError, match="IF-06"):
        await sdk.md.StrategyMd().subscribe("ticker.Paper_Spot_BTCUSDT")


def test_td_state_is_null_data() -> None:
    assert sdk.td.StrategyTd().state(42) is None


async def test_offload_is_not_implemented_yet() -> None:
    strategy = Strategy()
    with pytest.raises(NotImplementedError, match="IF-06"):
        await strategy.offload(len, [1, 2, 3])
    with pytest.raises(NotImplementedError, match="IF-06"):
        await strategy.offload(len, [1, 2, 3], isolate=True)
    with pytest.raises(NotImplementedError, match="IF-06"):
        await strategy.offload_pool(init=len, init_args=([],))


async def test_an_offload_pool_call_is_not_implemented_yet() -> None:
    pool = OffloadPool()
    with pytest.raises(NotImplementedError, match="IF-06"):
        await pool.call(len, [])
    assert await pool.close() is None


async def test_a_settled_oms_view_needs_a_session() -> None:
    """The settled read is real. An unbound OMS has nothing to ask.

    It must not answer an empty book, and it is no longer the IF-06 stub.
    """
    with pytest.raises(RuntimeError, match="not bound"):
        await sdk.oms.StrategyOms().view(1, settled=True)


def test_hook_slow_counts_nothing_yet() -> None:
    counter = HookSlow()
    assert counter.count == 0
    assert dict(counter.by_hook()) == {}
    with pytest.raises(NotImplementedError, match="IF-06"):
        counter.note("on_ticker", 1.5)


def test_the_exceptions_are_errors_a_strategy_can_catch() -> None:
    assert issubclass(NotReady, Exception)
    assert issubclass(OffloadWorkerLost, Exception)


def test_td_unavailable_is_a_td_side_refusal() -> None:
    """The band is the contract: ``1xx`` means nothing was sent (§5.6)."""
    assert is_td_internal(RejectCode.TD_UNAVAILABLE)


def test_the_states_are_the_words_the_plan_uses() -> None:
    assert set(FeedState.__args__) == {"live", "down"}
    assert set(AccountState.__args__) == {"ready", "degraded", "unavailable"}


@pytest.mark.parametrize(
    "name",
    [
        "start",
        "ready",
        "stop",
        "feed",
        "account_event",
        "md_update",
        "td_update",
        "resync",
        "universe_change",
    ],
)
def test_the_harness_drives_a_strategy_through_every_entry_point(name: str) -> None:
    """The API B5-08 will build against. Binding is real; driving is not."""
    method = getattr(StrategyHarness, name)
    assert inspect.iscoroutinefunction(method)


async def test_the_harness_records_nothing_yet() -> None:
    harness = StrategyHarness(Strategy(), td={"main": 1}, md=["ticker.X"])
    assert harness.submitted == ()
    assert harness.cancelled == ()
    assert harness.view(1).orders == {}
    with pytest.raises(NotImplementedError, match="IF-06"):
        await harness.start()
