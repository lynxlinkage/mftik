"""What the SDK will do, written down before it does it (IF-06).

Every test here is ``xfail(strict=True)``: it describes behaviour the plan
settles but no ticket has built yet, and it fails today because the surface
IF-06 added returns null data. ``strict`` is the point — the ticket that
implements one of these cannot merge while the marker is still on it, so the
description becomes the test rather than being replaced by one somebody
remembers to write.

The ticket that owns each is named in its ``reason``. They are grouped by the
decision they come from rather than by module, because that is how they will be
read: a reviewer of B5-05 wants §5.6's rules in one place, not scattered across
the accessor they happen to touch.

Nothing here reaches a plane. Each drives a strategy through
:class:`~mftik.strategy.harness.StrategyHarness`, which is the other thing this
file pins down: if the behaviour cannot be described through the harness, the
harness is the wrong shape (§9.2).
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mftik.exchange.models import OrderStatus, Side, Ticker
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol.reject_codes import RejectCode
from mftik.strategy import (
    NotReady,
    OffloadWorkerLost,
    Ready,
    Strategy,
    StrategyHarness,
    UniverseChange,
)

API_ID = 7
FEED = "ticker.Paper_Spot_BTCUSDT"
TICKER = "Paper_Spot_BTCUSDT"


def _ticker(price: str = "100") -> Ticker:
    return Ticker(
        universal_ticker=TICKER,
        bid=Decimal(price),
        ask=Decimal(price),
        last=Decimal(price),
    )


class Recorder(Strategy):
    """A strategy that keeps what it was told, so a test can read it back."""

    def __init__(self) -> None:
        super().__init__()
        self.ready_seen: list[Ready] = []
        self.md_seen: list[tuple[str, str, str]] = []
        self.td_seen: list[tuple[int, str, str]] = []
        self.resyncs: list[tuple[int, str]] = []
        self.universes: list[tuple[str, UniverseChange]] = []
        self.tickers: list[Ticker] = []

    async def on_ready(self, ready: Ready) -> None:
        self.ready_seen.append(ready)

    async def on_md_update(self, feed: str, state: str, reason: str) -> None:
        self.md_seen.append((feed, state, reason))

    async def on_td_update(self, api_id: int, state: str, reason: str) -> None:
        self.td_seen.append((api_id, state, reason))

    async def on_resync(self, api_id: int, cause: str, view) -> None:
        self.resyncs.append((api_id, cause))

    async def on_universe_change(self, name: str, change: UniverseChange) -> None:
        self.universes.append((name, change))

    async def on_ticker(self, ticker: Ticker) -> None:
        self.tickers.append(ticker)


def _harness(strategy: Strategy | None = None) -> StrategyHarness:
    return StrategyHarness(
        strategy or Recorder(), td={"main": API_ID}, md=[FEED]
    )


# --- F12: the lifecycle gate ----------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B5-08 StrategyHarness: the worker already gates order entry; "
        "harness.start still raises IF-06"
    ),
)
async def test_submitting_before_on_ready_raises_not_ready() -> None:
    """``on_start`` runs before any account has reconciled, so a strategy there
    does not know what it holds. The SDK refuses rather than send (F12)."""
    harness = _harness()
    await harness.start()
    with pytest.raises(NotReady):
        await harness.strategy.oms.submit_order(
            API_ID, ticker=TICKER, side=Side.BUY, qty=Decimal("1")
        )
    assert harness.submitted == ()


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B5-08 StrategyHarness: the worker already allows order entry "
        "from on_ready; harness.ready still raises IF-06"
    ),
)
async def test_submitting_after_on_ready_is_allowed() -> None:
    harness = _harness()
    await harness.start()
    await harness.ready()
    assert await harness.strategy.oms.submit_order(
        API_ID, ticker=TICKER, side=Side.BUY, qty=Decimal("1")
    )
    assert len(harness.submitted) == 1


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B5-08 StrategyHarness: the worker already passes "
        "Ready.missing_feeds; harness.ready still raises IF-06"
    ),
)
async def test_on_ready_names_the_feeds_that_did_not_arrive() -> None:
    """MD is a soft condition: the session starts anyway and says what is
    missing, because only the strategy can judge whether it can work (F12)."""
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready(missing_feeds=[FEED])
    assert strategy.ready_seen[0].missing_feeds == (FEED,)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "B5-08 StrategyHarness: the worker already calls on_ready once; "
        "harness.ready still raises IF-06"
    ),
)
async def test_on_ready_fires_once() -> None:
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready()
    await harness.md_update(FEED, "down")
    await harness.md_update(FEED, "live")
    assert len(strategy.ready_seen) == 1


# --- F14 / §5.6: losing MD or TD ------------------------------------------


@pytest.mark.xfail(strict=True, reason="B5-05 refuses on an unavailable account")
async def test_submitting_to_an_unavailable_account_is_refused_locally() -> None:
    """False, ``td_unavailable``, and nothing on the wire — the same meaning
    ``False`` has always had: it did not reach the venue (§5.6)."""
    harness = _harness()
    await harness.start()
    await harness.ready()
    await harness.td_update(API_ID, "unavailable", "worker restarting")
    accepted = await harness.strategy.oms.submit_order(
        API_ID, ticker=TICKER, side=Side.BUY, qty=Decimal("1")
    )
    assert accepted is False
    assert harness.strategy.oms.last_reject_code == RejectCode.TD_UNAVAILABLE
    assert harness.submitted == ()


@pytest.mark.xfail(strict=True, reason="B5-05 keeps sending on a degraded account")
async def test_a_degraded_account_still_takes_orders() -> None:
    """``degraded`` is late confirmations, not a closed door. A strategy that
    has to flatten is better served by a slow answer than by a refusal."""
    harness = _harness()
    await harness.start()
    await harness.ready()
    await harness.td_update(API_ID, "degraded", "private stream dropped")
    assert await harness.strategy.oms.submit_order(
        API_ID, ticker=TICKER, side=Side.BUY, qty=Decimal("1")
    )


@pytest.mark.xfail(strict=True, reason="B5-05 notifies instead of failing")
async def test_losing_a_feed_notifies_and_does_not_end_the_session() -> None:
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready()
    await harness.md_update(FEED, "down", "venue socket closed")
    assert strategy.md_seen == [(FEED, "down", "venue socket closed")]
    assert strategy.md.state(FEED) == "down"
    await harness.md_update(FEED, "live", "resubscribed")
    assert strategy.md.state(FEED) == "live"


@pytest.mark.xfail(strict=True, reason="B5-05 broadcasts account state")
async def test_an_account_state_is_readable_as_well_as_pushed() -> None:
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready()
    await harness.td_update(API_ID, "unavailable", "incarnation changed")
    assert strategy.td_seen == [(API_ID, "unavailable", "incarnation changed")]
    assert strategy.td.state(API_ID) == "unavailable"


# --- F13: resync ----------------------------------------------------------


@pytest.mark.xfail(strict=True, reason="B5-05 triggers on_resync")
async def test_a_rebuilt_account_resyncs_before_it_is_ready_again() -> None:
    """The order matters: a strategy must have corrected its own picture before
    it is told it may trade again (§5.6)."""
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready()
    await harness.td_update(API_ID, "unavailable", "worker replaced")
    await harness.resync(API_ID, "account_reset")
    await harness.td_update(API_ID, "ready", "recon complete")
    assert strategy.resyncs == [(API_ID, "account_reset")]
    assert [state for _, state, _ in strategy.td_seen] == [
        "unavailable",
        "ready",
    ]


@pytest.mark.xfail(strict=True, reason="IF-11 serves oms.view(settled=True)")
async def test_a_settled_view_waits_for_unknown_orders_to_resolve() -> None:
    """What replaced ``send_recon`` / ``on_recon_done``: a read that does not
    answer until the book says what the account holds (F13)."""
    harness = _harness()
    await harness.start()
    await harness.ready()
    view = await harness.strategy.oms.view(API_ID, settled=True)
    assert all(
        order.status is not OrderStatus.UNKNOWN for order in view.orders.values()
    )


# --- F33 / I-SEL1: selector universes -------------------------------------


@pytest.mark.xfail(strict=True, reason="B9 applies universe changes (I-SEL1)")
async def test_no_event_for_a_contract_before_it_is_added() -> None:
    """I-SEL1, first half. A contract the strategy has not been told about must
    not reach a hook — the strategy has nothing set up for it yet."""
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready()
    await harness.feed(_ticker())
    assert strategy.tickers == []
    await harness.universe_change(
        "btc_chain",
        UniverseChange(added=(UniversalTicker.parse(TICKER),), epoch=1),
    )
    await harness.feed(_ticker())
    assert len(strategy.tickers) == 1


@pytest.mark.xfail(strict=True, reason="B9 applies universe changes (I-SEL1)")
async def test_no_event_for_a_contract_after_it_is_removed() -> None:
    """I-SEL1, second half — including events already queued when the change
    arrived, which are dropped rather than delivered late."""
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready()
    contract = UniversalTicker.parse(TICKER)
    await harness.universe_change(
        "btc_chain", UniverseChange(added=(contract,), epoch=1)
    )
    await harness.feed(_ticker())
    await harness.universe_change(
        "btc_chain", UniverseChange(removed=(contract,), epoch=2)
    )
    delivered = len(strategy.tickers)
    await harness.feed(_ticker())
    assert len(strategy.tickers) == delivered


@pytest.mark.xfail(strict=True, reason="B9 keeps the universe readable")
async def test_the_universe_is_readable_after_a_change() -> None:
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready()
    contract = UniversalTicker.parse(TICKER)
    await harness.universe_change(
        "btc_chain", UniverseChange(added=(contract,), epoch=1)
    )
    assert strategy.md.universe("btc_chain") == frozenset({contract})


@pytest.mark.xfail(strict=True, reason="B9 rolls a future's current contract")
async def test_a_roll_moves_current_and_keeps_the_old_contract() -> None:
    """The contract rolled off stays until it expires, so both books are live
    across a roll and ``current`` is the only thing that moved (§6.4)."""
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready()
    front = UniversalTicker.parse("Deribit_Future_BTCUSD-260626")
    back = UniversalTicker.parse("Deribit_Future_BTCUSD-260925")
    await harness.universe_change(
        "btc_q", UniverseChange(added=(front,), epoch=1, current=front)
    )
    await harness.universe_change(
        "btc_q", UniverseChange(added=(back,), epoch=2, current=back)
    )
    assert strategy.md.current("btc_q") == back
    assert strategy.md.universe("btc_q") == frozenset({front, back})


@pytest.mark.xfail(strict=True, reason="B8-05 serves a runtime subscribe")
async def test_a_runtime_subscribe_does_not_disturb_readiness() -> None:
    """A feed added while running reports through ``on_md_update``, because
    ``on_ready`` has already happened (F12)."""
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready()
    assert await strategy.md.subscribe("trade.Paper_Spot_ETHUSDT")
    assert len(strategy.ready_seen) == 1


# --- F9 / §5.5: offload ---------------------------------------------------


@pytest.mark.xfail(strict=True, reason="B5-03 builds the offload pools")
async def test_offload_runs_the_work_and_returns_its_result() -> None:
    harness = _harness()
    await harness.start()
    assert await harness.strategy.offload(sum, [1, 2, 3]) == 6
    assert await harness.strategy.offload(sum, [1, 2, 3], isolate=True) == 6


@pytest.mark.xfail(strict=True, reason="B5-03 builds the offload pools")
async def test_an_offload_pool_keeps_what_init_loaded() -> None:
    """The point of a pool over ``isolate=True``: the expensive thing is loaded
    once in the child instead of pickled on every call (§5.5)."""
    harness = _harness()
    await harness.start()
    pool = await harness.strategy.offload_pool(init=dict, init_args=())
    assert await pool.call(len) == 0
    await pool.close()


@pytest.mark.xfail(strict=True, reason="B5-03 reports a lost worker")
async def test_a_lost_offload_worker_raises_rather_than_answers() -> None:
    """A child killed for memory must not look like a result, and must not take
    the session with it either — isolation is the reason for the mode."""
    harness = _harness()
    await harness.start()
    with pytest.raises(OffloadWorkerLost):
        await harness.strategy.offload(_killed_in_the_child, isolate=True)


def _killed_in_the_child() -> None:
    """Module level, because a process-mode offload has to pickle it."""
    import os
    import signal

    os.kill(os.getpid(), signal.SIGKILL)


# --- F15: hook time budget ------------------------------------------------


@pytest.mark.xfail(strict=True, reason="B5-04 measures blocked time")
async def test_a_slow_hook_is_counted_and_not_killed() -> None:
    """Past the warning line a hook is counted and logged, and that is all:
    the ingress has the socket, so a slow strategy only slows itself (F15)."""
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready()
    await harness.feed(_ticker())
    assert strategy.hook_slow.count == 0
    strategy.hook_slow.note("on_ticker", 1.5)
    assert strategy.hook_slow.count == 1
    assert dict(strategy.hook_slow.by_hook()) == {"on_ticker": 1}


@pytest.mark.xfail(strict=True, reason="B5-04 excludes offload from blocked time")
async def test_time_spent_in_offload_is_not_blocked_time() -> None:
    """``await self.offload(...)`` leaves the loop empty, so a hook that uses it
    is not late however long the computation takes (F15, §5.5)."""
    strategy = Recorder()
    harness = _harness(strategy)
    await harness.start()
    await harness.ready()
    await strategy.offload(sum, range(1000))
    assert strategy.hook_slow.count == 0


# --- I1: the harness keeps the teardown order -----------------------------


@pytest.mark.xfail(strict=True, reason="B5-08 builds the harness")
async def test_on_stop_can_still_cancel_and_be_answered() -> None:
    """I1: the ingress outlives the strategy, so a cancel from ``on_stop`` gets
    its reply and any fill that beat it (§5.3)."""

    class Closer(Strategy):
        async def on_stop(self) -> None:
            await self.oms.cancel_order(API_ID, "1")

    harness = StrategyHarness(Closer(), td={"main": API_ID}, md=[FEED])
    await harness.start()
    await harness.ready()
    await harness.stop()
    assert len(harness.cancelled) == 1
    assert harness.cancelled[0].accepted
