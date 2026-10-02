"""The session worker's surface: what is real now, and what is still null.

IF-05 defines the layer and does not run it. Two kinds of test live
here, and both should pass:

* The table. Phases, delivery modes, overflow policy, hook-budget
  numbers, the report shape, ``event.age``. These are data. A later
  ticket should not have to rediscover them.
* The null. Operations a later ticket owns still raise
  ``NotImplementedError("IF-05")``. I1–I4 are real (B4-03). The queues
  are real (B5-01). Hook classification is B5-04. The log file is B5-02.

An operation that starts returning a plausible answer has to come off
the raise-list in this file in the same change that removes the
``xfail`` from the contract that describes it. A stub that quietly
returns the right queue is worse than one that refuses.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest
from mftik.protocol import (
    DEFAULT_START_TIMEOUT_S,
    DELIVERY_ALL,
    DELIVERY_KLINE,
    DELIVERY_LATEST,
    DELIVERY_MODES,
    ON_STOP_TIMEOUT_S,
    StsCreateSessionRequest,
    StsStatusProgress,
)
from mftik_sts.session_worker import (
    DEFAULT_DELIVERY,
    HOOK_HARD_S,
    HOOK_WARN_S,
    MUST_DELIVER,
    ON_READY_LIMIT_S,
    ON_STOP_LIMIT_S,
    Delivery,
    Disposition,
    HookBudgetReport,
    Inbound,
    Ingress,
    LogMark,
    LogRecord,
    Measure,
    Overflow,
    Phase,
    StrategyRunner,
    StreamKind,
    assess_hook,
    delivery_mode,
    is_lifecycle,
    kind_of_feed,
    kind_of_topic,
    overflow_policy,
    topic_of,
)

ROOT = Path(__file__).resolve().parents[1] / "src" / "mftik_sts" / "session_worker"

#: Imports that would make this package decide code identity, touch the
#: database, or serve an operator disk path. F39 / F40 put those
#: somewhere else.
FORBIDDEN_MODULES = (
    "mftik.registry",
    "mftik_db",
    "mftik_sts.app",
    "mftik_sts.registry_catchup",
    "mftik_sts.rpc",
    "mftik_sts.runtime_env",
    "mftik.envapply",
    "mftik.environment",
)
FORBIDDEN_FIELDS = frozenset({"strategy_digest", "env_generation", "code_ref"})


def _spec() -> StsCreateSessionRequest:
    return StsCreateSessionRequest(
        session_id="abc123", created_by=1, strategy="noop"
    )


def _ticker(event_id: str = "e") -> Inbound:
    return Inbound(
        kind=StreamKind.TICKER,
        feed="ticker.Paper_Spot_BTCUSDT",
        recv_ts=1.0,
        body=b"{}",
        event_id=event_id,
        seq=1,
    )


# --- the table -------------------------------------------------------------


def test_phases_are_zero_through_six_in_order() -> None:
    assert [int(phase) for phase in Phase] == list(range(7))
    assert list(Phase) == [
        Phase.BOOT,
        Phase.LOAD,
        Phase.ON_START,
        Phase.READY,
        Phase.RUNNING,
        Phase.STOPPING,
        Phase.TEARDOWN,
    ]


def test_log_marks_are_the_three_words() -> None:
    assert {mark.value for mark in LogMark} == {
        "delivered",
        "superseded",
        "dropped",
    }


def test_the_default_delivery_table_is_the_plan() -> None:
    """§5.3, one row per group. The strings are IF-07's mode names."""
    latest = {
        StreamKind.TICKER,
        StreamKind.BESTQUOTE,
        StreamKind.GREEKS,
        StreamKind.FUNDING,
        StreamKind.OPEN_INTEREST,
        StreamKind.ORDERBOOK,
    }
    market_all = {StreamKind.TRADE, StreamKind.AGGTRADE, StreamKind.LIQUIDATION}
    assert set(DEFAULT_DELIVERY) == set(StreamKind)
    got_latest = {
        kind for kind, mode in DEFAULT_DELIVERY.items() if mode == DELIVERY_LATEST
    }
    assert got_latest == latest
    assert DEFAULT_DELIVERY[StreamKind.KLINE] == DELIVERY_KLINE
    assert {
        kind for kind, mode in DEFAULT_DELIVERY.items() if mode == DELIVERY_ALL
    } == market_all | MUST_DELIVER
    assert set(DEFAULT_DELIVERY.values()) <= set(DELIVERY_MODES)


@pytest.mark.parametrize("kind", list(StreamKind))
def test_delivery_mode_defaults_when_nothing_overrides(
    kind: StreamKind,
) -> None:
    assert delivery_mode(kind) == DEFAULT_DELIVERY[kind]
    assert delivery_mode(kind) in DELIVERY_MODES


def test_must_deliver_ignores_an_override_and_fails_on_overflow() -> None:
    """TD, feed_end and RPC replies are not feeds. A ``delivery:`` line
    cannot make them droppable."""
    for kind in MUST_DELIVER:
        assert delivery_mode(kind, DELIVERY_LATEST) == DELIVERY_ALL
        assert overflow_policy(kind, DELIVERY_LATEST) is Overflow.FAIL


def test_an_override_moves_a_feed_between_conflate_and_drop() -> None:
    assert overflow_policy(StreamKind.TICKER) is Overflow.CONFLATE
    assert overflow_policy(StreamKind.KLINE) is Overflow.CONFLATE
    assert overflow_policy(StreamKind.TRADE) is Overflow.DROP_OLDEST
    assert delivery_mode(StreamKind.TRADE, DELIVERY_LATEST) == DELIVERY_LATEST
    assert overflow_policy(StreamKind.TRADE, DELIVERY_LATEST) is Overflow.CONFLATE
    assert delivery_mode(StreamKind.TICKER, DELIVERY_ALL) == DELIVERY_ALL


def test_an_unknown_override_is_refused() -> None:
    with pytest.raises(ValueError, match="sometimes"):
        delivery_mode(StreamKind.TICKER, "sometimes")
    with pytest.raises(ValueError, match="sometimes"):
        Delivery(capacity=1, overrides={"ticker.Paper_Spot_BTCUSDT": "sometimes"})


def test_feed_keys_map_onto_kinds() -> None:
    assert topic_of("ticker.Paper_Spot_BTCUSDT") == "ticker"
    assert topic_of("kline_1m.Paper_Spot_BTCUSDT") == "kline_1m"
    assert kind_of_feed("ticker.Paper_Spot_BTCUSDT") is StreamKind.TICKER
    assert kind_of_feed("kline_1m.Paper_Spot_BTCUSDT") is StreamKind.KLINE
    assert kind_of_feed("funding_rate.Paper_Perp_BTCUSDT") is StreamKind.FUNDING
    assert kind_of_topic("open_interest") is StreamKind.OPEN_INTEREST
    assert kind_of_topic("bestquote") is StreamKind.BESTQUOTE
    with pytest.raises(ValueError):
        topic_of("ticker")
    with pytest.raises(ValueError):
        kind_of_topic("candle")


def test_mode_of_applies_a_feed_override() -> None:
    feed = "ticker.Paper_Spot_BTCUSDT"
    event = _ticker()
    assert Delivery(capacity=1).mode_of(event) == DELIVERY_LATEST
    overridden = Delivery(capacity=1, overrides={feed: DELIVERY_ALL})
    assert overridden.mode_of(event) == DELIVERY_ALL
    td = Inbound(
        kind=StreamKind.TD, feed="td", recv_ts=1.0, body=b"", event_id="t"
    )
    assert overridden.mode_of(td) == DELIVERY_ALL


@pytest.mark.parametrize("capacity", [0, -1, True, 1.5])
def test_capacity_has_to_be_a_positive_integer(capacity: object) -> None:
    with pytest.raises(ValueError):
        Delivery(capacity=capacity)  # type: ignore[arg-type]


def test_the_budget_lines_are_the_plan() -> None:
    assert HOOK_WARN_S == 1
    assert HOOK_HARD_S == 30
    assert ON_READY_LIMIT_S == 10
    assert ON_STOP_LIMIT_S == 10
    assert ON_STOP_LIMIT_S == ON_STOP_TIMEOUT_S
    assert DEFAULT_START_TIMEOUT_S == 60


def test_the_budget_is_not_a_strategy_setting() -> None:
    """No ``limits.hook_timeout_s``. The lines are the table's."""
    parameters = inspect.signature(assess_hook).parameters
    assert "hook_timeout_s" not in parameters
    assert "limits" not in parameters
    assert is_lifecycle("on_start")
    assert is_lifecycle("on_ready")
    assert is_lifecycle("on_stop")
    assert not is_lifecycle("on_ticker")
    assert not is_lifecycle("on_timer")


def test_the_report_names_the_hook_and_how_long() -> None:
    """The status progress IF-01 already defined, filled from this report."""
    warning = HookBudgetReport(
        hook="on_ticker",
        measure=Measure.BLOCKED,
        elapsed_s=1.5,
        limit_s=HOOK_WARN_S,
        disposition=Disposition.WARN,
    )
    assert warning.ends_session is False
    assert warning.restarts is False
    assert warning.as_progress(dropped=3) == StsStatusProgress(
        hook="on_ticker", elapsed_s=1.5, dropped=3
    )
    crash = HookBudgetReport(
        hook="on_ticker",
        measure=Measure.BLOCKED,
        elapsed_s=31,
        limit_s=HOOK_HARD_S,
        disposition=Disposition.CRASH_B,
    )
    assert crash.ends_session is True
    assert crash.restarts is False


def test_age_is_recv_ts_against_the_bound_clock() -> None:
    """``event.age`` is how long the ingress has been holding it.

    No clock bound means unknown, not zero: zero would look like a
    print that just arrived.
    """
    unbound = _ticker()
    assert unbound.age is None
    bound = Inbound(
        kind=StreamKind.TICKER,
        feed="ticker.Paper_Spot_BTCUSDT",
        recv_ts=10.0,
        body=b"",
        event_id="e",
        seq=4,
        clock=lambda: 12.0,
    )
    assert bound.age == 2.0
    assert bound.recv_ts == 10.0
    assert bound.seq == 4


def test_a_log_record_can_carry_a_mark() -> None:
    record = LogRecord(
        event_id="e",
        log_seq=1,
        recv_ts=10.0,
        body=b"raw",
        kind=StreamKind.TRADE,
        feed="trade.Paper_Spot_BTCUSDT",
        event_seq=3,
        mark=LogMark.DROPPED,
    )
    assert record.mark is LogMark.DROPPED
    assert record.event_seq == 3
    assert record.log_seq == 1


# --- null ------------------------------------------------------------------


def test_the_worker_is_handed_a_spec_without_code_identity() -> None:
    ingress = Ingress(_spec(), capacity=2, start_timeout_s=15)
    assert ingress.spec.session_id == "abc123"
    assert ingress.start_timeout_s == 15
    assert "strategy_digest" not in type(ingress.spec).model_fields
    assert "env_generation" not in type(ingress.spec).model_fields


def test_a_fresh_worker_returns_null() -> None:
    ingress = Ingress(_spec(), capacity=2)
    runner = StrategyRunner(ingress, _strategy())
    lane = ingress.delivery
    assert ingress.phase is None
    assert ingress.exit_code is None
    assert ingress.thread is None
    assert ingress.pull() is None
    assert ingress.progress() is None
    assert ingress.log_records() == ()
    assert runner.alive is False
    assert runner.thread is None
    assert lane.take() is None
    assert lane.dropped == 0
    assert lane.failed is False
    assert lane.fail_reason is None
    assert lane.mark("missing") is None
    assert lane.warnings() == ()
    assert lane.log_records() == ()


def test_operations_refuse_with_the_ticket_number() -> None:
    """Delete a line when the ticket that owns it starts doing the work.

    I1–I4 are B4-03. The queues are B5-01. Hook classification is
    B5-04. The log file is B5-02, and it is not a method here yet.
    """
    ingress = Ingress(_spec(), capacity=2)
    runner = StrategyRunner(ingress, _strategy())
    calls = [
        lambda: runner.note_hook("on_ticker", 1.5),
        lambda: assess_hook("on_ticker", 1.5),
    ]
    for call in calls:
        with pytest.raises(NotImplementedError, match="IF-05"):
            call()


def test_accept_refuses_a_kline_with_no_bar_open_and_an_event_with_no_id() -> None:
    """Those two are how the queue would place the event. They are
    checked before the not-implemented queue, so a caller finds out
    which one it forgot."""
    lane = Delivery(capacity=1)
    with pytest.raises(ValueError, match="bar_open"):
        lane.accept(
            Inbound(
                kind=StreamKind.KLINE,
                feed="kline_1m.Paper_Spot_BTCUSDT",
                recv_ts=1.0,
                body=b"",
                event_id="k",
            )
        )
    with pytest.raises(ValueError, match="event_id"):
        lane.accept(
            Inbound(
                kind=StreamKind.TICKER,
                feed="ticker.Paper_Spot_BTCUSDT",
                recv_ts=1.0,
                body=b"",
                event_id="",
            )
        )


def test_the_platform_does_not_offer_a_feed_gap() -> None:
    """F23. A hole in ``seq`` is the strategy's to notice."""
    assert not hasattr(Ingress, "on_feed_gap")
    assert not hasattr(StrategyRunner, "on_feed_gap")
    assert not hasattr(Delivery, "record_gap")
    assert not hasattr(Delivery, "gaps")


def test_the_package_is_actually_scanned() -> None:
    files = list(ROOT.glob("*.py"))
    assert len(files) >= 6, f"only {len(files)} files under {ROOT}"


def test_it_does_not_import_the_registry_the_database_or_the_plane() -> None:
    found: list[str] = []
    for path in sorted(ROOT.glob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            for module in modules:
                for banned in FORBIDDEN_MODULES:
                    if module == banned or module.startswith(banned + "."):
                        found.append(f"{path.name}:{node.lineno} imports {module}")
    assert found == []


def test_it_does_not_declare_code_identity_fields() -> None:
    found: list[str] = []
    for path in sorted(ROOT.glob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id in FORBIDDEN_FIELDS
            ):
                found.append(f"{path.name}:{node.lineno} {node.target.id}")
    assert found == []


def _strategy():
    from mftik.strategy import Strategy

    return Strategy()
