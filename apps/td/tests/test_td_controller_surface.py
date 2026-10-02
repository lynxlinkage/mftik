"""The TD controller interface IF-12 defines.

The shape is real: an account binding, a trading bit, an action, and the
worker spec procman is allowed to see (``restart`` is ``on_failure``).
The account set, the trading level, the spawn gate, reconcile and
intent put / delete answer (B4-07, B3-03). Drain-replace still raises
``NotImplementedError("IF-12")``.

What B3 and B6-04 still have to make true is in
``test_td_controller_contract.py``, as xfail.
"""

from __future__ import annotations

import ast
import inspect
from dataclasses import fields
from pathlib import Path

import pytest
from mftik.exchange.venues import UnknownVenueError
from mftik.procman import (
    OOM_SCORE_ADJ,
    CloseMode,
    ObservedWorker,
    ReattachAction,
    RestartIntensity,
    Supervisor,
    WorkerPhase,
    plan_restart,
)
from mftik.protocol import (
    TD_ERROR,
    TD_INTENT_DELETE,
    TD_INTENT_PUT,
    Envelope,
    IntentOwner,
    RpcError,
    TdIntentDelete,
    TdIntentPut,
)
from mftik_td.account import AccountWorker
from mftik_td.controller import (
    ACCOUNT_KIND,
    FIRST_INCARNATION,
    INTENT_TYPES,
    TICKET,
    AccountView,
    ActionKind,
    BoundAccount,
    OrchestratorAction,
    TdIntentBook,
    TdOrchestrator,
    TradingDesired,
    account_pid_gone,
    account_worker_id,
    account_worker_spec,
    apply_delete,
    apply_put,
    close_actions,
    control_subject,
    desired_accounts,
    intent_handler,
    observation_view,
    plan_account_restart,
    release_named,
    spawn_allowed,
    td_reattach,
    trading_active,
    trading_pushes,
)

_ROOT = Path(__file__).resolve().parents[1] / "src" / "mftik_td"
_CONTROLLER = _ROOT / "controller"


def _sources() -> list[ast.AST]:
    return [ast.parse(path.read_text()) for path in sorted(_CONTROLLER.glob("*.py"))]


def _intensity() -> RestartIntensity:
    """Numbers the test supplies. The package does not have its own."""
    return RestartIntensity(max_restarts=2, window_s=30, min_backoff_s=0.5)


def _orch(tmp_path: Path, intensity: RestartIntensity | None = None) -> TdOrchestrator:
    return TdOrchestrator(
        Supervisor(tmp_path, plane="td", instance="td"),
        intensity=intensity or _intensity(),
        code_ref="v1",
    )


def _account(**overrides: object) -> BoundAccount:
    raw: dict[str, object] = {"api_id": 7, "venue": "Paper", "instance": "td"}
    raw.update(overrides)
    return BoundAccount(**raw)  # type: ignore[arg-type]


def _spec(account: BoundAccount | None = None, **overrides: object):
    raw: dict[str, object] = {
        "incarnation": 1,
        "argv": ("python",),
        "code_ref": "v1",
        "start_timeout_s": 60,
        "hb_timeout_s": 3,
        "stop_grace_s": 8,
    }
    raw.update(overrides)
    return account_worker_spec(account or _account(), **raw)  # type: ignore[arg-type]


def _put(*api_ids: int) -> TdIntentPut:
    return TdIntentPut(
        session_id="abc123",
        owner=IntentOwner(sts_instance="sts", session_id="abc123"),
        api_ids=list(api_ids),
    )


def test_ticket_id_is_the_stub_message() -> None:
    assert TICKET == "IF-12"


def test_the_worker_id_matches_the_account_worker() -> None:
    assert account_worker_id(7) == "td/account/7"
    assert account_worker_id(7) == AccountWorker(7, venue="Paper").worker_id
    assert FIRST_INCARNATION == 1
    assert ACCOUNT_KIND == "account"


def test_an_intent_is_not_an_argument_of_desired_accounts() -> None:
    """F35: the worker set is the binding, not who currently holds an intent."""
    assert "intents" not in inspect.signature(desired_accounts).parameters


def test_a_binding_is_a_venue_on_an_instance() -> None:
    account = _account(venue="binanceum", instance="td-jp")
    assert account.api_id == 7
    assert account.venue == "BinanceUM"
    assert account.instance == "td-jp"
    assert "cancel_on_disconnect" not in {item.name for item in fields(BoundAccount)}


@pytest.mark.parametrize("api_id", [0, -1, True, "7"])
def test_api_id_has_to_be_a_positive_int(api_id: object) -> None:
    with pytest.raises(ValueError):
        _account(api_id=api_id)


def test_an_unknown_venue_or_a_bad_instance_is_refused() -> None:
    with pytest.raises(UnknownVenueError):
        _account(venue="NotAVenue")
    with pytest.raises(ValueError):
        _account(instance="TD-JP")


def test_a_trading_bit_is_a_bool() -> None:
    assert TradingDesired(api_id=7, active=True).active is True
    with pytest.raises(ValueError):
        TradingDesired(api_id=7, active="yes")  # type: ignore[arg-type]


def test_a_view_is_the_supervisors_observation() -> None:
    view = AccountView(
        api_id=7,
        observed=ObservedWorker.EXITED,
        pid_gone=True,
        incarnation=2,
    )
    assert view.observed is ObservedWorker.EXITED
    assert view.pid_gone is True
    assert view.shim_waiting is False
    assert view.incarnation == 2
    absent = AccountView(
        api_id=7, observed="absent", pid_gone=True  # type: ignore[arg-type]
    )
    assert absent.observed is ObservedWorker.ABSENT
    assert absent.incarnation is None
    with pytest.raises(ValueError):
        AccountView(api_id=7, observed=ObservedWorker.RUNNING, pid_gone="no")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        AccountView(
            api_id=7,
            observed=ObservedWorker.EXITED,
            pid_gone=True,
            shim_waiting="yes",  # type: ignore[arg-type]
        )


def test_an_action_carries_only_what_its_kind_needs() -> None:
    spawn = OrchestratorAction(kind=ActionKind.SPAWN, api_id=7, incarnation=1)
    assert spawn.incarnation == 1
    assert spawn.active is None
    push = OrchestratorAction(kind=ActionKind.PUSH_TRADING, api_id=7, active=True)
    assert push.active is True
    stop = OrchestratorAction(kind=ActionKind.STOP, api_id=7)
    assert stop.incarnation is None and stop.active is None
    with pytest.raises(ValueError):
        OrchestratorAction(kind=ActionKind.SPAWN, api_id=7, incarnation=0)
    with pytest.raises(ValueError):
        OrchestratorAction(kind=ActionKind.PUSH_TRADING, api_id=7, incarnation=1)
    with pytest.raises(ValueError):
        OrchestratorAction(kind=ActionKind.DRAIN, api_id=7, active=False)


def test_the_shape_has_no_code_identity_pins() -> None:
    """``strategy_digest`` and ``env_generation`` are IF-16."""
    names: set[str] = set()
    for model in (BoundAccount, TradingDesired, AccountView, OrchestratorAction):
        names |= {item.name for item in fields(model)}
    assert "strategy_digest" not in names
    assert "env_generation" not in names
    assert "code_ref" not in names


def test_the_worker_spec_asks_procman_to_restart_on_failure() -> None:
    """An account is infrastructure (§4.3). Procman restarts it.
    The deploy-style ``never`` of an STS session is not this spec."""
    worker = _spec()
    assert worker.restart == "on_failure"
    assert worker.plane == "td"
    assert worker.kind == ACCOUNT_KIND
    assert worker.id == account_worker_id(7)
    assert worker.incarnation == FIRST_INCARNATION
    assert worker.code_ref == "v1"
    assert dict(worker.labels) == {}
    assert "strategy_digest" not in worker.labels
    assert "env_generation" not in worker.labels
    assert worker.oom_score_adj == OOM_SCORE_ADJ[("td", "account")]
    assert worker.hb_timeout_s == 3
    assert worker.start_timeout_s == 60
    assert worker.stop_grace_s == 8
    assert "restart" not in inspect.signature(account_worker_spec).parameters


def test_timeouts_and_the_release_are_the_callers() -> None:
    worker = _spec(
        incarnation=4,
        code_ref="release-9",
        start_timeout_s=15,
        hb_timeout_s=2.5,
        stop_grace_s=1,
        env={"VENUE": "paper"},
    )
    assert worker.incarnation == 4
    assert worker.code_ref == "release-9"
    assert worker.start_timeout_s == 15
    assert worker.hb_timeout_s == 2.5
    assert worker.stop_grace_s == 1
    assert dict(worker.env) == {"VENUE": "paper"}
    with pytest.raises(ValueError):
        _spec(hb_timeout_s=None)
    with pytest.raises(ValueError):
        _spec(code_ref="")
    with pytest.raises(ValueError):
        _spec(incarnation=0)


def test_intensity_is_required_and_not_chosen_here(tmp_path: Path) -> None:
    parameter = inspect.signature(TdOrchestrator.__init__).parameters["intensity"]
    assert parameter.default is inspect.Parameter.empty
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    with pytest.raises(TypeError):
        TdOrchestrator(supervisor, code_ref="v1")  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        TdOrchestrator(supervisor, intensity=object(), code_ref="v1")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        TdOrchestrator(supervisor, intensity=_intensity(), code_ref="")
    with pytest.raises(ValueError, match="td supervisor"):
        TdOrchestrator(
            Supervisor(tmp_path, plane="md", instance="md"),
            intensity=_intensity(),
            code_ref="v1",
        )
    orch = _orch(tmp_path)
    assert orch.intensity is not None
    assert orch.intensity.max_restarts == 2
    assert orch.code_ref == "v1"
    assert not hasattr(orch, "strategy_digest")
    assert not hasattr(orch, "env_generation")


def test_the_controller_does_not_choose_restart_numbers() -> None:
    """Issue #286. A call to ``RestartIntensity`` would be a number we picked."""
    for tree in _sources():
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else None
            if isinstance(func, ast.Attribute):
                name = func.attr
            assert name != "RestartIntensity"


def test_decisions_answer() -> None:
    """B4-07. ``td_reattach`` is the procman table and stays in the contract."""
    account = _account()
    put = _put(7)
    delete = TdIntentDelete(
        session_id="abc123",
        owner=IntentOwner(sts_instance="sts", session_id="abc123"),
        api_ids=[],
    )
    assert desired_accounts([account], instance="td") == (account,)
    assert apply_put((), put) == (put,)
    assert apply_delete((put,), delete) == ()
    assert trading_active(7, ()) is False
    assert trading_pushes(publish=False, accounts=(), intents=()) == ()
    assert spawn_allowed(previous=True, pid_gone=False) is False
    assert account_pid_gone(recorded_start_ticks=10, live_start_ticks=None) is True
    assert release_named(ReattachAction.NONE, shim_waiting=True) is True
    assert release_named(ReattachAction.ADOPT, shim_waiting=True) is False
    intensity = _intensity()
    assert plan_account_restart(
        phase=WorkerPhase.CRASHED,
        restarts_in_window=0,
        intensity=intensity,
        attempt=1,
    ) == plan_restart(
        phase=WorkerPhase.CRASHED,
        restart="on_failure",
        restarts_in_window=0,
        intensity=intensity,
        attempt=1,
    )
    assert close_actions(CloseMode.DETACH) == ()


def test_a_bad_argument_is_refused_before_the_stub() -> None:
    with pytest.raises(ValueError):
        desired_accounts([], instance="TD")
    with pytest.raises(TypeError):
        apply_put((), object())  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        trading_active(0, ())
    with pytest.raises(ValueError):
        trading_pushes(publish="yes", accounts=(), intents=())  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        td_reattach(desired="somewhere", observed=ObservedWorker.RUNNING)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        spawn_allowed(previous=1, pid_gone=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        account_pid_gone(recorded_start_ticks=-1, live_start_ticks=None)
    with pytest.raises(TypeError):
        release_named("none", shim_waiting=True)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        observation_view(
            7,
            object(),  # type: ignore[arg-type]
            recorded_start_ticks=None,
            live_start_ticks=None,
        )
    with pytest.raises(ValueError):
        plan_account_restart(
            phase=WorkerPhase.CRASHED,
            restarts_in_window=0,
            intensity=_intensity(),
            attempt=0,
        )
    with pytest.raises(TypeError):
        plan_account_restart(
            phase=WorkerPhase.CRASHED,
            restarts_in_window=0,
            intensity=object(),  # type: ignore[arg-type]
            attempt=1,
        )
    with pytest.raises(ValueError):
        close_actions("halt")  # type: ignore[arg-type]


def test_reconcile_of_another_instance_is_empty_and_drain_still_raises(
    tmp_path: Path,
) -> None:
    """Another instance is not spawned here. Drain-replace is still B6-04."""
    orch = _orch(tmp_path)
    view = AccountView(
        api_id=7,
        observed=ObservedWorker.RUNNING,
        pid_gone=False,
        incarnation=1,
    )
    assert orch.reconcile((_account(instance="td-jp"),), (), ()) == ()
    with pytest.raises(TypeError):
        orch.reconcile(object(), (), ())  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="^IF-12$"):
        orch.drain_replace(_account(), view)
    with pytest.raises(ValueError, match="bound to"):
        orch.drain_replace(_account(instance="td-jp"), view)
    other = AccountView(
        api_id=8, observed=ObservedWorker.RUNNING, pid_gone=False, incarnation=1
    )
    with pytest.raises(ValueError, match="does not match"):
        orch.drain_replace(_account(), other)
    assert orch.account_restart(
        phase=WorkerPhase.CRASHED, restarts_in_window=0, attempt=1
    ) == plan_restart(
        phase=WorkerPhase.CRASHED,
        restart="on_failure",
        restarts_in_window=0,
        intensity=orch.intensity,
        attempt=1,
    )
    with pytest.raises(ValueError):
        orch.account_restart(
            phase=WorkerPhase.CRASHED, restarts_in_window=-1, attempt=1
        )


def test_intent_types_and_the_subject() -> None:
    assert INTENT_TYPES == frozenset({TD_INTENT_PUT, TD_INTENT_DELETE})
    assert control_subject("td-jp") == "td.td-jp"
    with pytest.raises(ValueError):
        control_subject("TD")


async def test_the_intent_handler_refuses_an_empty_payload() -> None:
    message = Envelope[dict[str, object]].wrap({}, type=TD_INTENT_PUT, source="api")
    reply = await intent_handler(TdIntentBook())(message)
    assert reply is not None
    assert reply.type == TD_ERROR
    assert RpcError.model_validate(reply.payload).code == "invalid_payload"


def test_the_td_process_registers_the_intent_handler() -> None:
    """B4-07 wires put and delete, and the report subscription, into the process."""
    app = ast.parse((_ROOT / "app.py").read_text())
    router = ast.parse((_ROOT / "rpc" / "router.py").read_text())

    def _imported(tree: ast.AST) -> set[str]:
        found: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                found.add(node.module)
                found.update(alias.name for alias in node.names)
            elif isinstance(node, ast.Import):
                found.update(alias.name for alias in node.names)
            elif isinstance(node, ast.Name):
                found.add(node.id)
            elif isinstance(node, ast.Attribute):
                found.add(node.attr)
        return found

    assert "intent_handler" in _imported(router)
    assert "mftik_td.controller" in _imported(router)
    assert "watch_sts_reports" in _imported(app)
    assert "intent_book" in _imported(app)


def test_the_controller_does_not_import_the_worker_or_strategy_code() -> None:
    """P6: trading-layer semantics stay here as decisions. The account
    worker, the running process, and strategy code are not imported."""
    forbidden_prefixes = (
        "mftik.strategy",
        "mftik_td.account",
        "mftik_td.app",
        "mftik_td.session",
        "mftik_sts",
    )
    for tree in _sources():
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules.append(node.module)
            for module in modules:
                assert not module.startswith(forbidden_prefixes)
