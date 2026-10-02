"""The STS controller interface IF-04 defines.

The shape is real: F11's defaults, a session spec, and the worker spec
procman is allowed to see (``restart`` is ``never``). Classifying a crash
and choosing a rehang still raise ``NotImplementedError("IF-04")``.
Reconcile creates or stops a worker, and start / end / list answer.
A crash-shaped reconcile still raises, so B5-06 stays the owner of that
path.

What B4-02 and B5-06 have to make true is in ``test_controller_contract.py``.
B5 stays xfail.
"""

from __future__ import annotations

import ast
from dataclasses import fields
from pathlib import Path

import pytest
from mftik.procman import OOM_SCORE_ADJ, Supervisor
from mftik.protocol import (
    STS_ERROR,
    STS_SESSION_END,
    STS_SESSION_FAIL,
    STS_SESSION_FORCE_STOP,
    STS_SESSION_LIST,
    STS_SESSION_START,
    Envelope,
    RpcError,
)
from mftik.protocol.strategy_yml import (
    DEFAULT_MAX_RESTARTS,
    DEFAULT_READY_TIMEOUT_S,
    DEFAULT_RESTART_WINDOW_S,
    DEFAULT_START_TIMEOUT_S,
    MAX_START_TIMEOUT_S,
)
from mftik_sts.controller import (
    FIRST_INCARNATION,
    LABEL_ENV_GENERATION,
    LABEL_STRATEGY_DIGEST,
    SESSION_KIND,
    STS_MAX_RESTARTS,
    STS_MIN_BACKOFF_S,
    STS_RESTART_WINDOW_S,
    TICKET,
    ActionKind,
    Cleanup,
    CrashCause,
    CrashClass,
    OrchestratorAction,
    SessionPhase,
    SessionSpec,
    SessionStatus,
    StsOrchestrator,
    backoff_s,
    classify_crash,
    control_subject,
    decide_restart,
    end_handler,
    reported_session_ids,
    retains_intents,
    session_worker_id,
    session_worker_spec,
    spawn_allowed,
    start_handler,
    sts_restart_intensity,
)
from mftik_sts.rpc.router import _HANDLERS, CONTROLLER_TYPES
from mftik_sts.rpc.sessions import (
    handle_session_fail,
    handle_session_force_stop,
)

_ROOT = Path(__file__).resolve().parents[1] / "src" / "mftik_sts" / "controller"

#: Parsed once, at import. The call phase only walks the trees. Parsing
#: the controller sources inside the test is enough to miss the 50 ms
#: unit cap when the worker is busy.
_PARSED = [ast.parse(path.read_text()) for path in sorted(_ROOT.glob("*.py"))]


def _sources() -> list[ast.AST]:
    return _PARSED


def _orch(tmp_path: Path) -> StsOrchestrator:
    return StsOrchestrator(Supervisor(tmp_path, plane="sts", instance="sts"))


def _spec(**overrides: object) -> SessionSpec:
    raw: dict[str, object] = {
        "session_id": "abc123",
        "instance": "sts",
        "strategy": "noop",
    }
    raw.update(overrides)
    return SessionSpec(**raw)  # type: ignore[arg-type]


def _worker(spec: SessionSpec):
    return session_worker_spec(
        spec,
        incarnation=1,
        argv=("python",),
        code_ref="v1",
        start_timeout_s=60,
        hb_timeout_s=None,
        stop_grace_s=10,
    )


def test_ticket_id_is_the_stub_message() -> None:
    assert TICKET == "IF-04"


def test_f11_defaults_live_here() -> None:
    """5 / 600 / 1s. The yml schema uses the same cap and window (IF-07).
    The backoff floor is not a document field."""
    assert STS_MAX_RESTARTS == 5
    assert STS_RESTART_WINDOW_S == 600
    assert STS_MIN_BACKOFF_S == 1.0
    assert STS_MAX_RESTARTS == DEFAULT_MAX_RESTARTS
    assert STS_RESTART_WINDOW_S == DEFAULT_RESTART_WINDOW_S
    assert FIRST_INCARNATION == 1
    intensity = sts_restart_intensity()
    assert intensity.max_restarts == STS_MAX_RESTARTS
    assert intensity.window_s == STS_RESTART_WINDOW_S
    assert intensity.min_backoff_s == STS_MIN_BACKOFF_S


def test_a_spec_defaults_to_never_and_rejects_rebuild() -> None:
    spec = _spec()
    assert spec.restart == "never"
    assert spec.max_restarts == STS_MAX_RESTARTS
    assert spec.restart_window_s == STS_RESTART_WINDOW_S
    assert spec.start_timeout_s == DEFAULT_START_TIMEOUT_S
    assert spec.ready_timeout_s == DEFAULT_READY_TIMEOUT_S
    assert spec.generation == 1
    _spec(start_timeout_s=MAX_START_TIMEOUT_S)
    with pytest.raises(ValueError, match="always"):
        _spec(restart="always")
    with pytest.raises(ValueError):
        _spec(start_timeout_s=MAX_START_TIMEOUT_S + 1)


def test_the_spec_pins_code_identity_and_status_does_not() -> None:
    """F39 puts the pins on the spec. Status and actions do not carry them."""
    assert "strategy_digest" in {item.name for item in fields(SessionSpec)}
    assert "env_generation" in {item.name for item in fields(SessionSpec)}
    bare = _spec()
    assert bare.strategy_digest is None
    assert bare.env_generation is None
    digest = "sha256:" + "ab" * 32
    pinned = _spec(strategy_digest=digest, env_generation=4)
    assert pinned.strategy_digest == digest
    assert len(digest) == 71
    assert pinned.env_generation == 4
    _spec(env_generation=0)
    with pytest.raises(ValueError, match="sha256"):
        _spec(strategy_digest="sha256:abcd")
    with pytest.raises(ValueError):
        _spec(env_generation=True)
    with pytest.raises(ValueError):
        _spec(env_generation=-1)
    for model in (SessionStatus, OrchestratorAction):
        names = {item.name for item in fields(model)}
        assert "strategy_digest" not in names
        assert "env_generation" not in names
        assert "st_facts" not in names


def test_session_worker_spec_never_asks_procman_to_restart() -> None:
    """The deploy may say ``on_failure``. The worker spec still says
    ``never``, so procman cannot spawn the next incarnation on its own."""
    spec = _spec(restart="on_failure")
    worker = _worker(spec)
    assert spec.restart == "on_failure"
    assert worker.restart == "never"
    assert worker.plane == "sts"
    assert worker.kind == SESSION_KIND
    assert worker.id == session_worker_id("abc123")
    assert worker.id == "sts/session/abc123"
    assert worker.incarnation == 1
    assert worker.code_ref == "v1"
    assert dict(worker.labels) == {}
    assert LABEL_STRATEGY_DIGEST not in worker.labels
    assert LABEL_ENV_GENERATION not in worker.labels
    assert worker.oom_score_adj == OOM_SCORE_ADJ[("sts", "session")]
    assert worker.hb_timeout_s is None


def test_session_worker_spec_labels_carry_the_pins() -> None:
    """Procman does not grow fields. The pins ride in ``labels`` as strings."""
    digest = "sha256:" + "cd" * 32
    spec = _spec(strategy_digest=digest, env_generation=7)
    worker = _worker(spec)
    assert worker.labels[LABEL_STRATEGY_DIGEST] == digest
    assert worker.labels[LABEL_ENV_GENERATION] == "7"
    digest_only = _worker(_spec(strategy_digest=digest))
    assert digest_only.labels[LABEL_STRATEGY_DIGEST] == digest
    assert LABEL_ENV_GENERATION not in digest_only.labels
    env_only = _worker(_spec(env_generation=0))
    assert env_only.labels[LABEL_ENV_GENERATION] == "0"
    assert LABEL_STRATEGY_DIGEST not in env_only.labels


def test_decisions_raise_if_04() -> None:
    with pytest.raises(NotImplementedError, match="^IF-04$"):
        classify_crash(CrashCause.STRATEGY_EXCEPTION)
    with pytest.raises(ValueError):
        classify_crash("not-a-cause")  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="^IF-04$"):
        decide_restart(
            restart="on_failure",
            crash_class=CrashClass.A,
            ready=True,
            cleanup=Cleanup.CONFIRMED,
            exit_recorded=True,
            pid_gone=True,
            restarts_in_window=0,
            incarnation=1,
            attempt=1,
        )
    with pytest.raises(ValueError, match="always"):
        decide_restart(
            restart="always",
            crash_class=CrashClass.A,
            ready=True,
            cleanup=Cleanup.CONFIRMED,
            exit_recorded=True,
            pid_gone=True,
            restarts_in_window=0,
            incarnation=1,
            attempt=1,
        )
    with pytest.raises(ValueError, match="no crash class"):
        decide_restart(
            restart="on_failure",
            crash_class=None,
            ready=True,
            cleanup=Cleanup.CONFIRMED,
            exit_recorded=True,
            pid_gone=True,
            restarts_in_window=0,
            incarnation=1,
            attempt=1,
        )
    with pytest.raises(ValueError):
        backoff_s(0)
    with pytest.raises(NotImplementedError, match="^IF-04$"):
        backoff_s(1)
    with pytest.raises(NotImplementedError, match="^IF-04$"):
        spawn_allowed(
            exit_recorded=True, pid_gone=True, cleanup=Cleanup.CONFIRMED
        )
    with pytest.raises(NotImplementedError, match="^IF-04$"):
        retains_intents(SessionPhase.RESTARTING)
    with pytest.raises(NotImplementedError, match="^IF-04$"):
        reported_session_ids(())


def test_reconcile_checks_its_inputs_and_leaves_a_crash_to_b5(
    tmp_path: Path,
) -> None:
    """An empty status spawns. A crash-shaped one still raises (B5-06)."""
    orch = _orch(tmp_path)
    actions = orch.reconcile(_spec(), SessionStatus())
    assert any(action.kind is ActionKind.SPAWN for action in actions)
    with pytest.raises(ValueError, match="does not match"):
        orch.reconcile(_spec(instance="other"), SessionStatus())
    with pytest.raises(TypeError):
        orch.reconcile(object(), SessionStatus())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="sts supervisor"):
        StsOrchestrator(Supervisor(tmp_path, plane="md", instance="md"))
    crashed = SessionStatus(
        phase=SessionPhase.RUNNING,
        worker_incarnation=1,
        exit_recorded=True,
        pid_gone=True,
        crash_class=CrashClass.A,
    )
    with pytest.raises(NotImplementedError, match="^IF-04$"):
        orch.reconcile(_spec(restart="on_failure"), crashed)


async def test_handlers_reject_a_bad_payload_instead_of_raising(
    tmp_path: Path,
) -> None:
    orch = _orch(tmp_path)
    message = Envelope[dict].wrap({}, type=STS_SESSION_START, source="api")
    for handler in (start_handler(orch), end_handler(orch)):
        reply = await handler(message)
        assert reply is not None
        assert reply.type == STS_ERROR
        assert RpcError.model_validate(reply.payload).code == "invalid_request"


def test_start_and_list_are_named_on_the_instance_subject() -> None:
    assert control_subject("sts-jp") == "sts.sts-jp"


def test_an_action_carries_no_strategy_state() -> None:
    names = {item.name for item in fields(OrchestratorAction)}
    assert "st_facts" not in names
    assert ActionKind.SPAWN.value == "spawn"


def test_start_end_and_list_are_not_the_placeholders() -> None:
    """B4-02 serves these from the controller. Fail and force-stop stay."""
    assert STS_SESSION_START in CONTROLLER_TYPES
    assert STS_SESSION_END in CONTROLLER_TYPES
    assert STS_SESSION_LIST in CONTROLLER_TYPES
    assert STS_SESSION_START not in _HANDLERS
    assert STS_SESSION_END not in _HANDLERS
    assert STS_SESSION_LIST not in _HANDLERS
    assert _HANDLERS[STS_SESSION_FAIL] is handle_session_fail
    assert _HANDLERS[STS_SESSION_FORCE_STOP] is handle_session_force_stop


def test_the_controller_does_not_import_strategy_code_or_the_process() -> None:
    forbidden = {
        "mftik_sts.impl",
        "mftik_sts.session",
        "mftik_sts.session_worker",
        "mftik_sts.app",
        "mftik_sts.rpc",
        "mftik_sts.rpc.router",
        "mftik_sts.rpc.sessions",
    }
    for tree in _sources():
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules.append(node.module)
            for module in modules:
                assert module not in forbidden
                assert not module.startswith("mftik_sts.impl")
                assert not module.startswith("mftik_sts.session")


def test_the_controller_does_not_ask_procman_to_classify_or_plan() -> None:
    """Crash class and the F11 choice are this layer's. Procman's
    ``classify_failure`` and ``plan_restart`` do not see them (P6)."""
    banned = {"classify_failure", "plan_restart"}
    for tree in _sources():
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                imported = {alias.name for alias in node.names}
                assert banned.isdisjoint(imported)
            if isinstance(node, ast.Call):
                func = node.func
                name = func.id if isinstance(node.func, ast.Name) else None
                if isinstance(func, ast.Attribute):
                    name = func.attr
                assert name not in banned
