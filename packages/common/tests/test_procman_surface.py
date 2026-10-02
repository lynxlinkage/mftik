"""The procman interface IF-03 defines, and what B3 still has to decide.

The shape is real: ``WorkerSpec`` is §4.3, the transition table is the
diagram, and the NDJSON frames round-trip. The shim is real as of
B3-01. Classifying a death and planning a restart are real as of B3-02.
The liveness report is real as of B3-04; its wire shape is
:class:`mftik.protocol.v2.ProcmanReport`, not a second type in this
package. Reattach still raises ``NotImplementedError("IF-03")``.

What B3-03 has to make true is in ``test_procman_contract.py``, as xfail.
"""

from __future__ import annotations

import ast
import signal
from dataclasses import fields
from pathlib import Path

import pytest
from mftik import procman
from mftik.procman import (
    CONTROLLER_OOM_SCORE_ADJ,
    OOM_SCORE_ADJ,
    PIPE_BUF,
    SHIM_OOM_SCORE_ADJ,
    STATUS_FD_ENV,
    TICKET,
    TRANSITIONS,
    CloseMode,
    ExitRecord,
    InvalidTransition,
    InvalidWorkerId,
    InvalidWorkerSpec,
    MessageError,
    ReleaseCommand,
    ShimStatus,
    SignalCommand,
    StatusQuery,
    Supervisor,
    Trigger,
    WatchCommand,
    WorkerHeartbeat,
    WorkerPhase,
    WorkerSpec,
    decode_command,
    decode_exit,
    decode_heartbeat,
    decode_status,
    dump_frame,
    encode_command,
    encode_exit,
    encode_heartbeat,
    encode_status,
    exit_record_path,
    exit_record_tmp_path,
    load_frame,
    reattach_action,
    socket_path,
    supervisor_state_path,
    transition,
)
from mftik.procman.decisions import DesiredSlot, ObservedWorker

_CODE_REF = "the release version of the controller that spawned the worker (§4.5)"

_SPEC_FIELDS = {
    "id",
    "plane",
    "kind",
    "incarnation",
    "argv",
    "env",
    "code_ref",
    "restart",
    "start_timeout_s",
    "hb_timeout_s",
    "oom_score_adj",
    "rlimit_data_bytes",
    "stop_grace_s",
    "labels",
}

_BANNED = ("strategy_digest", "env_generation", "strategy registry")


def _spec(**overrides: object) -> WorkerSpec:
    raw: dict[str, object] = {
        "id": "td/account/42",
        "plane": "td",
        "kind": "account",
        "incarnation": 1,
        "argv": ("/bin/true",),
        "env": {},
        "code_ref": "v1",
        "restart": "on_failure",
        "start_timeout_s": 60,
        "hb_timeout_s": 3,
        "oom_score_adj": 100,
        "rlimit_data_bytes": None,
        "stop_grace_s": 8,
        "labels": {},
    }
    raw.update(overrides)
    return WorkerSpec(**raw)  # type: ignore[arg-type]


def test_the_package_imports() -> None:
    assert procman.TICKET == "IF-03"
    assert procman.Supervisor is Supervisor


def test_worker_spec_fields_are_section_4_3() -> None:
    assert {item.name for item in fields(WorkerSpec)} == _SPEC_FIELDS


def test_code_ref_is_described_as_the_spawning_release() -> None:
    def flat(text: str) -> str:
        return " ".join(text.split())

    assert _CODE_REF in flat(WorkerSpec.__doc__ or "")
    assert _CODE_REF in flat(procman.__doc__ or "")


def test_the_package_does_not_name_strategy_versions() -> None:
    """IF-16 owns digest and generation. This layer's wording stays off them."""
    root = Path(procman.__file__).resolve().parent
    for path in sorted(root.rglob("*.py")):
        text = path.read_text()
        for word in _BANNED:
            assert word not in text, f"{path.name} mentions {word}"


def test_a_spec_keeps_a_callers_mapping_at_arm_length() -> None:
    env = {"A": "1"}
    labels = {"k": "v"}
    spec = _spec(env=env, labels=labels, hb_timeout_s=None)
    env["A"] = "2"
    labels["k"] = "rewritten"
    assert spec.env["A"] == "1"
    assert spec.labels["k"] == "v"
    assert spec.hb_timeout_s is None
    assert spec.argv == ("/bin/true",)
    with pytest.raises(TypeError):
        spec.labels["k"] = "no"  # type: ignore[index]


def test_equal_specs_compare_equal() -> None:
    assert _spec() == _spec()


@pytest.mark.parametrize(
    "worker_id",
    ["sts/session/a1b2c3", "md/conn/Deribit/public/0", "td/account/42"],
)
def test_the_plans_worker_ids_are_safe_paths(worker_id: str) -> None:
    assert _spec(id=worker_id).id == worker_id


@pytest.mark.parametrize(
    "worker_id",
    ["", "..", "/abs", "a/../b", "a/./b", "a//b", ".hidden", "foo bar", "a\\b"],
)
def test_a_worker_id_that_escapes_run_is_refused(worker_id: str) -> None:
    with pytest.raises(InvalidWorkerId):
        _spec(id=worker_id)


@pytest.mark.parametrize(
    "overrides",
    [
        {"plane": "sym"},
        {"restart": "always"},
        {"argv": ()},
        {"incarnation": -1},
        {"start_timeout_s": 0},
        {"hb_timeout_s": 0},
        {"oom_score_adj": 1001},
        {"rlimit_data_bytes": 0},
        {"stop_grace_s": -1},
        {"kind": ""},
        {"labels": {"": "x"}},
        {"env": {"A": 1}},
    ],
)
def test_a_spec_refuses_a_field_section_4_3_cannot_describe(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(InvalidWorkerSpec):
        _spec(**overrides)


def test_oom_score_adj_initial_values_match_section_4_7() -> None:
    assert dict(OOM_SCORE_ADJ) == {
        ("sts", "session"): 800,
        ("md", "conn"): 300,
        ("md", "fetch"): 300,
        ("td", "account"): 100,
    }
    assert SHIM_OOM_SCORE_ADJ == 0
    assert CONTROLLER_OOM_SCORE_ADJ == 0
    assert ("sts", "offload") not in OOM_SCORE_ADJ


def test_transition_table_is_the_section_4_3_diagram() -> None:
    """Death before ready and death after ready are different edges.

    ``SIGTERM`` from ``STARTING`` is the same stop as from ``RUNNING``:
    ``stop`` can arrive before the worker is ready. The diagram draws the
    arrow once.
    """
    expected = {
        (WorkerPhase.STOPPED, Trigger.SPAWN): WorkerPhase.STARTING,
        (WorkerPhase.STARTING, Trigger.READY): WorkerPhase.RUNNING,
        (WorkerPhase.STARTING, Trigger.DEATH): WorkerPhase.FAILED,
        (WorkerPhase.STARTING, Trigger.START_TIMEOUT): WorkerPhase.FAILED,
        (WorkerPhase.STARTING, Trigger.SIGTERM): WorkerPhase.STOPPING,
        (WorkerPhase.STARTING, Trigger.SHIM_LOST): WorkerPhase.LOST,
        (WorkerPhase.RUNNING, Trigger.SIGTERM): WorkerPhase.STOPPING,
        (WorkerPhase.RUNNING, Trigger.DEATH): WorkerPhase.CRASHED,
        (WorkerPhase.RUNNING, Trigger.HEARTBEAT_TIMEOUT): WorkerPhase.CRASHED,
        (WorkerPhase.RUNNING, Trigger.SHIM_LOST): WorkerPhase.LOST,
        (WorkerPhase.STOPPING, Trigger.EXITED): WorkerPhase.STOPPED,
        (WorkerPhase.STOPPING, Trigger.SHIM_LOST): WorkerPhase.LOST,
        (WorkerPhase.CRASHED, Trigger.RESTART): WorkerPhase.BACKOFF,
        (WorkerPhase.CRASHED, Trigger.INTENSITY_EXCEEDED): WorkerPhase.FATAL,
        (WorkerPhase.BACKOFF, Trigger.BACKOFF_ELAPSED): WorkerPhase.STARTING,
    }
    assert dict(TRANSITIONS) == expected
    assert transition(WorkerPhase.STARTING, Trigger.DEATH) is WorkerPhase.FAILED
    assert transition(WorkerPhase.RUNNING, Trigger.DEATH) is WorkerPhase.CRASHED


def test_failed_fatal_and_lost_have_no_outgoing_edge() -> None:
    """``STOPPED`` still has ``SPAWN``. The phases nothing leaves are the
    ones the diagram does not draw an arrow out of."""
    sources = {phase for phase, _trigger in TRANSITIONS}
    assert {
        WorkerPhase.FAILED,
        WorkerPhase.FATAL,
        WorkerPhase.LOST,
    }.isdisjoint(sources)


def test_an_edge_the_diagram_does_not_draw_is_refused() -> None:
    with pytest.raises(InvalidTransition):
        transition(WorkerPhase.FAILED, Trigger.RESTART)


def test_paths_live_under_run() -> None:
    root = Path("/work")
    assert socket_path(root, "sts/session/a1b2c3") == Path(
        "/work/run/sts/session/a1b2c3.sock"
    )
    assert exit_record_path(root, "td/account/42") == Path(
        "/work/run/td/account/42.exit.json"
    )
    assert exit_record_tmp_path(root, "td/account/42") == Path(
        "/work/run/td/account/42.exit.json.tmp"
    )
    assert supervisor_state_path(root) == Path("/work/run/supervisor.json")
    with pytest.raises(InvalidWorkerId):
        socket_path(root, "../outside")


def test_shim_commands_round_trip() -> None:
    commands = (
        StatusQuery(),
        SignalCommand(signal.SIGTERM),
        WatchCommand(),
        ReleaseCommand(),
    )
    for command in commands:
        assert decode_command(encode_command(command)) == command


def test_status_and_exit_records_round_trip() -> None:
    alive = ShimStatus(
        id="md/conn/Deribit/public/0",
        incarnation=2,
        pid=100,
        ready=True,
        rss_bytes=4096,
        beats=3,
    )
    assert decode_status(encode_status(alive)) == alive
    dead = ShimStatus(
        id="td/account/42",
        incarnation=1,
        pid=100,
        ready=False,
        exit_code=None,
        signal=signal.SIGKILL,
        beats=1,
    )
    assert decode_status(encode_status(dead)) == dead
    record = ExitRecord(
        id="td/account/42",
        incarnation=1,
        pid=100,
        exit_code=3,
        signal=None,
        ready=False,
    )
    assert decode_exit(encode_exit(record)) == record
    missing_beats = dump_frame(
        {
            "op": "status",
            "id": "td/account/42",
            "incarnation": 1,
            "pid": 100,
            "ready": False,
            "exit_code": None,
            "signal": None,
            "rss_bytes": None,
        }
    )
    with pytest.raises(MessageError):
        decode_status(missing_beats)
    with pytest.raises(MessageError):
        ShimStatus(
            id="td/account/42",
            incarnation=1,
            pid=100,
            ready=False,
            beats=-1,
        )


def test_an_exit_record_has_exactly_one_of_code_and_signal() -> None:
    with pytest.raises(MessageError):
        ExitRecord(
            id="td/account/42",
            incarnation=1,
            pid=100,
            exit_code=1,
            signal=9,
            ready=False,
        )
    with pytest.raises(MessageError):
        ExitRecord(
            id="td/account/42",
            incarnation=1,
            pid=100,
            exit_code=None,
            signal=None,
            ready=False,
        )


def test_a_heartbeat_round_trips_inside_pipe_buf() -> None:
    assert PIPE_BUF == 4096
    assert STATUS_FD_ENV == "MFTIK_STATUS_FD"
    beat = WorkerHeartbeat(ready=True, extra={"progress": "on_start"})
    frame = encode_heartbeat(beat)
    assert frame.endswith(b"\n")
    assert len(frame) <= PIPE_BUF
    assert decode_heartbeat(frame) == beat


def test_a_heartbeat_over_pipe_buf_is_refused() -> None:
    beat = WorkerHeartbeat(ready=True, extra={"pad": "x" * 5000})
    with pytest.raises(MessageError):
        encode_heartbeat(beat)


def test_an_unknown_shim_op_is_refused() -> None:
    with pytest.raises(MessageError):
        decode_command(dump_frame({"op": "restart"}))
    with pytest.raises(MessageError):
        load_frame(b'{"op":"status"}')


def test_the_duplicate_report_types_are_gone() -> None:
    """The wire type is ``mftik.protocol.v2.ProcmanReport``. One subject helper."""
    for name in (
        "ProcmanReport",
        "ReportedWorker",
        "encode_report",
        "decode_report",
        "report_subject",
    ):
        assert not hasattr(procman, name), name
    assert procman.REPORT_PERIOD_S == 5.0


async def test_supervisor_methods_raise_the_ticket(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    # spawn, stop, status and report are real (B3-02, B3-04). start and
    # close stay with B3-03. close pauses publication, then still raises.
    calls = (
        supervisor.start(),
        supervisor.close(CloseMode.DETACH),
        supervisor.close("stop"),
    )
    for call in calls:
        with pytest.raises(NotImplementedError, match=rf"^{TICKET}$"):
            await call


async def test_close_rejects_an_unknown_mode(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    with pytest.raises(ValueError):
        await supervisor.close("reboot")  # type: ignore[arg-type]


async def test_spawn_refuses_a_spec_from_another_plane(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    with pytest.raises(ValueError):
        await supervisor.spawn(
            _spec(id="md/conn/Deribit/public/0", plane="md", kind="conn")
        )


def test_a_bad_instance_name_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        Supervisor(tmp_path, plane="td", instance="TD-JP-1")


async def test_stop_refuses_an_id_that_escapes_run(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    with pytest.raises(InvalidWorkerId):
        await supervisor.stop("../outside")


def test_decisions_raise_the_ticket() -> None:
    # classify_failure, plan_restart and count_restarts_in_window are real
    # (B3-02). reattach_action stays until B3-03.
    calls = (
        lambda: reattach_action(
            plane="sts",
            desired=DesiredSlot.PRESENT,
            observed=ObservedWorker.RUNNING,
        ),
    )
    for call in calls:
        with pytest.raises(NotImplementedError, match=rf"^{TICKET}$"):
            call()


def test_the_package_imports_neither_pydantic_nor_nats() -> None:
    """F29: the shim stays on the standard library. So does this package."""
    root = Path(procman.__file__).resolve().parent
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            for module in modules:
                assert module != "pydantic" and not module.startswith("pydantic.")
                assert module != "nats" and not module.startswith("nats.")


def test_the_package_does_not_spawn_with_asyncio() -> None:
    """§4.1: asyncio's subprocess transport kills the child on close.

    The docstring may name the call in order to forbid it. The tree may not.
    """
    banned = {"create_subprocess_exec", "create_subprocess_shell"}
    root = Path(procman.__file__).resolve().parent
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            name = None
            if isinstance(node, ast.Attribute):
                name = node.attr
            elif isinstance(node, ast.Name):
                name = node.id
            assert name not in banned, f"{path.name} references {name}"
