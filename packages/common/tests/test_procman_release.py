"""B3-07: which release this process is, and the S-2 pin file.

``current_release``, the pin-file lines and the atomic write are unit
tests: no processes. One integration test spawns a real shim.
"""

from __future__ import annotations

import importlib.metadata
import os
import sys
import textwrap
from pathlib import Path

import pytest
from mftik.procman import (
    ALIVE_PHASES,
    PINNED_PHASES,
    CloseMode,
    ProcmanError,
    Supervisor,
    SupervisorRecord,
    WorkerPhase,
    WorkerSpec,
    current_release,
    load_supervisor_state,
    pinned_releases,
    pinned_releases_path,
    write_pinned_releases,
)
from mftik.procman.release import _RELEASE_ENV
from mftik.procman.supervisor import _merge_spawn_intent, _Slot, _write_slots

_ENV = "STRATEGON_RELEASE_VERSION"


def _spec(code_ref: str, *, worker_id: str = "td/account/42") -> WorkerSpec:
    return WorkerSpec(
        id=worker_id,
        plane="td",
        kind="account",
        incarnation=1,
        argv=("/bin/true",),
        env={},
        code_ref=code_ref,
        restart="on_failure",
        start_timeout_s=30,
        hb_timeout_s=None,
        oom_score_adj=100,
        rlimit_data_bytes=None,
        stop_grace_s=2,
        labels={},
    )


def _record(spec: WorkerSpec, phase: WorkerPhase) -> SupervisorRecord:
    return SupervisorRecord(
        spec=spec,
        phase=phase,
        worker_pid=None,
        worker_start_ticks=None,
        shim_pid=None,
        shim_start_ticks=None,
        since_s=0.0,
    )


def _lines(path: Path) -> list[str]:
    return path.read_text().splitlines()


def test_current_release_prefers_strategon_when_it_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(_ENV, "v0.9.5")
    assert current_release() == "v0.9.5"
    monkeypatch.setenv(_ENV, "  v0.9.6  ")
    assert current_release() == "v0.9.6"


def test_current_release_uses_the_distribution_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(_ENV, raising=False)
    assert current_release() == importlib.metadata.version("mftik")
    assert current_release() != ""


def test_current_release_treats_an_empty_variable_as_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed = importlib.metadata.version("mftik")
    for raw in ("", "   ", "\n"):
        monkeypatch.setenv(_ENV, raw)
        assert current_release() == installed


def test_current_release_refuses_a_blank_distribution_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(_ENV, raising=False)

    def blank(name: str) -> str:
        del name
        return "  "

    monkeypatch.setattr(importlib.metadata, "version", blank)
    with pytest.raises(ProcmanError):
        current_release()


def test_current_release_refuses_when_mftik_is_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(_ENV, raising=False)

    def missing(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", missing)
    with pytest.raises(ProcmanError):
        current_release()


def test_only_the_release_module_reads_the_environment() -> None:
    """``STRATEGON_RELEASE_VERSION`` has one reader. Planes call the function.

    The worker heartbeat reads ``MFTIK_STATUS_FD`` (S6). That is a
    different variable, and the only other ``os.environ`` in this package.
    """
    root = Path(current_release.__code__.co_filename).resolve().parent
    hits = [
        path.name
        for path in sorted(root.rglob("*.py"))
        if "os.environ" in path.read_text()
    ]
    assert hits == ["heartbeat.py", "release.py"]
    beat = (root / "heartbeat.py").read_text()
    assert _ENV not in beat
    assert "STATUS_FD_ENV" in beat
    assert _RELEASE_ENV == _ENV


def test_pin_path_is_none_until_strategon_names_the_release(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv(_ENV, raising=False)
    monkeypatch.setenv("WORK_DIR", str(tmp_path))
    assert pinned_releases_path() is None
    assert pinned_releases_path(tmp_path) is None
    monkeypatch.setenv(_ENV, "   ")
    assert pinned_releases_path(tmp_path) is None


def test_pin_path_uses_the_work_dir_only_when_the_variable_is_set(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_ENV, "v0.9.5")
    explicit = tmp_path / "given"
    assert pinned_releases_path(explicit) == (
        explicit / ".strategon" / "pinned-releases"
    )
    monkeypatch.setenv("WORK_DIR", str(tmp_path / "from-env"))
    assert pinned_releases_path() == (
        tmp_path / "from-env" / ".strategon" / "pinned-releases"
    )
    monkeypatch.delenv("WORK_DIR")
    (tmp_path / "cwd").mkdir()
    monkeypatch.chdir(tmp_path / "cwd")
    assert pinned_releases_path() == Path.cwd() / ".strategon" / "pinned-releases"


def test_pinned_phases_are_the_alive_set_plus_lost() -> None:
    assert PINNED_PHASES == frozenset(ALIVE_PHASES | {WorkerPhase.LOST})


def test_pin_lines_keep_the_own_release_and_drop_a_released_slot() -> None:
    """Own is always there. A slot that is no longer held is not a line."""
    held = [
        ("rel-b", WorkerPhase.RUNNING),
        ("rel-a", WorkerPhase.LOST),
        ("rel-b", WorkerPhase.STARTING),
        ("rel-stop", WorkerPhase.STOPPING),
        ("rel-dead", WorkerPhase.FAILED),
        ("rel-stopped", WorkerPhase.STOPPED),
        ("rel-crashed", WorkerPhase.CRASHED),
        ("rel-backoff", WorkerPhase.BACKOFF),
        ("rel-fatal", WorkerPhase.FATAL),
        ("  ", WorkerPhase.RUNNING),
    ]
    lines = pinned_releases("v0.9.5", held)
    assert lines == ("rel-a", "rel-b", "rel-stop", "v0.9.5")
    released = [(code, phase) for code, phase in held if code != "rel-a"]
    assert pinned_releases("v0.9.5", released) == ("rel-b", "rel-stop", "v0.9.5")
    assert pinned_releases("v0.9.5", ()) == ("v0.9.5",)


def test_pin_lines_refuse_an_empty_own_release() -> None:
    with pytest.raises(ProcmanError):
        pinned_releases("  ", [("rel-a", WorkerPhase.RUNNING)])


def test_atomic_write_sorts_and_leaves_no_temp(tmp_path: Path) -> None:
    path = tmp_path / ".strategon" / "pinned-releases"
    write_pinned_releases(path, ["v0.9.5", "rel-b", "rel-a", "rel-b"])
    text = path.read_text()
    assert text == "rel-a\nrel-b\nv0.9.5\n"
    assert list(path.parent.glob("pinned-releases.*.tmp")) == []


def test_atomic_write_refuses_an_empty_file(tmp_path: Path) -> None:
    path = tmp_path / "pinned-releases"
    with pytest.raises(ProcmanError):
        write_pinned_releases(path, ["", "  "])
    assert not path.exists()


def test_a_failed_replace_leaves_no_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "pinned-releases"

    def boom(src: str, dst: str) -> None:
        del src, dst
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError, match="disk full"):
        write_pinned_releases(path, ["v0.9.5"])
    assert not path.exists()
    assert list(tmp_path.glob("pinned-releases.*.tmp")) == []


def test_spawn_intent_pins_the_same_snapshot_as_the_json(tmp_path: Path) -> None:
    pin = tmp_path / ".strategon" / "pinned-releases"
    spec = _spec("rel-intent")
    _merge_spawn_intent(tmp_path, spec, 0.0, pin)
    assert set(_lines(pin)) == {current_release(), "rel-intent"}
    stored = load_supervisor_state(tmp_path)
    assert [row.spec.code_ref for row in stored] == ["rel-intent"]
    assert stored[0].phase is WorkerPhase.STARTING
    assert list(pin.parent.glob("*.tmp")) == []


def test_kept_spawn_rows_stay_in_the_pin(tmp_path: Path) -> None:
    """A row retained for an in-flight spawn is part of the snapshot."""
    pin = tmp_path / "pinned-releases"
    kept = _spec("rel-kept", worker_id="td/account/1")
    _merge_spawn_intent(tmp_path, kept, 0.0, None)
    other = _record(_spec("rel-live", worker_id="td/account/2"), WorkerPhase.RUNNING)
    dead = _record(_spec("rel-dead", worker_id="td/account/3"), WorkerPhase.FAILED)
    _write_slots(tmp_path, (other, dead), (kept.id,), pin)
    assert set(_lines(pin)) == {current_release(), "rel-kept", "rel-live"}
    assert "rel-dead" not in _lines(pin)
    on_disk = {row.spec.id: row for row in load_supervisor_state(tmp_path)}
    assert set(on_disk) == {kept.id, other.spec.id, dead.spec.id}


def _slot(spec: WorkerSpec, phase: WorkerPhase, root: Path) -> _Slot:
    return _Slot(
        spec=spec,
        phase=phase,
        ready=phase is WorkerPhase.RUNNING,
        pid=10,
        exit_code=None,
        signal=None,
        since_s=0.0,
        beats=0,
        beats_at_s=None,
        term_sent=False,
        kill_sent=False,
        released=False,
        shim_gone=phase is WorkerPhase.LOST,
        shim_pid=0,
        socket=root / "missing.sock",
    )


async def test_persist_writes_the_pin_from_held_slots(tmp_path: Path) -> None:
    pin = tmp_path / ".strategon" / "pinned-releases"
    supervisor = Supervisor(tmp_path, plane="td", instance="td", pin_path=pin)
    live = _spec("rel-live", worker_id="td/account/1")
    lost = _spec("rel-lost", worker_id="td/account/2")
    dead = _spec("rel-dead", worker_id="td/account/3")
    supervisor._slots[live.id] = _slot(live, WorkerPhase.RUNNING, tmp_path)
    supervisor._slots[lost.id] = _slot(lost, WorkerPhase.LOST, tmp_path)
    supervisor._slots[dead.id] = _slot(dead, WorkerPhase.FAILED, tmp_path)
    await supervisor._persist()
    assert set(_lines(pin)) == {current_release(), "rel-live", "rel-lost"}
    supervisor._slots.pop(live.id)
    supervisor._slots.pop(lost.id)
    await supervisor._persist()
    assert _lines(pin) == [current_release()]


async def test_persist_without_a_pin_path_writes_no_file(tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path, plane="td", instance="td")
    spec = _spec("rel-live")
    supervisor._slots[spec.id] = _slot(spec, WorkerPhase.RUNNING, tmp_path)
    await supervisor._persist()
    assert not (tmp_path / ".strategon").exists()
    assert load_supervisor_state(tmp_path)[0].spec.code_ref == "rel-live"


_SLEEP = """
import time
time.sleep(30)
"""


def _argv(source: str) -> tuple[str, ...]:
    return (sys.executable, "-c", textwrap.dedent(source).strip())


@pytest.mark.integration
async def test_a_live_workers_release_is_pinned_until_the_worker_is_released(
    tmp_path: Path,
) -> None:
    """Spawn a shim on a release that is not this controller's, then drop it."""
    pin = tmp_path / ".strategon" / "pinned-releases"
    own = current_release()
    spec = WorkerSpec(
        id="td/account/42",
        plane="td",
        kind="account",
        incarnation=1,
        argv=_argv(_SLEEP),
        env={},
        code_ref="b3-07-worker-release",
        restart="never",
        start_timeout_s=30,
        hb_timeout_s=None,
        oom_score_adj=100,
        rlimit_data_bytes=None,
        stop_grace_s=2,
        labels={},
    )
    assert spec.code_ref != own
    supervisor = Supervisor(tmp_path, plane="td", instance="td", pin_path=pin)
    try:
        await supervisor.spawn(spec)
        assert set(_lines(pin)) == {own, spec.code_ref}
        assert _lines(pin) == sorted(set(_lines(pin)))
        stored = load_supervisor_state(tmp_path)
        assert [row.spec.code_ref for row in stored] == [spec.code_ref]
        await supervisor.close(CloseMode.STOP)
        assert _lines(pin) == [own]
        assert load_supervisor_state(tmp_path) == ()
        assert list(pin.parent.glob("*.tmp")) == []
    finally:
        if not supervisor._closed:
            await supervisor.close(CloseMode.STOP)
