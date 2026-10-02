"""B3-01 behaviour the S1–S7 contract does not already pin down.

Real subprocesses are ``integration`` (§9.1). The path helper is a unit
test: it does not start a process.
"""

from __future__ import annotations

import os
import sys
import textwrap
import time
from pathlib import Path

import pytest
from mftik.procman import ShimClient, WorkerSpec, log_path, spawn_shim
from mftik.procman.shim import LOG_MAX_BYTES

_SLEEP = """
import time
time.sleep(30)
"""

_PID = """
import os, sys, time
with open(sys.argv[1], "w") as handle:
    handle.write(str(os.getpid()))
time.sleep(30)
"""

_STDOUT = """
import sys, time
sys.stdout.write("x" * (256 * 1024 + 4096))
sys.stdout.flush()
with open(sys.argv[1], "w") as handle:
    handle.write("wrote")
time.sleep(30)
"""

# Three valid beats (the second does not change ready), one undecodable
# line, and one line over PIPE_BUF. The counter must stay at 3.
_BEATS = """
import os, sys, time
fd = int(os.environ["MFTIK_STATUS_FD"])
marker = sys.argv[1]
os.write(fd, b'{"ready":false}\\n')
os.write(fd, b'not-json\\n')
os.write(fd, b'{"ready":false}\\n')
os.write(fd, b'{"ready":true}\\n')
pad = b"x" * 5000
os.write(fd, b'{"ready":true,"pad":"' + pad + b'"}\\n')
with open(marker, "w") as handle:
    handle.write("sent")
time.sleep(30)
"""


def _argv(source: str, *args: str) -> tuple[str, ...]:
    return (sys.executable, "-c", textwrap.dedent(source).strip(), *args)


def _spec(argv: tuple[str, ...], **overrides: object) -> WorkerSpec:
    raw: dict[str, object] = {
        "id": "td/account/42",
        "plane": "td",
        "kind": "account",
        "incarnation": 1,
        "argv": argv,
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


def _wait_for(predicate, timeout_s: float = 3.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"condition was still false after {timeout_s}s")


def _kill(pid: int) -> None:
    if pid <= 1:
        return
    try:
        os.kill(pid, 9)
    except OSError:
        pass


def _stop(spawned) -> None:
    """Kill the worker, then the shim. Do not walk up to init."""
    client = ShimClient(spawned.socket)
    try:
        client.signal(9)
    except OSError:
        pass
    children = Path(f"/proc/{spawned.pid}/task/{spawned.pid}/children")
    try:
        text = children.read_text()
    except OSError:
        text = ""
    for part in text.split():
        _kill(int(part))
    try:
        client.release()
    except OSError:
        pass
    _kill(spawned.pid)


def test_log_path_names_the_three_streams(tmp_path: Path) -> None:
    assert log_path(tmp_path, "td/account/42", "stdout") == (
        tmp_path / "run" / "td" / "account" / "42.stdout.log"
    )
    assert log_path(tmp_path, "td/account/42", "stderr").name == "42.stderr.log"
    assert log_path(tmp_path, "td/account/42", "status").name == "42.status.log"
    with pytest.raises(ValueError):
        log_path(tmp_path, "td/account/42", "stdin")


@pytest.mark.integration
def test_shim_process_has_not_loaded_pydantic_or_nats(tmp_path: Path) -> None:
    """F29: the launch path skips ``mftik/__init__.py``, so the shim
    process itself has not imported the broker."""
    spec = _spec(_argv(_SLEEP))
    spawned = spawn_shim(spec, work_dir=tmp_path)
    try:
        text = log_path(tmp_path, spec.id, "stderr").read_text()
        assert "boot pydantic=0 nats=0" in text
    finally:
        _stop(spawned)


@pytest.mark.integration
def test_oom_score_adj_and_rlimit_are_applied_before_exec(tmp_path: Path) -> None:
    """§4.7: the child writes oom_score_adj and, when set, RLIMIT_DATA
    after fork and before exec. The shim itself stays at 0."""
    marker = tmp_path / "pid"
    spec = _spec(
        _argv(_PID, str(marker)),
        oom_score_adj=100,
        rlimit_data_bytes=50_000_000,
    )
    spawned = spawn_shim(spec, work_dir=tmp_path)
    try:
        _wait_for(marker.exists)
        worker = int(marker.read_text())
        assert Path(f"/proc/{worker}/oom_score_adj").read_text().strip() == "100"
        limits = Path(f"/proc/{worker}/limits").read_text()
        data = next(line for line in limits.splitlines() if "Max data size" in line)
        assert "50000000" in data
        shim_oom = Path(f"/proc/{spawned.pid}/oom_score_adj").read_text().strip()
        assert shim_oom == "0"
    finally:
        _stop(spawned)


@pytest.mark.integration
def test_status_counts_valid_heartbeats_only(tmp_path: Path) -> None:
    """B3-02: a beat counts even when ``ready`` does not change. A torn,
    undecodable or oversized line does not. Counting does not kill the
    worker; the shim does not enforce ``hb_timeout_s``."""
    marker = tmp_path / "sent"
    spec = _spec(_argv(_BEATS, str(marker)))
    spawned = spawn_shim(spec, work_dir=tmp_path)
    try:
        _wait_for(marker.exists)

        def _counted() -> bool:
            status = ShimClient(spawned.socket).status()
            return status.ready is True and status.beats == 3

        _wait_for(_counted)
        status = ShimClient(spawned.socket).status()
        assert status.beats == 3
        assert status.ready is True
        assert status.rss_bytes is None
        assert status.exit_code is None
        assert status.signal is None
        assert status.pid is not None
        assert Path(f"/proc/{status.pid}").exists()
    finally:
        _stop(spawned)


@pytest.mark.integration
def test_stdio_log_rotates_past_the_cap(tmp_path: Path) -> None:
    """S4: the shim writes stdout to a log and rotates it. No file grows
    without bound while the worker is still running."""
    marker = tmp_path / "wrote"
    spec = _spec(_argv(_STDOUT, str(marker)))
    spawned = spawn_shim(spec, work_dir=tmp_path)
    try:
        _wait_for(marker.exists)
        stdout = log_path(tmp_path, spec.id, "stdout")
        files = [stdout, stdout.with_name(stdout.name + ".1")]
        assert stdout.exists()
        assert files[1].exists()
        for path in files:
            assert path.stat().st_size <= LOG_MAX_BYTES
        status = ShimClient(spawned.socket).status()
        assert status.exit_code is None
        assert status.signal is None
    finally:
        _stop(spawned)
