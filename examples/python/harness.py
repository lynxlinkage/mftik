"""Run one example session against the mftik-testkit shim.

The Rust test ``crates/mftik-py/tests/examples.rs`` owns the fake shim,
nats-server, and the MD, TD, and SYM peers. This module writes the scenario
and reads the report. ``MFTIK_PYTHON`` is the interpreter whose ``mftik``
wheel is installed; it defaults to the current one.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]


def drive(scenario: dict[str, Any]) -> dict[str, Any]:
    """Run ``scenario`` and return the worker report."""
    work = Path(tempfile.mkdtemp(prefix="mftik-example-"))
    try:
        scenario_path = work / "scenario.json"
        report_path = work / "report.json"
        scenario_path.write_text(json.dumps(scenario), encoding="utf-8")
        env = os.environ.copy()
        env["MFTIK_PYTHON"] = env.get("MFTIK_PYTHON") or sys.executable
        env["MFTIK_EXAMPLE_SCENARIO"] = str(scenario_path)
        env["MFTIK_EXAMPLE_REPORT"] = str(report_path)
        proc = subprocess.run(
            [
                "cargo",
                "test",
                "-p",
                "mftik-py",
                "--test",
                "examples",
                "--",
                "--ignored",
                "--test-threads=1",
                "--exact",
                "drive",
            ],
            cwd=_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0 or not report_path.is_file():
            raise AssertionError(
                f"example drive failed ({proc.returncode})\n{proc.stdout}\n{proc.stderr}"
            )
        report = json.loads(report_path.read_text(encoding="utf-8"))
        if not isinstance(report, dict):
            raise AssertionError(repr(report))
        return report
    finally:
        shutil.rmtree(work, ignore_errors=True)


def logs(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Strategy JSON lines. The runtime's ``gate`` line is left out."""
    found: list[dict[str, Any]] = []
    stdout = report.get("stdout", "")
    if not isinstance(stdout, str):
        raise AssertionError(repr(stdout))
    for line in stdout.splitlines():
        text = line.strip()
        if not text.startswith("{"):
            continue
        row = json.loads(text)
        if not isinstance(row, dict):
            raise AssertionError(text)
        if row.get("hook") == "gate":
            continue
        found.append(row)
    return found


def by_hook(rows: list[dict[str, Any]], hook: str) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("hook") == hook]
