"""The distribution version comes from MFTIK_DIST_VERSION, not a literal.

A tag sets that variable; a source tree does not. The wheel we would
publish, and the metadata ``uv sync`` writes into an image, have to agree
with it — a second copy of the number is how #94 went stale.
"""

from __future__ import annotations

import os
import subprocess
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]


def _wheel_version(wheel: Path) -> str:
    with zipfile.ZipFile(wheel) as zf:
        meta = next(n for n in zf.namelist() if n.endswith(".dist-info/METADATA"))
        text = zf.read(meta).decode()
    for line in text.splitlines():
        if line.startswith("Version:"):
            return line.split(":", 1)[1].strip()
    raise AssertionError(f"no Version in {wheel}")


def _build(out: Path, env: dict[str, str] | None = None) -> str:
    cmd_env = os.environ.copy()
    if env is None:
        cmd_env.pop("MFTIK_DIST_VERSION", None)
    else:
        cmd_env.update(env)
    subprocess.run(
        ["uv", "build", "--package", "mftik", "--out-dir", str(out)],
        check=True,
        cwd=REPO,
        env=cmd_env,
        capture_output=True,
        text=True,
    )
    wheels = list(out.glob("mftik-*.whl"))
    assert len(wheels) == 1, wheels
    return _wheel_version(wheels[0])


def test_unset_is_not_a_release(tmp_path: Path) -> None:
    assert _build(tmp_path) == "0.0.0"


def test_the_tag_is_the_wheel_version(tmp_path: Path) -> None:
    assert _build(tmp_path, {"MFTIK_DIST_VERSION": "1.2.3"}) == "1.2.3"
