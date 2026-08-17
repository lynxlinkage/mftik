"""Version pick / rewrite live in the updater image; imported here by path."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "mftik_updater", ROOT / "deploy" / "updater" / "server.py"
)
assert SPEC and SPEC.loader
updater = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(updater)


def test_pick_version_prefers_the_newest_v_tag() -> None:
    assert updater.pick_version(["latest", "v0.0.1", "v0.0.10", "v0.0.2"]) == "v0.0.10"


def test_pick_version_falls_back_to_latest() -> None:
    assert updater.pick_version(["sha-abc", "latest"]) == "latest"


def test_pick_version_rejects_an_empty_list() -> None:
    with pytest.raises(RuntimeError, match="no tags"):
        updater.pick_version([])


def test_rewrite_version_replaces_only_that_line(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text(
        "MFTIK_VERSION=v0.0.1\nMFTIK_UPDATER_TOKEN=secret\n",
        encoding="utf-8",
    )
    updater.rewrite_version(str(env), "v0.0.2")
    assert env.read_text(encoding="utf-8") == (
        "MFTIK_VERSION=v0.0.2\nMFTIK_UPDATER_TOKEN=secret\n"
    )


def test_rewrite_version_appends_when_missing(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text("REDIS_URL=redis://localhost\n", encoding="utf-8")
    updater.rewrite_version(str(env), "v1.0.0")
    assert "MFTIK_VERSION=v1.0.0\n" in env.read_text(encoding="utf-8")
