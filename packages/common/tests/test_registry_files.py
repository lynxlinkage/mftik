"""``.py`` files and an optional root ``strategy.yml`` enter a tree."""

from __future__ import annotations

from pathlib import Path

import pytest
from mftik.registry.errors import RegistryError
from mftik.registry.files import TEMPLATE_NAME, normalize_files, read_tree


def test_non_py_is_dropped() -> None:
    out = normalize_files(
        {
            "strategy.py": b"x = 1\n",
            "mftik-strategy.toml": b'name = "a"\n',
            "README.md": b"hi\n",
        }
    )
    assert list(out) == ["strategy.py"]


def test_root_strategy_yml_is_kept() -> None:
    out = normalize_files(
        {
            "strategy.py": b"x = 1\n",
            TEMPLATE_NAME: b"sts: {}\n",
            "README.md": b"hi\n",
            "pkg/strategy.yml": b"sts: {}\n",
        }
    )
    assert set(out) == {"strategy.py", TEMPLATE_NAME}
    assert out[TEMPLATE_NAME] == b"sts: {}\n"


def test_pycache_and_pyc_are_dropped() -> None:
    out = normalize_files(
        {
            "strategy.py": b"x = 1\n",
            "__pycache__/strategy.cpython-312.pyc": b"junk",
            "mod.pyc": b"junk",
        }
    )
    assert list(out) == ["strategy.py"]


def test_empty_after_skip_is_refused() -> None:
    with pytest.raises(RegistryError, match=r"no \.py files"):
        normalize_files({"mftik-strategy.toml": b"name = \"x\"\n"})


def test_yml_alone_is_refused() -> None:
    with pytest.raises(RegistryError, match=r"no \.py files"):
        normalize_files({TEMPLATE_NAME: b"sts: {}\n"})


def test_read_tree_picks_up_root_yml(tmp_path: Path) -> None:
    root = tmp_path / "hello"
    root.mkdir()
    (root / "strategy.py").write_text("x = 1\n")
    (root / TEMPLATE_NAME).write_text("sts: {}\n")
    (root / "pkg").mkdir()
    (root / "pkg" / "strategy.yml").write_text("nested\n")
    files = read_tree(root)
    assert set(files) == {"strategy.py", TEMPLATE_NAME}
    assert files[TEMPLATE_NAME] == b"sts: {}\n"
