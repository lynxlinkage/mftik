"""``.py`` files and an optional ``strategy.yml`` enter a tree."""

from __future__ import annotations

from pathlib import Path

import pytest
from mftik.registry.errors import RegistryError
from mftik.registry.files import TEMPLATE_NAME, find_template, normalize_files, read_tree


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


def test_nested_strategy_yml_is_lifted() -> None:
    """A package that put the sidecar next to the class still ships a template."""
    out = normalize_files(
        {
            "pkg/strategy.py": b"x = 1\n",
            "pkg/strategy.yml": b"sts:\n  qty: 1\n",
        }
    )
    assert set(out) == {"pkg/strategy.py", TEMPLATE_NAME}
    assert out[TEMPLATE_NAME] == b"sts:\n  qty: 1\n"


def test_two_nested_yml_are_refused() -> None:
    with pytest.raises(RegistryError, match="multiple"):
        normalize_files(
            {
                "a/strategy.py": b"x = 1\n",
                "a/strategy.yml": b"sts: {}\n",
                "b/strategy.yml": b"sts: {}\n",
            }
        )


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


def test_read_tree_lifts_a_nested_yml(tmp_path: Path) -> None:
    root = tmp_path / "hello"
    pkg = root / "pkg"
    pkg.mkdir(parents=True)
    (pkg / "strategy.py").write_text("x = 1\n")
    (pkg / TEMPLATE_NAME).write_text("sts:\n  qty: 1\n")
    files = read_tree(root)
    assert files[TEMPLATE_NAME] == b"sts:\n  qty: 1\n"
    assert find_template(root) == pkg / TEMPLATE_NAME


def test_read_tree_refuses_two_nested_yml(tmp_path: Path) -> None:
    root = tmp_path / "hello"
    for name in ("a", "b"):
        dest = root / name
        dest.mkdir(parents=True)
        (dest / TEMPLATE_NAME).write_text("sts: {}\n")
    (root / "strategy.py").write_text("x = 1\n")
    with pytest.raises(RegistryError, match="multiple"):
        read_tree(root)
