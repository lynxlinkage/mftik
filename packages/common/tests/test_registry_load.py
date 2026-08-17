"""Load a registry tree as an isolated package so two helpers.py can coexist."""

from __future__ import annotations

import pytest
from mftik.registry.errors import RegistryError
from mftik.registry.load import load_class


def test_single_file_class(tmp_path) -> None:
    dest = tmp_path / "tiny"
    dest.mkdir()
    (dest / "strategy.py").write_text("class Tiny:\n    name = 'tiny'\n")
    cls = load_class(dest, type_name="Tiny", source="local", name="tiny")
    assert cls.__name__ == "Tiny"
    assert cls.name == "tiny"


def test_flat_sibling_import(tmp_path) -> None:
    dest = tmp_path / "tiny"
    dest.mkdir()
    (dest / "helpers.py").write_text("N = 7\n")
    (dest / "strategy.py").write_text(
        "from helpers import N\nclass Tiny:\n    n = N\n"
    )
    cls = load_class(dest, type_name="Tiny", source="local", name="tiny")
    assert cls.n == 7


def test_relative_import_in_a_package(tmp_path) -> None:
    dest = tmp_path / "tiny"
    (dest / "pkg").mkdir(parents=True)
    (dest / "pkg" / "__init__.py").write_text("")
    (dest / "pkg" / "helpers.py").write_text("N = 3\n")
    (dest / "pkg" / "strategy.py").write_text(
        "from .helpers import N\nclass Tiny:\n    n = N\n"
    )
    cls = load_class(dest, type_name="Tiny", source="local", name="tiny")
    assert cls.n == 3


def test_two_trees_do_not_share_helpers(tmp_path) -> None:
    """The reason for the unique package name: both trees ship helpers.py."""
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    (a / "helpers.py").write_text("N = 1\n")
    (a / "strategy.py").write_text("from helpers import N\nclass A:\n    n = N\n")
    (b / "helpers.py").write_text("N = 2\n")
    (b / "strategy.py").write_text("from helpers import N\nclass B:\n    n = N\n")
    cls_a = load_class(a, type_name="A", source="local", name="a")
    cls_b = load_class(b, type_name="B", source="local", name="b")
    assert cls_a.n == 1
    assert cls_b.n == 2


def test_missing_class_is_refused(tmp_path) -> None:
    dest = tmp_path / "tiny"
    dest.mkdir()
    (dest / "strategy.py").write_text("class Other:\n    pass\n")
    with pytest.raises(RegistryError, match="Tiny"):
        load_class(dest, type_name="Tiny", source="local", name="tiny")
