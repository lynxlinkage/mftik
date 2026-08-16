"""Only ``.py`` files enter a strategy tree."""

from __future__ import annotations

import pytest
from mft.registry.errors import RegistryError
from mft.registry.files import normalize_files


def test_non_py_is_dropped() -> None:
    out = normalize_files(
        {
            "strategy.py": b"x = 1\n",
            "mft-strategy.toml": b'name = "a"\n',
            "README.md": b"hi\n",
        }
    )
    assert list(out) == ["strategy.py"]


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
        normalize_files({"mft-strategy.toml": b"name = \"x\"\n"})
