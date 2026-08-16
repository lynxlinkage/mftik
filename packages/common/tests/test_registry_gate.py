"""The import gate is what makes a tree copyable to another node.

Stdlib and the SDK travel because every node has them. A third-party import
would be a missing module after a pull. Dynamic imports are refused because
the scan would not see them.
"""

from __future__ import annotations

import pytest
from mft.registry.errors import RegistryError
from mft.registry.gate import check_files

_TINY = """\
from mft_sts.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""


def _py(source: str, name: str = "strategy.py") -> dict[str, bytes]:
    return {name: source.encode("utf-8")}


def test_sdk_and_stdlib_imports_are_allowed() -> None:
    source = """\
from __future__ import annotations
from decimal import Decimal
from typing import Any

from mft.exchange.models import Order
from mft_sts.strategy import Strategy
from mft_sts.timer import TimerToken

class Tiny(Strategy):
    name = "tiny"
"""
    found = check_files(_py(source))
    assert len(found) == 1
    assert found[0].type == "Tiny"
    assert found[0].name == "tiny"


def test_third_party_import_is_refused() -> None:
    source = """\
import numpy
from mft_sts.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""
    with pytest.raises(RegistryError, match="numpy"):
        check_files(_py(source))


def test_importlib_is_refused() -> None:
    source = """\
import importlib
from mft_sts.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""
    with pytest.raises(RegistryError, match="dynamic"):
        check_files(_py(source))


def test_dunder_import_is_refused() -> None:
    source = """\
from mft_sts.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"

    def on_start(self):
        __import__("numpy")
"""
    with pytest.raises(RegistryError, match="dynamic import"):
        check_files(_py(source))


def test_type_checking_imports_are_ignored() -> None:
    """A TYPE_CHECKING pandas import must not block a strategy that never loads it."""
    source = """\
from typing import TYPE_CHECKING
from mft_sts.strategy import Strategy

if TYPE_CHECKING:
    import pandas

class Tiny(Strategy):
    name = "tiny"
"""
    found = check_files(_py(source))
    assert found[0].name == "tiny"


def test_sibling_module_is_allowed() -> None:
    files = {
        "strategy.py": (
            b"from helpers import N\n"
            b"from mft_sts.strategy import Strategy\n"
            b"class Tiny(Strategy):\n"
            b'    name = "tiny"\n'
        ),
        "helpers.py": b"N = 1\n",
    }
    assert check_files(files)[0].type == "Tiny"


def test_missing_sibling_is_refused() -> None:
    source = """\
from missing import N
from mft_sts.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""
    with pytest.raises(RegistryError, match="missing"):
        check_files(_py(source))


def test_relative_import_inside_a_package() -> None:
    files = {
        "pkg/__init__.py": b"",
        "pkg/helpers.py": b"N = 1\n",
        "pkg/strategy.py": (
            b"from .helpers import N\n"
            b"from mft_sts.strategy import Strategy\n"
            b"class Tiny(Strategy):\n"
            b'    name = "tiny"\n'
        ),
    }
    assert check_files(files)[0].filename == "pkg/strategy.py"


def test_relative_import_at_top_level_is_refused() -> None:
    source = """\
from .helpers import N
from mft_sts.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""
    with pytest.raises(RegistryError, match="package"):
        check_files(_py(source))


def test_strategy_via_module_attribute() -> None:
    source = """\
import mft_sts.strategy

class Tiny(mft_sts.strategy.Strategy):
    name = "tiny"
"""
    found = check_files(_py(source))
    assert found[0].type == "Tiny"


def test_annotated_name_is_read() -> None:
    source = """\
from mft_sts.strategy import Strategy

class Tiny(Strategy):
    name: str = "tiny"
"""
    assert check_files(_py(source))[0].name == "tiny"


def test_requires_mft_is_read() -> None:
    source = """\
from mft_sts.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
    requires_mft = "0.2.0"
"""
    found = check_files(_py(source))[0]
    assert found.requires_mft == "0.2.0"


def test_invalid_python_is_refused() -> None:
    with pytest.raises(RegistryError, match="not valid Python"):
        check_files(_py("def (\n"))


def test_shadowing_the_sdk_is_refused() -> None:
    files = {
        "mft.py": b"x = 1\n",
        "strategy.py": _TINY.encode(),
    }
    with pytest.raises(RegistryError, match="provided by the node"):
        check_files(files)
