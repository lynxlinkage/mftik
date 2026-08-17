"""Local registry trees become resolvable strategy classes on STS boot."""

from __future__ import annotations

from mftik.registry import RegistryStore
from mftik_sts.impl import load_local_registry, resolve, resolve_class
from mftik_sts.impl.noop import NoopStrategy

_TINY = """\
from mftik_sts.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""


def test_add_then_load_is_resolvable(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    loaded = load_local_registry(store)
    assert "private::Tiny" in loaded
    assert resolve("private::Tiny").name == "tiny"
    assert resolve_class("private::Tiny").__name__ == "Tiny"


def test_builtin_name_is_not_overwritten(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add(
        {
            "strategy.py": (
                "from mftik_sts.strategy import Strategy\n"
                "class Hijack(Strategy):\n"
                '    name = "noop"\n'
            )
        }
    )
    loaded = load_local_registry(store)
    assert loaded == []
    assert resolve_class("noop") is NoopStrategy


def test_a_broken_tree_does_not_block_the_others(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    store.add(
        {
            "strategy.py": (
                "from mftik_sts.strategy import Strategy\n"
                "class Broken(Strategy):\n"
                '    name = "broken"\n'
                "\n"
                'raise RuntimeError("boom")\n'
            )
        }
    )
    loaded = load_local_registry(store)
    assert loaded == ["private::Tiny"]
    assert resolve("private::Tiny").name == "tiny"


def test_pulled_tree_is_qualified_with_the_remote_name(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY}, origin="node1")
    loaded = load_local_registry(store)
    assert loaded == ["node1::Tiny"]
    assert resolve("node1::Tiny").name == "tiny"


def test_public_and_private_both_load(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    store.add({"strategy.py": _TINY}, origin="public")
    loaded = load_local_registry(store)
    assert loaded == ["public::Tiny", "private::Tiny"]
    assert resolve("public::Tiny").name == "tiny"
    assert resolve("private::Tiny").name == "tiny"
