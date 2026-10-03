"""The import probe spawns a real interpreter (integration, §9.1).

The controller process that calls :func:`mftik_sts.hostdisk.probe` does
not gain the strategy-tree module. A failed import comes back ``skipped``
with a reason.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from mftik.registry.digest import digest_files
from mftik.registry.files import normalize_files
from mftik_sts.hostdisk import TreeReplica, probe

pytestmark = pytest.mark.integration

_TINY = (
    "from mftik.strategy import Strategy\n"
    "class Tiny(Strategy):\n"
    "    pass\n"
)
_TINY_V2 = _TINY + "# next\n"
_BROKEN = (
    "from mftik.strategy import Strategy\n"
    "import not_a_real_extra_if16\n"
    "class Tiny(Strategy):\n"
    "    requires = ('not_a_real_extra_if16',)\n"
)


def _put(replica: TreeReplica, source: str) -> str:
    files = {"strategy.py": source}
    digest = digest_files(normalize_files(files))
    replica.put(digest, files)
    return digest


def _tree_modules(root: Path) -> list[str]:
    found: list[str] = []
    for name, module in sys.modules.items():
        file = getattr(module, "__file__", None)
        if not isinstance(file, str):
            continue
        try:
            Path(file).resolve().relative_to(root)
        except ValueError:
            continue
        found.append(name)
    return found


def test_a_probe_import_failure_is_skipped_with_a_reason(tmp_path: Path) -> None:
    replica = TreeReplica(tmp_path)
    digest = _put(replica, _BROKEN)
    before = _tree_modules(replica.trees_dir)
    result = probe(digest, None, replica=replica)
    assert result.status == "skipped"
    assert result.reason is not None
    assert "import error" in result.reason
    assert "not_a_real_extra_if16" in result.reason
    assert _tree_modules(replica.trees_dir) == before
    assert "mftik_sts.hostdisk.probe_child" not in sys.modules


def test_a_pinned_old_digest_still_loads_after_the_name_moves(tmp_path: Path) -> None:
    """The index names the new digest. Probing the old one still loads it,
    and the controller's ``sys.modules`` does not."""
    replica = TreeReplica(tmp_path)
    old = _put(replica, _TINY)
    replica.bind("Tiny", old)
    new = _put(replica, _TINY_V2)
    replica.bind("Tiny", new)
    assert replica.current("Tiny") == new
    before = {name for name in sys.modules if name.startswith("_mftik_reg_")}
    result = probe(old, None, replica=replica)
    assert result.status == "loaded"
    assert result.reason is None
    after = {name for name in sys.modules if name.startswith("_mftik_reg_")}
    assert after == before
    assert _tree_modules(replica.trees_dir) == []


def test_the_controller_process_holds_no_strategy_tree_module(tmp_path: Path) -> None:
    """Importing the controller and probing a tree leaves no strategy-tree
    module in this process."""
    import mftik_sts.controller  # noqa: F401
    import mftik_sts.hostdisk  # noqa: F401

    replica = TreeReplica(tmp_path)
    digest = _put(replica, _TINY)
    result = probe(digest, None, replica=replica)
    assert result.status == "loaded"
    leaked = [
        name
        for name, module in sys.modules.items()
        if name.startswith("_mftik_reg_")
        or (
            isinstance(getattr(module, "__file__", None), str)
            and "registry/trees/" in getattr(module, "__file__", "")
        )
    ]
    assert leaked == []
