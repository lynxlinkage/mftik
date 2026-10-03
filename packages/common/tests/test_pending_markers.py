"""The marker check fails when the retired phrase is put back (§9.1 unit).

The script is what CI runs. These cases use a temporary tree, so a
failure here is the check itself and not a marker in the repo. The
phrase is built here so this file does not contain it.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "check_pending_markers.py"


def _load():
    spec = importlib.util.spec_from_file_location("check_pending_markers", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tree(root: Path) -> None:
    (root / "apps").mkdir(parents=True)
    (root / "packages").mkdir()


def test_a_marker_fails_and_a_clean_tree_passes(tmp_path: Path) -> None:
    script = _load()
    phrase = "pending " + "Yi Te"
    cited_line = "待決"
    cited_line += "，見 #286\n"
    quoted = "待決"
    quoted += " #286\n"
    clean = tmp_path / "clean"
    _tree(clean)
    (clean / "docs").mkdir()
    (clean / "apps" / "ok.py").write_text("x = 1\n", encoding="utf-8")
    (clean / "docs" / "plan.md").write_text(
        phrase + " (#286)\n" + quoted,
        encoding="utf-8",
    )
    assert script.main(["check_pending_markers.py", str(clean)]) == 0

    marked = tmp_path / "marked"
    _tree(marked)
    (marked / "packages" / "inner").mkdir()
    (marked / "packages" / "inner" / "a.py").write_text(
        "# Pending " + "Yi Te\n",
        encoding="utf-8",
    )
    assert script.main(["check_pending_markers.py", str(marked)]) == 1

    cited = tmp_path / "cited"
    _tree(cited)
    (cited / "apps" / "a.py").write_text("# " + cited_line, encoding="utf-8")
    assert script.main(["check_pending_markers.py", str(cited)]) == 1

    other = tmp_path / "other"
    _tree(other)
    (other / "apps" / "a.py").write_text("# 待決，別的票\n", encoding="utf-8")
    assert script.main(["check_pending_markers.py", str(other)]) == 0
