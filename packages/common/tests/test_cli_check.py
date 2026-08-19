"""``mftik check`` — the four layers, and that they stop at the first refusal."""

from __future__ import annotations

from pathlib import Path

from mftik.cli.app import EXIT_ERROR, main

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""

_QTY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"

    @classmethod
    def on_initialized(cls, params):
        params = super().on_initialized(params)
        if "qty" not in params:
            raise ValueError("qty is required")
        return params
"""

_OK_YML = "td: []\nmd: []\nsts:\n  qty: 1\n"


def _tree(tmp_path: Path, source: str, name: str = "hello") -> Path:
    dest = tmp_path / name
    dest.mkdir()
    (dest / "strategy.py").write_text(source)
    return dest


def test_a_tree_without_config_is_ok(tmp_path: Path, capsys) -> None:
    dest = _tree(tmp_path, _TINY)

    assert main(["check", str(dest)]) == 0
    out = capsys.readouterr().out
    assert out.startswith("ok  tiny  (Tiny)")
    # Silence about the config would read as "the config is fine". There was none.
    assert "no strategy.yml" in out
    assert "parameters were not" in out


def test_a_tree_with_yml_runs_on_initialized(tmp_path: Path, capsys) -> None:
    dest = _tree(tmp_path, _QTY)
    cfg = tmp_path / "deploy.yml"
    cfg.write_text(_OK_YML)

    assert main(["check", str(dest), str(cfg)]) == 0
    out = capsys.readouterr().out
    assert out.startswith("ok  tiny  (Tiny)")
    assert "accepted by on_initialized" in out
    # The digest is what a push sends and what the node stores it under, so
    # it is worth being able to eyeball against `mftik registry ls`.
    assert "sha256:" in out


def test_strategy_yml_in_the_tree_is_used_when_cfg_is_omitted(
    tmp_path: Path, capsys
) -> None:
    dest = _tree(tmp_path, _QTY)
    (dest / "strategy.yml").write_text(_OK_YML)

    assert main(["check", str(dest)]) == 0
    out = capsys.readouterr().out
    assert "accepted by on_initialized" in out
    assert str(dest / "strategy.yml") in out


def test_nested_strategy_yml_is_used_when_cfg_is_omitted(
    tmp_path: Path, capsys
) -> None:
    dest = tmp_path / "hello"
    pkg = dest / "pkg"
    pkg.mkdir(parents=True)
    (pkg / "strategy.py").write_text(_QTY)
    (pkg / "strategy.yml").write_text(_OK_YML)

    assert main(["check", str(dest)]) == 0
    out = capsys.readouterr().out
    assert "accepted by on_initialized" in out
    assert str(pkg / "strategy.yml") in out


def test_third_party_import_is_refused(tmp_path: Path, capsys) -> None:
    dest = _tree(
        tmp_path,
        "import numpy\nfrom mftik.strategy import Strategy\n"
        "class Tiny(Strategy):\n    name = \"tiny\"\n",
    )

    assert main(["check", str(dest)]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "numpy" in err
    assert "Traceback" not in err


def test_declared_third_party_import_is_ok(tmp_path: Path, capsys) -> None:
    """Gate accepts a declared extra without the laptop having the package.

    The import lives in a method so ``load_class`` does not execute it —
    ``check`` still loads the tree, and looking at site-packages to skip
    that would be the node question this command does not ask.
    """
    dest = _tree(
        tmp_path,
        "from mftik.strategy import Strategy\n"
        "class Tiny(Strategy):\n    name = \"tiny\"\n"
        '    requires = ("numpy",)\n'
        "    def on_start(self):\n        import numpy\n",
    )

    assert main(["check", str(dest)]) == 0
    assert "tiny" in capsys.readouterr().out


def test_uppercase_name_is_refused(tmp_path: Path, capsys) -> None:
    dest = _tree(
        tmp_path,
        "from mftik.strategy import Strategy\n"
        "class Tiny(Strategy):\n    name = \"Tiny\"\n",
    )

    assert main(["check", str(dest)]) == EXIT_ERROR
    assert "lowercase" in capsys.readouterr().err


def test_missing_name_is_refused(tmp_path: Path, capsys) -> None:
    dest = _tree(
        tmp_path,
        "from mftik.strategy import Strategy\nclass Tiny(Strategy):\n    pass\n",
    )

    assert main(["check", str(dest)]) == EXIT_ERROR
    assert "has no name" in capsys.readouterr().err


def test_bad_yaml_is_refused(tmp_path: Path, capsys) -> None:
    dest = _tree(tmp_path, _TINY)
    cfg = tmp_path / "deploy.yml"
    cfg.write_text("td: [\n")

    assert main(["check", str(dest), str(cfg)]) == EXIT_ERROR
    assert "invalid YAML" in capsys.readouterr().err


def test_bad_yml_in_the_tree_is_refused(tmp_path: Path, capsys) -> None:
    dest = _tree(tmp_path, _TINY)
    (dest / "strategy.yml").write_text("td: [\n")

    assert main(["check", str(dest)]) == EXIT_ERROR
    assert "strategy.yml" in capsys.readouterr().err


def test_illegal_md_is_refused(tmp_path: Path, capsys) -> None:
    dest = _tree(tmp_path, _TINY)
    cfg = tmp_path / "deploy.yml"
    cfg.write_text("td: []\nmd: [not-a-ticker]\nsts: {}\n")

    assert main(["check", str(dest), str(cfg)]) == EXIT_ERROR
    assert "md entry" in capsys.readouterr().err


def test_a_name_error_at_import_is_refused(tmp_path: Path, capsys) -> None:
    dest = _tree(
        tmp_path,
        "from mftik.strategy import Strategy\n"
        "nope\n"
        "class Tiny(Strategy):\n    name = \"tiny\"\n",
    )

    assert main(["check", str(dest)]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "nope" in err
    assert "Traceback" not in err


def test_on_initialized_refusal_is_named(tmp_path: Path, capsys) -> None:
    dest = _tree(tmp_path, _QTY)
    cfg = tmp_path / "deploy.yml"
    cfg.write_text("td: []\nmd: []\nsts: {}\n")

    assert main(["check", str(dest), str(cfg)]) == EXIT_ERROR
    err = capsys.readouterr().err
    # The type is part of it: "qty is required" alone does not say whether
    # the strategy refused the config or the platform failed to ask.
    assert "Tiny.on_initialized raised ValueError: qty is required" in err
    assert "--traceback" in err
    assert "Traceback" not in err


def test_traceback_shows_where_the_strategy_raised(tmp_path: Path, capsys) -> None:
    """The message alone rarely locates a line in somebody's own code."""
    dest = _tree(tmp_path, _QTY)
    cfg = tmp_path / "deploy.yml"
    cfg.write_text("td: []\nmd: []\nsts: {}\n")

    assert main(["check", str(dest), str(cfg), "--traceback"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "Traceback" in err
    assert "strategy.py" in err
    assert 'raise ValueError("qty is required")' in err


def test_a_tree_that_raises_at_import_names_the_exception(
    tmp_path: Path, capsys
) -> None:
    dest = _tree(tmp_path, _TINY + '\nraise RuntimeError("boom")\n')

    assert main(["check", str(dest)]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "importing the tree raised RuntimeError: boom" in err


def test_missing_directory_exits_one(tmp_path: Path, capsys) -> None:
    assert main(["check", str(tmp_path / "gone")]) == EXIT_ERROR
    assert "does not exist" in capsys.readouterr().err


def test_a_file_is_refused(tmp_path: Path, capsys) -> None:
    path = tmp_path / "strategy.py"
    path.write_text(_TINY)

    assert main(["check", str(path)]) == EXIT_ERROR
    assert "directory" in capsys.readouterr().err


def test_an_explicit_missing_cfg_exits_one(tmp_path: Path, capsys) -> None:
    dest = _tree(tmp_path, _TINY)

    assert main(["check", str(dest), str(tmp_path / "gone.yml")]) == EXIT_ERROR
    assert "does not exist" in capsys.readouterr().err
