"""Offline rename of registry trees onto their class name."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from mftik.cli.app import main
from mftik.registry.errors import RegistryError
from mftik.registry.migrate import migrate_registry


def _plant(root: Path, dirname: str, cls: str) -> None:
    dest = root / dirname
    dest.mkdir(parents=True)
    (dest / "strategy.py").write_text(
        "from mftik.strategy import Strategy\n"
        f"\nclass {cls}(Strategy):\n"
        "    pass\n"
    )


def test_short_directories_move_through_a_temp_name_and_pulled_goes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "data"
    private = data / "registry" / "private"
    public = data / "registry" / "public"
    _plant(private, "tiny", "Tiny")
    _plant(public, "other", "Other")
    pulled = data / "registry" / "pulled" / "node1" / "tiny"
    pulled.mkdir(parents=True)
    (pulled / "strategy.py").write_text("x = 1\n")

    calls: list[tuple[str, str]] = []
    real = Path.rename

    def spy(self: Path, target: Path) -> Path:
        calls.append((self.name, Path(target).name))
        return real(self, target)

    monkeypatch.setattr(Path, "rename", spy)
    result = migrate_registry(data)

    assert any(name.startswith(".tmp-migrate-") for pair in calls for name in pair)
    assert os.listdir(private) == ["Tiny"]
    assert os.listdir(public) == ["Other"]
    assert "class Tiny" in (private / "Tiny" / "strategy.py").read_text()
    assert not (data / "registry" / "pulled" / "node1").exists()
    assert result.removed_pulled == ("node1",)
    assert len(result.renamed) == 2


def test_a_second_run_changes_nothing(tmp_path: Path) -> None:
    data = tmp_path / "data"
    _plant(data / "registry" / "private", "Tiny", "Tiny")

    result = migrate_registry(data)

    assert result.renamed == ()
    assert result.removed_pulled == ()
    assert os.listdir(data / "registry" / "private") == ["Tiny"]


def test_duplicate_class_names_refuse_the_whole_batch(tmp_path: Path) -> None:
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, "tiny", "Tiny")
    _plant(private, "other", "Tiny")
    pulled = data / "registry" / "pulled" / "node1"
    pulled.mkdir(parents=True)

    with pytest.raises(RegistryError, match="collide"):
        migrate_registry(data)

    assert {p.name for p in private.iterdir()} == {"tiny", "other"}
    assert pulled.is_dir()


def test_casefold_targets_refuse_the_whole_batch(tmp_path: Path) -> None:
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, "left", "Tiny")
    _plant(private, "right", "tiny")

    with pytest.raises(RegistryError, match="collide"):
        migrate_registry(data)

    assert {p.name for p in private.iterdir()} == {"left", "right"}


def test_the_command_is_idempotent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    data = tmp_path / "data"
    _plant(data / "registry" / "private", "tiny", "Tiny")

    assert main(["registry-migrate", "--data", str(data)]) == 0
    assert "renamed" in capsys.readouterr().out
    assert main(["registry-migrate", "--data", str(data)]) == 0
    assert "already uses class names" in capsys.readouterr().out
    assert os.listdir(data / "registry" / "private") == ["Tiny"]
