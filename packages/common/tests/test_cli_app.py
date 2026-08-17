"""Dispatch and exit codes.

The codes are a contract: a CI job retries a 2 and does not retry a 1, so
which errors map to which is asserted rather than left to whatever the
exception happened to be.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from mftik.cli import config
from mftik.cli.app import (
    COMMANDS,
    EXIT_ERROR,
    EXIT_INTERRUPTED,
    EXIT_UNREACHABLE,
    main,
)
from mftik.cli.client import CliError, NodeUnreachable
from mftik.cli.config import ConfigError, Profile


@pytest.fixture(autouse=True)
def config_file(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "config.toml"
    monkeypatch.setenv(config.CONFIG_ENV, str(path))
    monkeypatch.delenv(config.PROFILE_ENV, raising=False)
    return path


def test_no_command_prints_help_and_fails(capsys) -> None:
    """Bare ``mftik`` is a mistake, not a no-op — so it must not exit 0."""
    assert main([]) == EXIT_ERROR
    assert "usage: mftik" in capsys.readouterr().out


def test_every_command_is_reachable_from_the_parser(capsys) -> None:
    """The help text and the dispatch read the same table, and this says so."""
    with pytest.raises(SystemExit):
        main(["--help"])
    out = capsys.readouterr().out
    for command in COMMANDS:
        assert command.name in out


def test_profiles_lists_what_was_connected(capsys) -> None:
    config.put(Profile(name="prod", url="https://n.example.com", token="t"))
    config.put(Profile(name="local", url="http://localhost:8000"))

    assert main(["profiles"]) == 0
    out = capsys.readouterr().out
    assert "prod" in out and "https://n.example.com" in out
    # The default is the last one connected, and is marked as such.
    assert "* local" in out or "*  local" in out
    # Whether a node issued a key is shown; the key itself never is.
    assert "key" in out and "none" in out
    assert "t" not in out.split()


def test_profiles_with_nothing_connected_is_not_an_error(capsys) -> None:
    assert main(["profiles"]) == 0
    assert "mftik connect" in capsys.readouterr().out


def test_disconnect_forgets_the_node(capsys) -> None:
    config.put(Profile(name="prod", url="https://n.example.com", token="t"))

    assert main(["disconnect", "prod"]) == 0
    assert config.load().profiles == {}
    # A key this machine has forgotten is still live on the node, and a user
    # who thinks otherwise has an unrevoked credential they believe is gone.
    assert "revoke it there" in capsys.readouterr().out


def test_disconnecting_an_unknown_node_exits_one(capsys) -> None:
    assert main(["disconnect", "nope"]) == EXIT_ERROR
    assert "unknown profile" in capsys.readouterr().err


# --- how failures come out -------------------------------------------------


def _raising(exc: Exception):  # noqa: ANN202
    def run(args) -> int:  # noqa: ANN001
        raise exc

    return run


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (CliError("fix your input"), EXIT_ERROR),
        (ConfigError("no node connected"), EXIT_ERROR),
        (NodeUnreachable("cannot reach it"), EXIT_UNREACHABLE),
        (KeyboardInterrupt(), EXIT_INTERRUPTED),
    ],
)
def test_exit_codes(monkeypatch, capsys, exc: Exception, expected: int) -> None:
    monkeypatch.setattr(
        "mftik.cli.app.COMMANDS",
        tuple(
            replace(c, run=_raising(exc)) if c.name == "profiles" else c
            for c in COMMANDS
        ),
    )

    assert main(["profiles"]) == expected
    # Interruption prints a newline first, so the shell prompt does not
    # continue the line the ^C landed on.
    assert capsys.readouterr().err.strip().startswith("mftik: ")


def test_an_error_is_a_line_on_stderr_not_a_traceback(capsys) -> None:
    """A typo'd profile name is not a bug in this tool, and must not read as one."""
    main(["disconnect", "nope"])

    err = capsys.readouterr().err
    assert "Traceback" not in err
    assert err.count("\n") == 1
