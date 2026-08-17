"""The profile store — what it keeps, and who can read it.

The file holds bearer tokens, so its mode is part of its behaviour and is
asserted rather than assumed.
"""

from __future__ import annotations

import stat
from pathlib import Path

import pytest
from mftik.cli import config
from mftik.cli.config import Config, ConfigError, Profile


@pytest.fixture(autouse=True)
def config_file(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "config.toml"
    monkeypatch.setenv(config.CONFIG_ENV, str(path))
    monkeypatch.delenv(config.PROFILE_ENV, raising=False)
    return path


def test_absent_file_is_empty_not_an_error(config_file: Path) -> None:
    """A machine that has never connected is not a machine with a broken file."""
    assert not config_file.exists()
    loaded = config.load()
    assert loaded.profiles == {}
    assert loaded.default is None


def test_a_profile_survives_a_round_trip(config_file: Path) -> None:
    config.put(Profile(name="prod", url="https://node.example.com/api", token="t"))

    loaded = config.load()
    assert loaded.default == "prod"
    assert loaded.profiles["prod"].url == "https://node.example.com/api"
    assert loaded.profiles["prod"].token == "t"


def test_the_file_is_not_readable_by_anyone_else(config_file: Path) -> None:
    """It holds a credential, so 0600 is the point rather than a nicety."""
    config.put(Profile(name="prod", url="https://n.example.com", token="secret"))

    mode = stat.S_IMODE(config_file.stat().st_mode)
    assert mode == 0o600


def test_rewriting_an_existing_file_keeps_it_narrow(config_file: Path) -> None:
    """O_CREAT leaves an existing file's mode alone, so the chmod must repeat."""
    config_file.write_text("")
    config_file.chmod(0o644)

    config.put(Profile(name="prod", url="https://n.example.com", token="secret"))

    assert stat.S_IMODE(config_file.stat().st_mode) == 0o600


def test_a_token_is_omitted_rather_than_written_empty(config_file: Path) -> None:
    """A node with its gate off issues no key, and none is what gets stored."""
    config.put(Profile(name="local", url="http://localhost:8000"))

    assert "token" not in config_file.read_text()
    assert config.load().profiles["local"].token is None


def test_connecting_a_second_node_moves_the_default(config_file: Path) -> None:
    config.put(Profile(name="one", url="https://one.example.com"))
    config.put(Profile(name="two", url="https://two.example.com"))

    loaded = config.load()
    assert loaded.default == "two"
    assert set(loaded.profiles) == {"one", "two"}


def test_a_second_node_can_be_added_without_taking_the_default(
    config_file: Path,
) -> None:
    config.put(Profile(name="one", url="https://one.example.com"))
    config.put(Profile(name="two", url="https://two.example.com"), make_default=False)

    assert config.load().default == "one"


def test_dropping_the_default_hands_it_to_a_survivor(config_file: Path) -> None:
    config.put(Profile(name="one", url="https://one.example.com"))
    config.put(Profile(name="two", url="https://two.example.com"))

    config.drop("two")

    loaded = config.load()
    assert loaded.default == "one"
    assert set(loaded.profiles) == {"one"}


def test_dropping_the_last_leaves_no_default(config_file: Path) -> None:
    config.put(Profile(name="one", url="https://one.example.com"))

    config.drop("one")

    loaded = config.load()
    assert loaded.profiles == {}
    assert loaded.default is None


def test_dropping_an_unknown_profile_lists_the_known_ones(
    config_file: Path,
) -> None:
    config.put(Profile(name="one", url="https://one.example.com"))

    with pytest.raises(ConfigError, match="known: one"):
        config.drop("nope")


# --- which profile a command acts on ---------------------------------------


def test_resolve_prefers_the_argument_over_everything(monkeypatch) -> None:
    monkeypatch.setenv(config.PROFILE_ENV, "two")
    loaded = Config(
        profiles={
            "one": Profile(name="one", url="u1"),
            "two": Profile(name="two", url="u2"),
        },
        default="two",
    )

    assert loaded.resolve("one").name == "one"


def test_resolve_prefers_the_environment_over_the_default(monkeypatch) -> None:
    """A shell that set this meant it, more recently than the last connect."""
    monkeypatch.setenv(config.PROFILE_ENV, "one")
    loaded = Config(
        profiles={
            "one": Profile(name="one", url="u1"),
            "two": Profile(name="two", url="u2"),
        },
        default="two",
    )

    assert loaded.resolve().name == "one"


def test_resolve_falls_back_to_the_default() -> None:
    loaded = Config(profiles={"two": Profile(name="two", url="u2")}, default="two")

    assert loaded.resolve().name == "two"


def test_resolve_with_nothing_connected_says_what_to_run() -> None:
    with pytest.raises(ConfigError, match="mftik connect"):
        Config(profiles={}).resolve()


def test_resolve_of_an_unknown_name_lists_the_known_ones() -> None:
    loaded = Config(profiles={"one": Profile(name="one", url="u1")}, default="one")

    with pytest.raises(ConfigError, match="known: one"):
        loaded.resolve("nope")


# --- reading what is on disk -----------------------------------------------


def test_a_corrupt_file_is_an_error_not_a_fresh_start(config_file: Path) -> None:
    """It holds the only copy of a key that was shown once.

    Silently starting over would lose it without saying so, which is worse
    than refusing and letting the user look at the file.
    """
    config_file.write_text("this is not toml {{{")

    with pytest.raises(ConfigError, match="cannot read"):
        config.load()


def test_a_default_naming_nothing_falls_back_to_a_real_profile(
    config_file: Path,
) -> None:
    """Hand-edited files happen, and a dangling default must not strand them."""
    config_file.write_text(
        'default = "gone"\n\n[profiles.one]\nurl = "https://one.example.com"\n'
    )

    assert config.load().default == "one"


def test_a_profile_without_a_url_is_skipped(config_file: Path) -> None:
    config_file.write_text(
        '[profiles.broken]\ntoken = "t"\n\n'
        '[profiles.good]\nurl = "https://good.example.com"\n'
    )

    assert set(config.load().profiles) == {"good"}


# --- naming a profile from its URL -----------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://node.example.com", "node_example_com"),
        ("https://Node.Example.COM", "node_example_com"),
        ("http://localhost:8000", "default"),
        ("http://127.0.0.1:8000", "default"),
        ("https://10.0.0.4:8000", "10_0_0_4"),
    ],
)
def test_default_name_is_derived_from_the_host(url: str, expected: str) -> None:
    assert config.default_name(url) == expected
