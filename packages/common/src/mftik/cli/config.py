"""Which nodes this machine knows, and the credential for each.

One file, ``~/.config/mftik/config.toml``, holding a table per profile. A
profile is a node you have run ``mftik connect`` against: its URL, the API key
it issued, and the name you gave it.

The file holds bearer tokens, so it is written the way the registry writes
``remotes.toml`` — opened at 0600 and written into that handle, rather than
created under the umask and narrowed afterwards, which leaves a window where
it is world-readable.
"""

from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass, replace
from pathlib import Path

#: Overrides which profile a command acts on, for a shell or a CI job that
#: should not depend on whichever one happens to be default.
PROFILE_ENV = "MFTIK_PROFILE"
#: Overrides the whole file. Mostly for tests and for a CI job that would
#: rather write a throwaway config than touch the runner's home directory.
CONFIG_ENV = "MFTIK_CONFIG"

_DEFAULT_NAME = "default"


class ConfigError(Exception):
    """The config file is unusable, or names something that is not there."""


@dataclass(frozen=True, slots=True)
class Profile:
    """One node this machine can talk to."""

    name: str
    url: str
    #: The `mftik_ak_` key this node issued. Absent when the node runs with
    #: its gate off, which is a normal way to run a local stack.
    token: str | None = None


def config_path() -> Path:
    """Where the profiles live.

    ``XDG_CONFIG_HOME`` is honoured because a user who has moved their config
    directory means it, and a tool that writes to ``~/.config`` anyway is the
    reason that variable keeps having to be set again.
    """
    override = os.getenv(CONFIG_ENV, "").strip()
    if override:
        return Path(override).expanduser()
    base = os.getenv("XDG_CONFIG_HOME", "").strip()
    root = Path(base).expanduser() if base else Path.home() / ".config"
    return root / "mftik" / "config.toml"


@dataclass(frozen=True, slots=True)
class Config:
    """Every profile on this machine, and which one commands default to."""

    profiles: dict[str, Profile]
    default: str | None = None

    def resolve(self, name: str | None = None) -> Profile:
        """The profile a command should use, or a refusal that says why.

        Precedence is argument, then ``MFTIK_PROFILE``, then the default set
        by the last ``connect``. Each step is something the user chose, in
        descending order of how recently they chose it.
        """
        wanted = (name or os.getenv(PROFILE_ENV, "").strip() or self.default or "")
        if not wanted:
            raise ConfigError(
                "no node connected — run: mftik connect <url>"
            )
        profile = self.profiles.get(wanted)
        if profile is None:
            known = ", ".join(sorted(self.profiles)) or "(none)"
            raise ConfigError(f"unknown profile {wanted!r}; known: {known}")
        return profile


def load() -> Config:
    """Read the config, treating absent as empty rather than as an error.

    A corrupt file is an error, though. It is small, hand-editable and holds
    the only copy of a credential that is shown once — silently starting over
    with an empty one would lose it without saying so.
    """
    path = config_path()
    if not path.is_file():
        return Config(profiles={})
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise ConfigError(f"cannot read {path}: {exc}") from exc

    profiles: dict[str, Profile] = {}
    table = raw.get("profiles")
    if isinstance(table, dict):
        for name, value in table.items():
            if not isinstance(name, str) or not isinstance(value, dict):
                continue
            url = value.get("url")
            if not isinstance(url, str) or not url:
                continue
            token = value.get("token")
            profiles[name] = Profile(
                name=name,
                url=url,
                token=token if isinstance(token, str) and token else None,
            )
    default = raw.get("default")
    if not isinstance(default, str) or default not in profiles:
        default = next(iter(profiles), None)
    return Config(profiles=profiles, default=default)


def save(config: Config) -> Path:
    """Write every profile back, at 0600. Returns where it went."""
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    body = _dump(config)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(body)
    # An existing file keeps its old mode through O_CREAT, so say it again.
    os.chmod(path, 0o600)
    return path


def put(profile: Profile, *, make_default: bool = True) -> Path:
    """Add or replace one profile."""
    config = load()
    profiles = dict(config.profiles)
    profiles[profile.name] = profile
    default = profile.name if make_default else (config.default or profile.name)
    return save(Config(profiles=profiles, default=default))


def drop(name: str) -> Profile:
    """Forget a node. Returns what was forgotten."""
    config = load()
    profile = config.profiles.get(name)
    if profile is None:
        known = ", ".join(sorted(config.profiles)) or "(none)"
        raise ConfigError(f"unknown profile {name!r}; known: {known}")
    profiles = {k: v for k, v in config.profiles.items() if k != name}
    default = config.default if config.default != name else next(iter(profiles), None)
    save(Config(profiles=profiles, default=default))
    return profile


def retoken(profile: Profile, token: str | None) -> Profile:
    return replace(profile, token=token)


def default_name(url: str) -> str:
    """A profile name derived from the URL, for a connect that gave none.

    The host, with the dots that would read as a hierarchy flattened. Good
    enough to tell two nodes apart in ``mftik profiles``, and short enough to
    type — a user who wants something else passes ``--name``.
    """
    from urllib.parse import urlparse

    host = (urlparse(url).hostname or "").strip()
    if not host or host in {"localhost", "127.0.0.1", "::1"}:
        return _DEFAULT_NAME
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in host).strip("_")
    return cleaned.lower() or _DEFAULT_NAME


def _dump(config: Config) -> str:
    lines: list[str] = []
    if config.default:
        lines.append(f"default = {json.dumps(config.default)}")
        lines.append("")
    for name in sorted(config.profiles):
        profile = config.profiles[name]
        lines.append(f"[profiles.{name}]")
        lines.append(f"url = {json.dumps(profile.url)}")
        if profile.token:
            lines.append(f"token = {json.dumps(profile.token)}")
        lines.append("")
    return "\n".join(lines)
