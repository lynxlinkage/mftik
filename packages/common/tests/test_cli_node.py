"""``mftik node init`` — a stack that a laptop can actually run.

The maintainers' own deploy/docker-compose.yml expects Postgres, Redis and a
Traefik somebody else runs. None of that is true on a laptop, so what this
writes has to bring its own — including the edge, because the browser asks
for ``/api`` and opens ``ws://<this host>/ws`` on whatever host served the
page and something has to put those on one origin.
"""

from __future__ import annotations

import stat
from pathlib import Path

import pytest
import yaml
from mftik.cli.app import EXIT_ERROR, main


def _written(root: Path) -> dict[str, str]:
    return {
        p.name: p.read_text()
        for p in root.iterdir()
        if p.name in {"docker-compose.yml", "Caddyfile", ".env"}
    }


def _compose(root: Path) -> dict:
    return yaml.safe_load((root / "docker-compose.yml").read_text())


def test_it_writes_a_stack(tmp_path: Path, capsys) -> None:
    root = tmp_path / "mynode"

    assert main(["node-init", str(root)]) == 0

    assert set(_written(root)) == {"docker-compose.yml", "Caddyfile", ".env"}
    out = capsys.readouterr().out
    assert "docker compose up -d" in out
    assert "mftik connect http://localhost:8080" in out


def test_the_compose_file_parses(tmp_path: Path) -> None:
    root = tmp_path / "mynode"
    main(["node-init", str(root)])

    compose = _compose(root)

    assert set(compose["services"]) == {
        "postgres",
        "redis",
        "migrate",
        "seed",
        "api",
        "frontend",
        "caddy",
        "td",
        "md",
        "sts",
        "paper",
        "sym",
    }


def test_nothing_is_built_from_source(tmp_path: Path) -> None:
    """The point is hosting a node without cloning the repository."""
    root = tmp_path / "mynode"
    main(["node-init", str(root)])

    for name, service in _compose(root)["services"].items():
        assert "build" not in service, name


def test_the_database_and_broker_come_with_it(tmp_path: Path) -> None:
    """Unlike the maintainers' deploy, which expects them as substrate."""
    root = tmp_path / "mynode"
    main(["node-init", str(root)])
    compose = _compose(root)

    assert compose["services"]["postgres"]["image"].startswith("postgres:")
    assert compose["services"]["redis"]["image"].startswith("redis:")
    body = _written(root)[".env"]
    assert "@postgres:5432/mftik" in body
    assert "redis://redis:6379" in body


def test_one_port_serves_the_ui_the_api_and_the_sockets(tmp_path: Path) -> None:
    """The browser opens all three on whatever host served the document."""
    root = tmp_path / "mynode"
    main(["node-init", str(root)])

    services = _compose(root)["services"]
    assert services["caddy"]["ports"] == ["${MFTIK_PORT:-8080}:80"]
    for name, service in services.items():
        if name != "caddy":
            assert "ports" not in service, name

    routes = _written(root)["Caddyfile"]
    # handle_path strips the prefix, handle does not — the API's routers have
    # no /api prefix and the WebSocket paths are literal.
    assert "handle_path /api/*" in routes
    assert "handle /ws/*" in routes
    assert "reverse_proxy frontend:3000" in routes


def test_the_registry_volume_is_shared_by_api_and_sts(tmp_path: Path) -> None:
    """A push the process that runs strategies cannot see is not a push."""
    root = tmp_path / "mynode"
    main(["node-init", str(root)])
    services = _compose(root)["services"]

    for name in ("api", "sts"):
        assert "mftik_data:/var/lib/mftik" in services[name]["volumes"], name
        assert services[name]["environment"]["MFTIK_DATA"] == "/var/lib/mftik"


def test_migrations_and_seed_are_not_in_the_running_set(tmp_path: Path) -> None:
    """`up -d` must not race a schema migration."""
    root = tmp_path / "mynode"
    main(["node-init", str(root)])
    services = _compose(root)["services"]

    assert services["migrate"]["profiles"] == ["tools"]
    assert services["seed"]["profiles"] == ["tools"]


# --- the .env --------------------------------------------------------------


def test_the_env_file_is_not_readable_by_anyone_else(tmp_path: Path) -> None:
    """It holds the database password."""
    root = tmp_path / "mynode"
    main(["node-init", str(root)])

    assert stat.S_IMODE((root / ".env").stat().st_mode) == 0o600


def test_the_password_is_generated_and_not_a_default(tmp_path: Path) -> None:
    first = tmp_path / "a"
    second = tmp_path / "b"
    main(["node-init", str(first)])
    main(["node-init", str(second)])

    def password(root: Path) -> str:
        line = next(
            ln for ln in (root / ".env").read_text().splitlines()
            if ln.startswith("POSTGRES_PASSWORD=")
        )
        return line.split("=", 1)[1]

    assert password(first) != password(second)
    assert len(password(first)) >= 20


def test_the_tag_and_port_can_be_chosen(tmp_path: Path, capsys) -> None:
    root = tmp_path / "mynode"

    assert main(["node-init", str(root), "--tag", "v1.2.3", "--port", "9000"]) == 0

    body = _written(root)[".env"]
    assert "MFTIK_VERSION=v1.2.3" in body
    assert "MFTIK_PORT=9000" in body
    out = capsys.readouterr().out
    assert "http://localhost:9000" in out
    # Only :latest earns the warning.
    assert "moves under you" not in out


def test_latest_says_it_will_move(tmp_path: Path, capsys) -> None:
    main(["node-init", str(tmp_path / "mynode")])

    assert "moves under you" in capsys.readouterr().out


def test_the_project_name_comes_from_the_directory(tmp_path: Path) -> None:
    """So two nodes on one machine do not collide on container names."""
    root = tmp_path / "My Node"
    main(["node-init", str(root)])

    assert "MFTIK_PROJECT=my-node" in _written(root)[".env"]


def test_existing_files_are_not_overwritten(tmp_path: Path, capsys) -> None:
    root = tmp_path / "mynode"
    root.mkdir()
    (root / ".env").write_text("POSTGRES_PASSWORD=mine\n")

    assert main(["node-init", str(root)]) == EXIT_ERROR

    assert (root / ".env").read_text() == "POSTGRES_PASSWORD=mine\n"
    assert "--force" in capsys.readouterr().err


def test_force_overwrites(tmp_path: Path) -> None:
    root = tmp_path / "mynode"
    root.mkdir()
    (root / ".env").write_text("POSTGRES_PASSWORD=mine\n")

    assert main(["node-init", str(root), "--force"]) == 0

    assert "POSTGRES_PASSWORD=mine" not in (root / ".env").read_text()


def test_the_templates_ship_in_the_package(tmp_path: Path) -> None:
    """Data, not code — a wheel that dropped them ships a command that cannot run.

    Reading them through the installed package rather than the source tree is
    the point: this is what fails if the build stops including them.
    """
    from importlib.resources import files

    for name in ("docker-compose.yml", "Caddyfile", "env"):
        assert (files("mftik.cli") / "templates" / name).is_file(), name


@pytest.mark.parametrize("path", [".", ""])
def test_the_current_directory_needs_no_cd(path: str, tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    assert main(["node-init", path] if path else ["node-init"]) == 0

    assert "cd ." not in capsys.readouterr().out
