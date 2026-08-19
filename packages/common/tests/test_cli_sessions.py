"""``mftik ps`` / ``logs`` / ``stop`` — the paths they hit."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from mftik.cli import client as client_module
from mftik.cli import config
from mftik.cli.app import main
from mftik.cli.client import Client
from mftik.cli.config import Profile

_REAL_HTTPX = httpx.Client


@pytest.fixture(autouse=True)
def config_file(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "config.toml"
    monkeypatch.setenv(config.CONFIG_ENV, str(path))
    monkeypatch.delenv(config.PROFILE_ENV, raising=False)
    config.put(Profile(name="local", url="http://node.test", token="mftik_ak_t"))
    return path


class Node_:
    def __init__(self) -> None:
        self.paths: list[str] = []
        self.sessions: list[dict] = [
            {
                "session_id": "sess-1",
                "strategy": "private::Tiny",
                "status": "live",
            }
        ]
        self.logs: list[dict] = [
            {"level": "info", "message": "newer"},
            {"level": "info", "message": "older"},
        ]

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        if request.url.path == "/sts/sessions" and request.method == "GET":
            return httpx.Response(200, json={"sessions": self.sessions})
        if request.url.path == "/sts/sessions/sess-1/stop":
            return httpx.Response(
                200,
                json={
                    "session_id": "sess-1",
                    "status": "done",
                },
            )
        if request.url.path == "/logs/sts/sess-1":
            return httpx.Response(200, json={"logs": self.logs, "has_more": False})
        return httpx.Response(404, json={"detail": "nope"})


def _install(monkeypatch: pytest.MonkeyPatch, fake: Node_) -> list[str]:
    followed: list[str] = []

    def build(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs.pop("transport", None)
        return _REAL_HTTPX(*args, transport=httpx.MockTransport(fake), **kwargs)

    def follow(self, session_id: str, write=print) -> None:  # noqa: ANN001
        del self, write
        followed.append(session_id)

    monkeypatch.setattr(httpx, "Client", build)
    monkeypatch.setattr(client_module, "Client", Client)
    monkeypatch.setattr(Client, "follow_sts_logs", follow)
    return followed


def test_ps_lists_live_sessions(monkeypatch, capsys) -> None:
    fake = Node_()
    _install(monkeypatch, fake)

    assert main(["ps"]) == 0
    out = capsys.readouterr().out
    assert "sess-1" in out
    assert "private::Tiny" in out
    assert "live" in out
    assert fake.paths == ["/sts/sessions"]


def test_ps_with_nothing_live_is_not_an_error(monkeypatch, capsys) -> None:
    fake = Node_()
    fake.sessions = []
    _install(monkeypatch, fake)

    assert main(["ps"]) == 0
    assert "no live sessions" in capsys.readouterr().out


def test_stop_posts_the_session(monkeypatch, capsys) -> None:
    fake = Node_()
    _install(monkeypatch, fake)

    assert main(["stop", "sess-1"]) == 0
    assert fake.paths == ["/sts/sessions/sess-1/stop"]
    assert "stopped sess-1 status=done" in capsys.readouterr().out


def test_logs_prints_the_stored_page_oldest_first(monkeypatch, capsys) -> None:
    fake = Node_()
    followed = _install(monkeypatch, fake)

    assert main(["logs", "sess-1"]) == 0
    assert fake.paths == ["/logs/sts/sess-1"]
    assert followed == []
    lines = capsys.readouterr().out.splitlines()
    assert lines == ["info  older", "info  newer"]


def test_logs_follow_uses_the_same_helper_as_run(monkeypatch, capsys) -> None:
    fake = Node_()
    followed = _install(monkeypatch, fake)

    assert main(["logs", "-f", "sess-1"]) == 0
    assert followed == ["sess-1"]
    assert "/logs/sts/sess-1" not in fake.paths
    del capsys
