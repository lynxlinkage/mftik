"""``mftik push`` — what it sends, and the three ``loaded`` outcomes."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from mftik.cli import client as client_module
from mftik.cli import config
from mftik.cli.app import EXIT_ERROR, main
from mftik.cli.client import Client
from mftik.cli.config import Profile

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""

_REAL_HTTPX = httpx.Client


@pytest.fixture(autouse=True)
def config_file(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "config.toml"
    monkeypatch.setenv(config.CONFIG_ENV, str(path))
    monkeypatch.delenv(config.PROFILE_ENV, raising=False)
    config.put(Profile(name="local", url="http://node.test", token="mftik_ak_t"))
    return path


class Node_:
    def __init__(self, *, loaded: bool = True, load_error: str | None = None) -> None:
        self.loaded = loaded
        self.load_error = load_error
        self.paths: list[str] = []
        self.bodies: list[object] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        if request.content:
            self.bodies.append(json.loads(request.content))
        if request.url.path == "/registry/v1/add":
            return httpx.Response(
                200,
                json={
                    "name": "tiny",
                    "type": "Tiny",
                    "digest": "sha256:abc",
                    "requires_mftik": "0.1.0",
                    "origin": "private",
                    "files": ["strategy.py"],
                    "loaded": self.loaded,
                    "load_error": self.load_error,
                },
            )
        return httpx.Response(404, json={"detail": "nope"})


def _install(monkeypatch: pytest.MonkeyPatch, fake: Node_) -> None:
    def build(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs.pop("transport", None)
        return _REAL_HTTPX(*args, transport=httpx.MockTransport(fake), **kwargs)

    monkeypatch.setattr(httpx, "Client", build)
    # ``connected`` builds a Client after this patch, so its httpx is the fake.
    monkeypatch.setattr(client_module, "Client", Client)


def _tree(tmp_path: Path, source: str = _TINY) -> Path:
    dest = tmp_path / "hello"
    dest.mkdir()
    (dest / "strategy.py").write_text(source)
    return dest


def test_push_sends_private_replace_and_the_files(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    dest = _tree(tmp_path)

    assert main(["push", str(dest)]) == 0
    out = capsys.readouterr().out
    assert "pushed tiny type=Tiny origin=private" in out
    assert "sha256:abc" in out

    assert fake.paths == ["/registry/v1/add"]
    body = fake.bodies[0]
    assert isinstance(body, dict)
    assert body["origin"] == "private"
    assert body["replace"] is True
    assert body["files"]["strategy.py"] == _TINY


def test_push_exits_one_when_sts_did_not_reload(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    fake = Node_(
        loaded=False,
        load_error="the strategy was stored, but STS did not reload (timeout).",
    )
    _install(monkeypatch, fake)

    assert main(["push", str(_tree(tmp_path))]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "did not reload" in err
    assert "Traceback" not in err


def test_push_exits_one_when_sts_skipped_the_tree(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    fake = Node_(
        loaded=False,
        load_error="STS did not load it as 'private::Tiny'",
    )
    _install(monkeypatch, fake)

    assert main(["push", str(_tree(tmp_path))]) == EXIT_ERROR
    assert "private::Tiny" in capsys.readouterr().err


def test_push_refuses_a_file(tmp_path: Path, capsys) -> None:
    path = tmp_path / "strategy.py"
    path.write_text(_TINY)
    assert main(["push", str(path)]) == EXIT_ERROR
    assert "directory" in capsys.readouterr().err


def test_push_refuses_a_missing_directory(tmp_path: Path, capsys) -> None:
    assert main(["push", str(tmp_path / "gone")]) == EXIT_ERROR
    assert "does not exist" in capsys.readouterr().err
