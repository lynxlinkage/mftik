"""``mftik artifact`` — put, list, and remove one STS's uploaded objects."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from mftik.cli import client as client_module
from mftik.cli import config
from mftik.cli.app import EXIT_ERROR, main
from mftik.cli.client import Client
from mftik.cli.config import Profile

_REAL_HTTPX = httpx.Client


@pytest.fixture(autouse=True)
def config_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "config.toml"
    monkeypatch.setenv(config.CONFIG_ENV, str(path))
    monkeypatch.delenv(config.PROFILE_ENV, raising=False)
    config.put(Profile(name="local", url="http://node.test", token="mftik_ak_t"))
    return path


class Node_:
    def __init__(self, instances: list[str]) -> None:
        self.instances = instances
        self.paths: list[str] = []
        self.methods: list[str] = []
        self.params: list[dict[str, str]] = []
        self.bodies: list[bytes] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        self.methods.append(request.method)
        self.params.append(dict(request.url.params))
        if request.content:
            self.bodies.append(request.content)
        if request.url.path == "/instances":
            return httpx.Response(
                200,
                json={
                    "instances": [
                        {"name": name, "domain": "sts"} for name in self.instances
                    ]
                },
            )
        if request.url.path == "/sts/artifacts" and request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "objects": [
                        {
                            "path": "weights/model.pt",
                            "size": 10,
                            "mtime": 1_700_000_000.0,
                            "digest": "sha256:abc",
                            "instance": "sts-jp",
                        }
                    ],
                    "unanswered": [],
                },
            )
        if request.url.path == "/sts/artifacts" and request.method == "PUT":
            return httpx.Response(
                200,
                json={
                    "path": request.url.params["path"],
                    "size": len(request.content),
                    "mtime": 1.0,
                    "digest": "sha256:abc",
                    "instance": request.url.params.get("instance"),
                },
            )
        if request.url.path == "/sts/artifacts" and request.method == "DELETE":
            return httpx.Response(
                200,
                json={
                    "path": request.url.params["path"],
                    "size": 0,
                    "mtime": 0,
                    "digest": "",
                    "instance": request.url.params.get("instance"),
                },
            )
        return httpx.Response(404, json={"detail": "nope"})


def _install(monkeypatch: pytest.MonkeyPatch, fake: Node_) -> None:
    def build(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs.pop("transport", None)
        return _REAL_HTTPX(*args, transport=httpx.MockTransport(fake), **kwargs)

    monkeypatch.setattr(httpx, "Client", build)
    monkeypatch.setattr(client_module, "Client", Client)


def test_ls_uses_the_only_declared_sts(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    node = Node_(["sts-jp"])
    _install(monkeypatch, node)
    assert main(["artifact", "ls"]) == 0
    out = capsys.readouterr().out
    assert "weights/model.pt" in out
    assert "sha256:abc" in out
    assert node.params[-1]["instance"] == "sts-jp"


def test_two_sts_processes_require_an_instance(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    node = Node_(["sts-jp", "sts-tw"])
    _install(monkeypatch, node)
    assert main(["artifact", "ls"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "--instance" in err
    assert "sts-jp" in err and "sts-tw" in err
    assert "/sts/artifacts" not in node.paths


def test_put_sends_the_file_as_one_body(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys
) -> None:
    local = tmp_path / "model.pt"
    local.write_bytes(b"abcdefghij")
    node = Node_(["sts-jp"])
    _install(monkeypatch, node)
    assert main(
        [
            "artifact",
            "put",
            str(local),
            "weights/model.pt",
            "--instance",
            "sts-jp",
        ]
    ) == 0
    assert node.methods[-1] == "PUT"
    assert node.params[-1] == {"instance": "sts-jp", "path": "weights/model.pt"}
    assert node.bodies[-1] == b"abcdefghij"
    assert "sha256:abc" in capsys.readouterr().out


def test_put_and_rm_refuse_a_session_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys
) -> None:
    local = tmp_path / "model.pt"
    local.write_bytes(b"x")
    node = Node_(["sts-jp"])
    _install(monkeypatch, node)
    assert (
        main(["artifact", "put", str(local), "sessions/abc/weights/model.pt"])
        == EXIT_ERROR
    )
    assert main(["artifact", "rm", "sessions/abc/weights/model.pt"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert err.count("cannot put or remove") == 2
    assert node.paths == []


def test_rm_names_the_key(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    node = Node_(["sts-jp"])
    _install(monkeypatch, node)
    assert main(["artifact", "rm", "weights/model.pt", "--instance", "sts-jp"]) == 0
    assert node.methods[-1] == "DELETE"
    assert "removed weights/model.pt on sts-jp" in capsys.readouterr().out


def test_put_of_a_missing_file_does_not_call_the_node(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    node = Node_(["sts-jp"])
    _install(monkeypatch, node)
    assert main(["artifact", "put", "missing.pt", "weights/model.pt"]) == EXIT_ERROR
    assert "not a file" in capsys.readouterr().err
    assert node.paths == []
