"""``mftik rm`` — what it sends, and the three ``unloaded`` outcomes."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from mftik.cli import client as client_module
from mftik.cli import config
from mftik.cli.app import EXIT_ERROR, main
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
    def __init__(
        self,
        *,
        unloaded: bool = True,
        unload_error: str | None = None,
        status: int = 200,
        detail: str | None = None,
    ) -> None:
        self.unloaded = unloaded
        self.unload_error = unload_error
        self.status = status
        self.detail = detail
        self.paths: list[str] = []
        self.queries: list[dict[str, list[str]]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        parsed = urlparse(str(request.url))
        self.paths.append(parsed.path)
        self.queries.append(parse_qs(parsed.query))
        if request.method == "DELETE" and parsed.path.startswith(
            "/registry/v1/strategies/"
        ):
            if self.status != 200:
                return httpx.Response(self.status, json={"detail": self.detail})
            return httpx.Response(
                200,
                json={
                    "name": "tiny",
                    "type": "Tiny",
                    "digest": "sha256:abc",
                    "requires_mftik": "0.1.0",
                    "origin": parse_qs(parsed.query).get("origin", ["private"])[0],
                    "files": ["strategy.py"],
                    "unloaded": self.unloaded,
                    "unload_error": self.unload_error,
                },
            )
        return httpx.Response(404, json={"detail": "nope"})


def _install(monkeypatch: pytest.MonkeyPatch, fake: Node_) -> None:
    def build(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs.pop("transport", None)
        return _REAL_HTTPX(*args, transport=httpx.MockTransport(fake), **kwargs)

    monkeypatch.setattr(httpx, "Client", build)
    monkeypatch.setattr(client_module, "Client", Client)


def test_rm_sends_private_by_default(monkeypatch, capsys) -> None:
    fake = Node_()
    _install(monkeypatch, fake)

    assert main(["rm", "tiny"]) == 0
    out = capsys.readouterr().out
    assert "removed tiny type=Tiny origin=private" in out
    assert "sha256:abc" in out

    assert fake.paths == ["/registry/v1/strategies/tiny"]
    assert fake.queries == [{"origin": ["private"]}]


def test_rm_sends_the_origin_it_was_given(monkeypatch, capsys) -> None:
    fake = Node_()
    _install(monkeypatch, fake)

    assert main(["rm", "tiny", "--origin", "public"]) == 0
    assert "origin=public" in capsys.readouterr().out
    assert fake.queries == [{"origin": ["public"]}]


def test_rm_exits_one_when_sts_did_not_reload(monkeypatch, capsys) -> None:
    fake = Node_(
        unloaded=False,
        unload_error="the strategy was deleted, but STS did not reload (timeout).",
    )
    _install(monkeypatch, fake)

    assert main(["rm", "tiny"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "did not reload" in err
    assert "Traceback" not in err


def test_rm_exits_one_when_sts_still_answers(monkeypatch, capsys) -> None:
    fake = Node_(
        unloaded=False,
        unload_error=(
            "the strategy was deleted, but STS still answers to 'private::Tiny'."
        ),
    )
    _install(monkeypatch, fake)

    assert main(["rm", "tiny"]) == EXIT_ERROR
    assert "private::Tiny" in capsys.readouterr().err


def test_rm_exits_one_when_a_session_is_still_running_it(
    monkeypatch, capsys
) -> None:
    fake = Node_(
        status=409,
        detail=(
            "cannot delete private::Tiny: live sessions are running it. "
            "Stop these first: sts-7"
        ),
    )
    _install(monkeypatch, fake)

    assert main(["rm", "tiny"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "sts-7" in err
    assert "Traceback" not in err


def test_rm_exits_one_when_the_tree_is_missing(monkeypatch, capsys) -> None:
    fake = Node_(status=404, detail="no private strategy named 'tiny'")
    _install(monkeypatch, fake)

    assert main(["rm", "tiny"]) == EXIT_ERROR
    assert "tiny" in capsys.readouterr().err
