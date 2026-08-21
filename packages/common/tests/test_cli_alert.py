"""``mftik alert`` — the Alert graph from a terminal.

The assertions that matter most here are the two the surface exists to make
true: the webhook URL is never an argument and never printed back, and a
Source cannot be wired straight to an Alert.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import httpx
import pytest
from mftik.cli import client as client_module
from mftik.cli import config
from mftik.cli.app import EXIT_ERROR, main
from mftik.cli.client import Client
from mftik.cli.config import Profile

_REAL_HTTPX = httpx.Client
HOOK = "https://discord.com/api/webhooks/1/super-secret-token"
MASK = "https://discord.com/api/webhooks/…/***"


@pytest.fixture(autouse=True)
def config_file(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "config.toml"
    monkeypatch.setenv(config.CONFIG_ENV, str(path))
    monkeypatch.delenv(config.PROFILE_ENV, raising=False)
    config.put(Profile(name="local", url="http://node.test", token="mftik_ak_t"))
    return path


def _alert(**over: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": 1,
        "created_by": 1,
        "name": "ops",
        "kind": "discord_webhook",
        "webhook_masked": MASK,
        "enabled": True,
        "flush_interval_s": 30,
        "max_events_in_payload": 15,
        "max_buffer_events": 200,
        "dedupe": True,
        "matcher_ids": [100],
    }
    row.update(over)
    return row


def _matcher(**over: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": 100,
        "created_by": 1,
        "name": "warn-or-error",
        "kind": "level",
        "spec": {"levels": ["warn", "error"]},
        "source_ids": [7],
        "alert_ids": [1],
        "disabled_reason": None,
    }
    row.update(over)
    return row


def _source(**over: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": 7,
        "created_by": 1,
        "domain": "sts",
        "selector": "private::Tiny",
        "matcher_ids": [100],
    }
    row.update(over)
    return row


class Node_:
    """A node that records what was asked of it and answers plausibly."""

    def __init__(self, **over: object) -> None:
        self.alerts = [_alert()]
        self.matchers = [_matcher()]
        self.sources = [_source()]
        self.delivery_error: str | None = None
        self.calls: list[tuple[str, str]] = []
        self.bodies: list[object] = []
        for key, value in over.items():
            setattr(self, key, value)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append((request.method, path))
        if request.content:
            self.bodies.append(json.loads(request.content))

        if path == "/alerts" and request.method == "GET":
            return httpx.Response(200, json={"alerts": self.alerts})
        if path == "/alerts" and request.method == "POST":
            return httpx.Response(201, json=_alert(id=2, name="new"))
        if path == "/alerts/sources" and request.method == "GET":
            return httpx.Response(200, json={"sources": self.sources})
        if path == "/alerts/sources" and request.method == "POST":
            return httpx.Response(201, json=_source(id=8))
        if path == "/alerts/matchers" and request.method == "GET":
            return httpx.Response(200, json={"matchers": self.matchers})
        if path == "/alerts/matchers" and request.method == "POST":
            return httpx.Response(201, json=_matcher(id=101))
        if path == "/sts/types":
            return httpx.Response(200, json={"types": ["NoopStrategy", "CrossArb"]})
        if path.endswith("/test"):
            return httpx.Response(
                200,
                json={
                    "delivery": {
                        "id": 1,
                        "alert_id": 1,
                        "window_start": 1_780_000_000.0,
                        "event_count": 0,
                        "dropped_count": 0,
                        "http_status": None if self.delivery_error else 204,
                        "error": self.delivery_error,
                        "ts": 1_780_000_000.0,
                    }
                },
            )
        if path.endswith("/deliveries"):
            return httpx.Response(200, json={"deliveries": []})
        if request.method in {"PUT", "DELETE"}:
            return httpx.Response(200, json={"wired": True, "matcher_id": 100})
        return httpx.Response(404, json={"detail": "nope"})


def _install(monkeypatch: pytest.MonkeyPatch, fake: Node_) -> None:
    def build(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs.pop("transport", None)
        return _REAL_HTTPX(*args, transport=httpx.MockTransport(fake), **kwargs)

    monkeypatch.setattr(httpx, "Client", build)
    monkeypatch.setattr(client_module, "Client", Client)


def test_list_shows_the_mask_and_never_a_url(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_())
    assert main(["alert", "list"]) == 0
    out = capsys.readouterr().out
    assert "ops" in out and MASK in out
    assert "super-secret-token" not in out


def test_add_refuses_a_url_it_was_not_given_a_way_to_read(monkeypatch, capsys) -> None:
    """No TTY and no flag is not an empty string — it is a question."""
    _install(monkeypatch, Node_())
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    assert main(["alert", "add", "--name", "ops"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "--webhook-url-stdin" in err


def test_add_reads_the_url_from_stdin_and_does_not_echo_it(
    monkeypatch, capsys
) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    monkeypatch.setattr(sys, "stdin", io.StringIO(f"{HOOK}\n"))
    assert main(["alert", "add", "--name", "ops", "--webhook-url-stdin"]) == 0
    captured = capsys.readouterr()
    assert fake.bodies[0]["webhook_url"] == HOOK
    assert "super-secret-token" not in captured.out
    assert "super-secret-token" not in captured.err
    # An Alert with nothing wired to it is silent, and saying so here is
    # cheaper than the Owner discovering it during an incident.
    assert "wire" in captured.out


def test_add_says_so_when_the_pipe_was_empty(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_())
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    assert main(["alert", "add", "--name", "ops", "--webhook-url-stdin"]) == EXIT_ERROR
    assert "stdin was empty" in capsys.readouterr().err


def test_the_url_is_not_an_argument(monkeypatch) -> None:
    """There is no ``--webhook-url``. Shell history is not a secret store."""
    _install(monkeypatch, Node_())
    with pytest.raises(SystemExit):
        main(["alert", "add", "--name", "ops", "--webhook-url", HOOK])


def test_wire_source_to_matcher(monkeypatch, capsys) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    assert main(["alert", "wire", "--source", "7", "--matcher", "100"]) == 0
    assert ("PUT", "/alerts/sources/7/matchers/100") in fake.calls


def test_wire_matcher_to_alert(monkeypatch, capsys) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    assert main(["alert", "wire", "--matcher", "100", "--alert", "1"]) == 0
    assert ("PUT", "/alerts/matchers/100/alerts/1") in fake.calls


def test_a_source_cannot_be_wired_to_an_alert(monkeypatch, capsys) -> None:
    """Invariant 4 has no row shape for it, so the CLI does not send one."""
    fake = Node_()
    _install(monkeypatch, fake)
    assert main(["alert", "wire", "--source", "7", "--alert", "1"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "Source → Matcher → Alert" in err
    assert not fake.calls, "nothing is asked of the node"


def test_wire_needs_two_ends(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_())
    assert main(["alert", "wire", "--matcher", "100"]) == EXIT_ERROR
    assert "name one edge" in capsys.readouterr().err


def test_unwire_uses_the_same_edge_grammar(monkeypatch) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    assert main(["alert", "unwire", "--source", "7", "--matcher", "100"]) == 0
    assert ("DELETE", "/alerts/sources/7/matchers/100") in fake.calls


def test_matcher_add_coerces_the_value_before_the_node_sees_it(
    monkeypatch, capsys
) -> None:
    """``as: float`` with a string is stored happily and dies at match time."""
    fake = Node_()
    _install(monkeypatch, fake)
    code = main(
        [
            "alert", "matcher", "add", "--name", "risk", "--kind", "extract",
            "--pattern", r"risk value = ([\d.]+)", "--as", "float",
            "--op", ">", "--value", "0.99",
        ]
    )
    assert code == 0
    assert fake.bodies[0]["spec"]["value"] == 0.99


def test_matcher_add_refuses_a_value_that_is_not_the_declared_type(
    monkeypatch, capsys
) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    code = main(
        [
            "alert", "matcher", "add", "--name", "risk", "--kind", "extract",
            "--pattern", "x", "--as", "float", "--value", "high",
        ]
    )
    assert code == EXIT_ERROR
    assert "--as str" in capsys.readouterr().err
    assert not fake.bodies, "nothing was stored"


def test_level_matcher_needs_a_level(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_())
    code = main(["alert", "matcher", "add", "--name", "x", "--kind", "level"])
    assert code == EXIT_ERROR
    assert "--level" in capsys.readouterr().err


def test_source_add_warns_about_a_session_id(monkeypatch, capsys) -> None:
    """A hex id is a legal selector that never matches. Only a person can tell."""
    fake = Node_()
    _install(monkeypatch, fake)
    session_id = "0123456789abcdef0123456789abcdef"
    code = main(
        ["alert", "source", "add", "--domain", "sts", "--selector", session_id]
    )
    assert code == 0
    err = capsys.readouterr().err
    assert "session_id" in err and "never match" in err
    assert fake.bodies[0]["selector"] == session_id, "stored anyway; the API decides"


def test_source_add_does_not_warn_about_a_type(monkeypatch, capsys) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    code = main(
        ["alert", "source", "add", "--domain", "sts", "--selector", "private::Tiny"]
    )
    assert code == 0
    assert capsys.readouterr().err == ""


def test_types_lists_what_a_selector_may_be(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_())
    assert main(["alert", "types"]) == 0
    out = capsys.readouterr().out
    assert "NoopStrategy" in out and "CrossArb" in out


def test_graph_renders_the_whole_wiring(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_())
    assert main(["alert", "graph"]) == 0
    out = capsys.readouterr().out
    assert "sts:private::Tiny" in out
    assert "warn-or-error" in out
    assert "ops" in out


def test_graph_names_a_matcher_that_fires_nothing(monkeypatch, capsys) -> None:
    fake = Node_(matchers=[_matcher(alert_ids=[])])
    _install(monkeypatch, fake)
    assert main(["alert", "graph"]) == 0
    assert "fires nothing" in capsys.readouterr().out


def test_test_fire_reports_a_refusal_as_a_failure(monkeypatch, capsys) -> None:
    """Exit non-zero, or a CI job that "verified the webhook" verified nothing."""
    fake = Node_(delivery_error="ConnectTimeout")
    _install(monkeypatch, fake)
    assert main(["alert", "test", "1"]) == 1
    assert "ConnectTimeout" in capsys.readouterr().out


def test_test_fire_reports_success(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_())
    assert main(["alert", "test", "1"]) == 0
    assert "204" in capsys.readouterr().out


def test_matcher_list_surfaces_a_disabled_matcher(monkeypatch, capsys) -> None:
    """A Matcher that stopped judging must not look like one that judges."""
    fake = Node_(matchers=[_matcher(disabled_reason="timed out 5 times")])
    _install(monkeypatch, fake)
    assert main(["alert", "matcher", "list"]) == 0
    assert "timed out 5 times" in capsys.readouterr().err


def test_bare_alert_prints_its_verbs(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_())
    assert main(["alert"]) == EXIT_ERROR
    assert "usage: mftik alert" in capsys.readouterr().out


@pytest.mark.parametrize("group", ["source", "matcher"])
def test_a_group_named_without_a_verb_answers_for_itself(
    monkeypatch, capsys, group: str
) -> None:
    """Not the outer command's verb list — that is the wrong noun's help."""
    _install(monkeypatch, Node_())
    assert main(["alert", group]) == EXIT_ERROR
    out = capsys.readouterr().out
    assert out.startswith(f"usage: mftik alert {group} ")
    assert "graph" not in out
