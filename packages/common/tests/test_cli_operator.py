"""IF-15's CLI surface, as it behaves today.

The new commands parse and then refuse, except ``mftik workers`` without
``--stale``, which lists each worker's release (B3-07), ``mftik td drain``,
which posts one account (B6-04), and ``mftik run``, whose ``--wait`` is the
default (B4-08). ``mftik --help`` lists the new commands because they are
rows in the same table the dispatch reads.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from mftik.cli import client as client_module
from mftik.cli import config
from mftik.cli.app import EXIT_ERROR, build_parser, main
from mftik.cli.client import DEFAULT_TIMEOUT_S, Client
from mftik.cli.config import Profile
from mftik.cli.operator import (
    IntentGc,
    MdRestart,
    TdDrain,
    intent_gc,
    md_restart,
    select_workers,
    td_drain,
)
from mftik.cli.run import _DEPLOY_HTTP_TIMEOUT_S
from mftik.protocol import ProcmanWorker

# `main()` builds the whole CLI parser; that call does not fit 50 ms.
pytestmark = pytest.mark.component


def test_help_lists_the_new_commands(capsys) -> None:
    """Acceptance: ``mftik --help`` names every command this ticket adds."""
    with pytest.raises(SystemExit) as caught:
        main(["--help"])
    assert caught.value.code == 0
    out = capsys.readouterr().out
    for name in ("workers", "md", "td", "intents"):
        assert f"  {name} " in out


def test_run_help_lists_wait_and_no_wait(capsys) -> None:
    with pytest.raises(SystemExit) as caught:
        main(["run", "--help"])
    assert caught.value.code == 0
    out = capsys.readouterr().out
    assert "--wait" in out
    assert "--no-wait" in out


def test_nested_help_lists_the_verb(capsys) -> None:
    """The verb is part of the command the ticket names, so it is on --help."""
    expected = {
        "md": "restart",
        "td": "drain",
        "intents": "gc",
    }
    for command, verb in expected.items():
        with pytest.raises(SystemExit) as caught:
            main([command, "--help"])
        assert caught.value.code == 0
        assert verb in capsys.readouterr().out


def test_workers_help_lists_stale(capsys) -> None:
    with pytest.raises(SystemExit) as caught:
        main(["workers", "--help"])
    assert caught.value.code == 0
    assert "--stale" in capsys.readouterr().out


@pytest.mark.parametrize(
    "argv",
    [
        ["md", "restart", "binance-um-1"],
        ["intents", "gc", "--instance", "sts-jp"],
    ],
)
def test_new_commands_say_not_implemented_and_exit_one(
    capsys, argv: list[str]
) -> None:
    """A stub exits 1 with one stderr line. It does not need a node."""
    assert main(argv) == EXIT_ERROR
    err = capsys.readouterr().err
    assert err.startswith("mftik: ")
    assert "not implemented (IF-15)" in err
    assert "Traceback" not in err
    assert err.count("\n") == 1


@pytest.mark.parametrize(
    ("argv", "usage"),
    [
        (["md"], "usage: mftik md {restart}"),
        (["td"], "usage: mftik td {drain}"),
        (["intents"], "usage: mftik intents {gc}"),
    ],
)
def test_a_group_without_a_verb_prints_its_own_usage(
    capsys, argv: list[str], usage: str
) -> None:
    assert main(argv) == EXIT_ERROR
    out = capsys.readouterr().out
    assert usage in out
    assert "not implemented" not in out


@pytest.mark.parametrize(
    "argv",
    [
        ["md", "restart"],
        ["td", "drain"],
        ["td", "drain", "nope"],
        ["intents", "gc"],
        ["run", "path", "--wait", "--no-wait"],
    ],
)
def test_missing_or_conflicting_arguments_are_an_argparse_error(
    argv: list[str],
) -> None:
    with pytest.raises(SystemExit) as caught:
        main(argv)
    assert caught.value.code == 2


def test_run_without_the_new_flags_defaults_to_wait() -> None:
    """Bare ``mftik run`` watches (F12). B4-08 flipped the unset default."""
    args = build_parser().parse_args(["run", "some/path"])
    assert args.wait is True
    assert args.no_follow is False


def test_wait_flags_set_the_choice() -> None:
    parser = build_parser()
    assert parser.parse_args(["run", "p", "--wait"]).wait is True
    assert parser.parse_args(["run", "p", "--no-wait"]).wait is False


def test_wait_looks_up_the_tree_instead_of_refusing(capsys) -> None:
    """``--wait`` is implemented. A missing tree fails before any deploy."""
    assert main(["run", "does-not-exist", "--wait"]) == EXIT_ERROR
    captured = capsys.readouterr()
    assert "not implemented" not in captured.err
    assert "does not exist" in captured.err
    assert captured.out == ""


def test_no_wait_looks_up_the_tree_instead_of_refusing(capsys) -> None:
    assert main(["run", "does-not-exist", "--no-wait"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "not implemented" not in err
    assert "does not exist" in err


def test_wait_with_no_follow_looks_up_the_tree(capsys) -> None:
    """DEFAULT 3: the two flags combine. They do not refuse first."""
    assert main(["run", "does-not-exist", "--wait", "--no-follow"]) == EXIT_ERROR
    captured = capsys.readouterr()
    assert "not implemented" not in captured.err
    assert "does not exist" in captured.err
    assert captured.out == ""


def test_deploy_post_uses_an_ordinary_http_timeout() -> None:
    """RM-09 left the number for IF-15. It is one hop, not an on_start budget."""
    assert _DEPLOY_HTTP_TIMEOUT_S == DEFAULT_TIMEOUT_S


def test_a_result_names_one_target() -> None:
    """O3–O5 are the shape, not a later filter. No second conn, account, or instance."""
    assert set(MdRestart.__dataclass_fields__) == {"conn"}
    assert set(TdDrain.__dataclass_fields__) == {"api_id"}
    assert set(IntentGc.__dataclass_fields__) == {"instance"}


def _connect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "config.toml"
    monkeypatch.setenv(config.CONFIG_ENV, str(path))
    monkeypatch.delenv(config.PROFILE_ENV, raising=False)
    config.put(Profile(name="local", url="http://node.test", token="mftik_ak_t"))


def _stub_client(monkeypatch: pytest.MonkeyPatch, handler) -> None:  # noqa: ANN001
    real = httpx.Client

    def build(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs.pop("transport", None)
        return real(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "Client", build)
    monkeypatch.setattr(client_module, "Client", Client)


def test_workers_prints_each_release_from_the_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """O1. The table is ``GET /workers``, including an old release."""
    _connect(tmp_path, monkeypatch)
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path == "/workers":
            return httpx.Response(
                200,
                json={
                    "workers": [
                        {
                            "plane": "md",
                            "instance": "md-1",
                            "id": "md/conn/a",
                            "incarnation": 2,
                            "phase": "running",
                            "ready": True,
                            "code_ref": "1.4.0",
                            "rss_bytes": 10,
                            "age_s": 1.5,
                        },
                        {
                            "plane": "td",
                            "instance": "td-1",
                            "id": "td/account/7",
                            "incarnation": 1,
                            "phase": "starting",
                            "ready": False,
                            "code_ref": "1.5.0",
                            "rss_bytes": None,
                            "age_s": 1.5,
                        },
                    ]
                },
            )
        return httpx.Response(404, json={"detail": "nope"})

    _stub_client(monkeypatch, handler)
    assert main(["workers"]) == 0
    out = capsys.readouterr().out
    assert seen == ["/workers"]
    assert out.index("1.4.0") < out.index("1.5.0")
    assert "md/conn/a" in out
    assert "td/account/7" in out
    assert "md-1" in out
    assert "RELEASE" in out


def test_workers_stale_lists_a_digest_that_is_not_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """Digest staleness is the JSON row. Release staleness is not this flag."""
    _connect(tmp_path, monkeypatch)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/workers":
            return httpx.Response(
                200,
                json={
                    "workers": [
                        {
                            "plane": "sts",
                            "instance": "sts",
                            "id": "sts/session/old",
                            "incarnation": 2,
                            "phase": "running",
                            "ready": True,
                            "code_ref": "1.5.0",
                            "rss_bytes": 10,
                            "age_s": 1.0,
                            "strategy_digest": "sha256:old",
                            "current_digest": "sha256:new",
                        },
                        {
                            "plane": "sts",
                            "instance": "sts",
                            "id": "sts/session/new",
                            "incarnation": 1,
                            "phase": "running",
                            "ready": True,
                            "code_ref": "1.0.0",
                            "rss_bytes": 10,
                            "age_s": 1.0,
                            "strategy_digest": "sha256:new",
                            "current_digest": "sha256:new",
                        },
                        {
                            "plane": "sts",
                            "instance": "sts",
                            "id": "sts/session/gone",
                            "incarnation": 1,
                            "phase": "running",
                            "ready": True,
                            "code_ref": "1.5.0",
                            "rss_bytes": None,
                            "age_s": 1.0,
                            "strategy_digest": "sha256:pinned",
                        },
                        {
                            "plane": "md",
                            "instance": "md-1",
                            "id": "md/conn/a",
                            "incarnation": 1,
                            "phase": "running",
                            "ready": True,
                            "code_ref": "0.1.0",
                            "rss_bytes": 1,
                            "age_s": 1.0,
                        },
                    ]
                },
            )
        return httpx.Response(404, json={"detail": "nope"})

    _stub_client(monkeypatch, handler)
    assert main(["workers", "--stale"]) == 0
    out = capsys.readouterr().out
    assert "sts/session/old" in out
    assert "sts/session/gone" in out
    assert "sts/session/new" not in out
    assert "md/conn/a" not in out


def test_workers_with_nothing_reported_is_not_an_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    _connect(tmp_path, monkeypatch)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/workers":
            return httpx.Response(200, json={"workers": []})
        return httpx.Response(404, json={"detail": "nope"})

    _stub_client(monkeypatch, handler)
    assert main(["workers"]) == 0
    assert "no workers" in capsys.readouterr().out


def test_decisions_raise_not_implemented() -> None:
    """md restart, stale workers and intent gc still raise ``IF-15``.

    ``td_drain`` names the account (B6-04).
    """
    worker = ProcmanWorker(
        id="md/conn/a",
        code_ref="1.4.0",
        rss_bytes=1,
        phase="running",
        ready=True,
        incarnation=1,
    )
    with pytest.raises(NotImplementedError, match="^IF-15$"):
        select_workers([worker], stale=True, latest="1.5.0")
    with pytest.raises(NotImplementedError, match="^IF-15$"):
        md_restart("binance-um-1")
    assert td_drain(7) == TdDrain(api_id=7)
    with pytest.raises(NotImplementedError, match="^IF-15$"):
        intent_gc("sts-jp")


def test_td_drain_posts_that_account(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """O4. One POST, that api_id, and the new incarnation on stdout."""
    _connect(tmp_path, monkeypatch)
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        if request.url.path == "/td/accounts/7/drain":
            return httpx.Response(
                200,
                json={"api_id": 7, "ok": True, "incarnation": 3, "reason": ""},
            )
        return httpx.Response(404, json={"detail": "nope"})

    _stub_client(monkeypatch, handler)
    assert main(["td", "drain", "7"]) == 0
    out = capsys.readouterr().out
    assert seen == [("POST", "/td/accounts/7/drain")]
    assert "api_id=7" in out
    assert "incarnation=3" in out


def test_td_drain_refusal_exits_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    _connect(tmp_path, monkeypatch)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "api_id": 7,
                "ok": False,
                "incarnation": None,
                "reason": "not_drained",
            },
        )

    _stub_client(monkeypatch, handler)
    assert main(["td", "drain", "7"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "not_drained" in err
    assert "Traceback" not in err
