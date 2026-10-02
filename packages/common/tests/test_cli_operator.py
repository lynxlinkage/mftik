"""IF-15's CLI surface, as it behaves today.

The new commands parse and then refuse. ``mftik run`` grows ``--wait`` /
``--no-wait`` without taking them: passing either one refuses before a
deploy, and passing neither keeps the command that already exists.
``mftik --help`` lists the new commands because they are rows in the same
table the dispatch reads.
"""

from __future__ import annotations

import pytest
from mftik.cli.app import EXIT_ERROR, build_parser, main
from mftik.cli.client import DEFAULT_TIMEOUT_S
from mftik.cli.operator import (
    IntentGc,
    MdRestart,
    TdDrain,
    intent_gc,
    md_restart,
    select_workers,
    td_drain,
)
from mftik.cli.run import _DEPLOY_HTTP_TIMEOUT_S, run_wait_action
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
        ["workers"],
        ["workers", "--stale"],
        ["md", "restart", "binance-um-1"],
        ["td", "drain", "7"],
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


def test_run_without_the_new_flags_leaves_wait_unset() -> None:
    """Bare ``mftik run`` is still today's command. B4-08 flips this."""
    args = build_parser().parse_args(["run", "some/path"])
    assert args.wait is None
    assert args.no_follow is False


def test_wait_flags_set_the_choice() -> None:
    parser = build_parser()
    assert parser.parse_args(["run", "p", "--wait"]).wait is True
    assert parser.parse_args(["run", "p", "--no-wait"]).wait is False


def test_wait_refuses_before_any_deploy(capsys) -> None:
    """``--wait`` must not start a session on the way to saying it cannot."""
    assert main(["run", "does-not-exist", "--wait"]) == EXIT_ERROR
    captured = capsys.readouterr()
    assert "not implemented (IF-15)" in captured.err
    assert "directory" not in captured.err
    assert captured.out == ""


def test_no_wait_refuses_the_same_way(capsys) -> None:
    assert main(["run", "does-not-exist", "--no-wait"]) == EXIT_ERROR
    assert "not implemented (IF-15)" in capsys.readouterr().err


def test_wait_with_no_follow_is_still_not_implemented(capsys) -> None:
    """The plan does not say how these two flags combine. IF-15 does not pick."""
    assert main(["run", "does-not-exist", "--wait", "--no-follow"]) == EXIT_ERROR
    captured = capsys.readouterr()
    assert "not implemented (IF-15)" in captured.err
    assert captured.out == ""


def test_deploy_post_uses_an_ordinary_http_timeout() -> None:
    """RM-09 left the number for IF-15. It is one hop, not an on_start budget."""
    assert _DEPLOY_HTTP_TIMEOUT_S == DEFAULT_TIMEOUT_S


def test_a_result_names_one_target() -> None:
    """O3–O5 are the shape, not a later filter. No second conn, account, or instance."""
    assert set(MdRestart.__dataclass_fields__) == {"conn"}
    assert set(TdDrain.__dataclass_fields__) == {"api_id"}
    assert set(IntentGc.__dataclass_fields__) == {"instance"}


def test_decisions_raise_not_implemented() -> None:
    """共同驗收: the surface returns ``NotImplementedError("IF-15")``."""
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
    with pytest.raises(NotImplementedError, match="^IF-15$"):
        td_drain(7)
    with pytest.raises(NotImplementedError, match="^IF-15$"):
        intent_gc("sts-jp")
    with pytest.raises(NotImplementedError, match="^IF-15$"):
        run_wait_action("starting", wait=True)
