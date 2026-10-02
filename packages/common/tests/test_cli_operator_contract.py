"""What the operator CLI will do, written down before it does it (IF-15).

Every test here is ``xfail(strict=True)``. It describes behaviour the plan
settles (F12, F24, F27, F32) and fails today because the surface raises
``NotImplementedError("IF-15")`` or, for the two ``mftik run`` invocations,
refuses before it deploys. ``strict`` is the point: the ticket that
implements one of these cannot merge while the marker is still on it.

The decisions are pure. Nothing here names an HTTP route — IF-15 does not
add API routes, and the B tickets that perform these commands choose how
the CLI reaches a node. The two ``mftik run`` cases are the exception:
they drive ``main``, because ``--wait`` / ``--no-wait`` are flags on a
command that already deploys, and the deploy URL is the one ``run``
already posts to.

``stopping`` and ``done`` are absent on purpose. The plan says a ``--wait``
ends on ``running`` or ``failed`` and does not say what a snapshot of
``stopping`` or ``done`` means if ``running`` was never observed.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from mftik.cli import client as client_module
from mftik.cli import config
from mftik.cli.app import main
from mftik.cli.client import Client, CliError
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
from mftik.cli.run import run_wait_action
from mftik.protocol import ProcmanWorker

# `main()` builds the whole CLI parser; that call does not fit 50 ms.
pytestmark = pytest.mark.component

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""

_REAL_HTTPX = httpx.Client


def _worker(worker_id: str, code_ref: str) -> ProcmanWorker:
    return ProcmanWorker(
        id=worker_id,
        code_ref=code_ref,
        rss_bytes=1,
        phase="running",
        ready=True,
        incarnation=1,
    )


# --- F12: mftik run --wait / --no-wait ------------------------------------


@pytest.mark.xfail(strict=True, reason="B4-08 makes --wait the default of mftik run")
def test_run_defaults_to_wait() -> None:
    """§5.2: a bare ``mftik run`` watches until running or failed, then tails.

    IF-15 leaves the flag unset so today's deploy-and-follow still runs.
    B4-08 deletes that split and this marker together.
    """
    from mftik.cli.app import build_parser

    assert build_parser().parse_args(["run", "some/path"]).wait is True


@pytest.mark.xfail(strict=True, reason="B4-08 watches until running or failed")
@pytest.mark.parametrize(
    ("status", "step"),
    [
        ("pending", "wait"),
        ("starting", "wait"),
        ("restarting", "wait"),
        ("running", "tail"),
        ("failed", "tail"),
    ],
)
def test_wait_tails_only_once_running_or_failed(status: str, step: str) -> None:
    """W1. A 202 of ``starting`` is not the end of the watch (F12)."""
    assert run_wait_action(status, wait=True) == step


@pytest.mark.xfail(strict=True, reason="B4-08 --no-wait returns the session id")
@pytest.mark.parametrize("status", ["starting", "running", "failed", "pending"])
def test_no_wait_returns_the_id_for_every_status(status: str) -> None:
    """W2. ``--no-wait`` does not tail, whatever the snapshot says."""
    assert run_wait_action(status, wait=False) == "return_id"


class _Deploy:
    """A node whose deploy answers 202, which is what F12's start returns."""

    def __init__(self, status: str) -> None:
        self.status = status
        self.paths: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        if request.url.path == "/sts/deploy/private::Tiny":
            return httpx.Response(
                202,
                json={"session_id": "sess-1", "status": self.status},
            )
        return httpx.Response(404, json={"detail": "nope"})


@pytest.fixture
def _connected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "config.toml"
    monkeypatch.setenv(config.CONFIG_ENV, str(path))
    monkeypatch.delenv(config.PROFILE_ENV, raising=False)
    config.put(Profile(name="local", url="http://node.test", token="mftik_ak_t"))
    return path


def _install(monkeypatch: pytest.MonkeyPatch, fake: _Deploy) -> list[str]:
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


def _tree(tmp_path: Path) -> Path:
    dest = tmp_path / "hello"
    dest.mkdir()
    (dest / "strategy.py").write_text(_TINY)
    return dest


@pytest.mark.xfail(strict=True, reason="B4-08 --no-wait returns the session id")
def test_no_wait_prints_the_session_id_and_does_not_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys, _connected: Path
) -> None:
    """``--no-wait`` posts the deploy and stops. It does not take the legacy
    ``starting`` path, which says there is nothing to follow."""
    del _connected
    fake = _Deploy("starting")
    followed = _install(monkeypatch, fake)

    assert main(["run", str(_tree(tmp_path)), "--no-push", "--no-wait"]) == 0
    out = capsys.readouterr().out
    assert "sess-1" in out
    assert "nothing to follow" not in out
    assert "left running" not in out
    assert followed == []
    assert fake.paths == ["/sts/deploy/private::Tiny"]


@pytest.mark.xfail(strict=True, reason="B4-08 --wait tails once the session is running")
def test_wait_tails_when_the_deploy_is_already_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys, _connected: Path
) -> None:
    """``running`` is the v2 word. The legacy command only follows ``live``,
    so a status of ``running`` must not be treated as already finished."""
    del _connected
    fake = _Deploy("running")
    followed = _install(monkeypatch, fake)

    assert main(["run", str(_tree(tmp_path)), "--no-push", "--wait"]) == 0
    out = capsys.readouterr().out
    assert "nothing to follow" not in out
    assert followed == ["sess-1"]
    assert "/sts/sessions/sess-1/stop" not in fake.paths


# --- F24: workers --stale, md restart --------------------------------------


@pytest.mark.xfail(strict=True, reason="B3-07 lists every worker's release")
def test_workers_lists_every_reported_worker_in_order() -> None:
    """O1. Without ``--stale``, an old release is still listed."""
    reported = [
        _worker("md/conn/a", "1.4.0"),
        _worker("td/account/7", "1.5.0"),
        _worker("md/conn/b", "1.4.0"),
    ]
    assert select_workers(reported, stale=False, latest="1.5.0") == reported


@pytest.mark.xfail(strict=True, reason="B8-06 lists workers not on the latest release")
def test_stale_keeps_only_workers_off_the_latest_release() -> None:
    """O2. Listing is the whole command. Nothing here restarts them (F24)."""
    old = _worker("md/conn/a", "1.4.0")
    current = _worker("td/account/7", "1.5.0")
    also_old = _worker("sts/session/s", "1.4.0")
    chosen = select_workers(
        [old, current, also_old], stale=True, latest="1.5.0"
    )
    assert chosen == [old, also_old]


@pytest.mark.xfail(strict=True, reason="B8-06 lists workers not on the latest release")
def test_stale_is_empty_when_every_worker_is_current() -> None:
    reported = [_worker("md/conn/a", "1.5.0")]
    assert select_workers(reported, stale=True, latest="1.5.0") == []


@pytest.mark.xfail(strict=True, reason="B3-07 lists every worker's release")
def test_no_workers_lists_nothing() -> None:
    assert select_workers((), stale=False, latest="1.5.0") == []


@pytest.mark.xfail(strict=True, reason="B3-07 lists every worker's release")
def test_listing_does_not_need_the_latest_release() -> None:
    """O1. ``--stale`` is the only mode that reads the current release."""
    reported = [_worker("md/conn/a", "1.4.0")]
    assert select_workers(reported, stale=False, latest=None) == reported


@pytest.mark.xfail(strict=True, reason="B8-06 lists workers not on the latest release")
def test_stale_of_no_workers_is_empty() -> None:
    assert select_workers((), stale=True, latest="1.5.0") == []


@pytest.mark.xfail(
    strict=True,
    reason="B8-06 refuses --stale when the latest release is unknown",
)
def test_stale_without_a_latest_release_is_refused() -> None:
    """An unread release must not come out as 'every worker is stale' (O2)."""
    with pytest.raises(CliError):
        select_workers([_worker("md/conn/a", "1.4.0")], stale=True, latest=None)


@pytest.mark.xfail(strict=True, reason="B8-06 restarts one MD connection in place")
def test_md_restart_names_that_connection_and_no_other() -> None:
    """O3. One conn, in place. Not a placement change (F24)."""
    assert md_restart("binance-um-1") == MdRestart(conn="binance-um-1")


# --- F27: td drain ---------------------------------------------------------


@pytest.mark.xfail(strict=True, reason="B6-04 drain-replaces one account")
def test_td_drain_names_that_account_and_no_other() -> None:
    """O4. One api_id. Other accounts are not this command's (F27)."""
    assert td_drain(7) == TdDrain(api_id=7)


# --- F32: intents gc -------------------------------------------------------


@pytest.mark.xfail(strict=True, reason="F32 manual reclaim of one instance")
def test_intent_gc_names_that_instance() -> None:
    """O5. The operator names the machine. A missing report does not."""
    assert intent_gc("sts-jp") == IntentGc(instance="sts-jp")


@pytest.mark.xfail(strict=True, reason="F32 refuses a gc that names no instance")
@pytest.mark.parametrize("instance", ["", "   "])
def test_intent_gc_refuses_a_blank_instance(instance: str) -> None:
    with pytest.raises(CliError):
        intent_gc(instance)
