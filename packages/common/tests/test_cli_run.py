"""``mftik run`` — push then deploy, and the flags that skip a step."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from mftik.cli import client as client_module
from mftik.cli import config
from mftik.cli import run as run_module
from mftik.cli.app import EXIT_ERROR, EXIT_INTERRUPTED, main
from mftik.cli.client import Client, CliError
from mftik.cli.config import Profile
from mftik.clock import FakeClock, SystemClock

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""

_OK_YML = "td: {}\nmd: []\nsts: {}\n"
_REAL_HTTPX = httpx.Client

# `main()` builds the whole CLI parser; that call does not fit 50 ms.
pytestmark = pytest.mark.component


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
        if request.url.path.endswith("/stop"):
            return httpx.Response(
                200,
                json={
                    "session_id": "sess-1",
                    "status": "done",
                    "strategy": "tiny",
                    "reason": "operator_stop",
                },
            )
        if request.url.path == "/sts/deploy/private::Tiny":
            return httpx.Response(
                200,
                json={
                    "id": 1,
                    "session_id": "sess-1",
                    "type": "private::Tiny",
                    "config": {},
                    "td": [],
                    "md": [],
                    "status": "running",
                },
            )
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


def _tree(tmp_path: Path) -> Path:
    dest = tmp_path / "hello"
    dest.mkdir()
    (dest / "strategy.py").write_text(_TINY)
    return dest


def test_run_pushes_then_deploys_and_follows(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    fake = Node_()
    followed = _install(monkeypatch, fake)
    dest = _tree(tmp_path)
    cfg = tmp_path / "deploy.yml"
    cfg.write_text(_OK_YML)

    assert main(["run", str(dest), str(cfg)]) == 0
    out = capsys.readouterr().out
    assert "pushed tiny" in out
    assert "running private::Tiny session=sess-1" in out
    assert fake.paths == ["/registry/v1/add", "/sts/deploy/private::Tiny"]
    deploy = fake.bodies[1]
    assert isinstance(deploy, dict)
    assert deploy["yaml"] == _OK_YML
    assert followed == ["sess-1"]


def test_no_push_skips_add(tmp_path: Path, monkeypatch, capsys) -> None:
    fake = Node_()
    followed = _install(monkeypatch, fake)

    assert main(["run", str(_tree(tmp_path)), "--no-push", "--no-follow"]) == 0
    assert fake.paths == ["/sts/deploy/private::Tiny"]
    assert followed == []
    assert "running private::Tiny session=sess-1" in capsys.readouterr().out


def test_loaded_false_does_not_deploy(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    fake = Node_(loaded=False, load_error="STS did not load it as 'private::Tiny'")
    followed = _install(monkeypatch, fake)

    assert main(["run", str(_tree(tmp_path)), "--no-follow"]) == EXIT_ERROR
    assert "/sts/deploy/private::Tiny" not in fake.paths
    assert followed == []
    assert "private::Tiny" in capsys.readouterr().err


def test_no_follow_does_not_open_a_socket(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    fake = Node_()
    followed = _install(monkeypatch, fake)

    assert main(["run", str(_tree(tmp_path)), "--no-follow"]) == 0
    assert followed == []
    assert "session=sess-1" in capsys.readouterr().out


def test_run_refuses_a_file(tmp_path: Path, capsys) -> None:
    path = tmp_path / "strategy.py"
    path.write_text(_TINY)
    assert main(["run", str(path)]) == EXIT_ERROR
    assert "directory" in capsys.readouterr().err


# --- Ctrl-C ---------------------------------------------------------------
#
# The decision this command exists around. A strategy is placing orders and
# somebody is watching it in the foreground; the key they reach for to make it
# stop has to make it stop. Detaching instead would leave a live position
# behind on a keystroke every other program treats as "end this".


def _interrupting(monkeypatch: pytest.MonkeyPatch, fake: Node_) -> None:
    """Follow the log, then have the socket raise KeyboardInterrupt."""

    def follow(self, session_id: str, write=print) -> None:  # noqa: ANN001
        del self, session_id, write
        raise KeyboardInterrupt

    def build(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs.pop("transport", None)
        return _REAL_HTTPX(*args, transport=httpx.MockTransport(fake), **kwargs)

    monkeypatch.setattr(httpx, "Client", build)
    monkeypatch.setattr(client_module, "Client", Client)
    monkeypatch.setattr(Client, "follow_sts_logs", follow)


def test_ctrl_c_stops_the_session(tmp_path: Path, monkeypatch, capsys) -> None:
    fake = Node_()
    _interrupting(monkeypatch, fake)
    dest = _tree(tmp_path)

    code = main(["run", str(dest)])

    assert "/sts/sessions/sess-1/stop" in fake.paths
    out = capsys.readouterr().out
    assert "stopped sess-1" in out
    assert code == EXIT_INTERRUPTED


def test_the_offer_is_made_before_it_is_needed(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Nobody reads the manual first. Attaching says what ^C will do."""
    _interrupting(monkeypatch, Node_())
    dest = _tree(tmp_path)

    main(["run", str(dest)])

    assert "^C stops this session" in capsys.readouterr().out


def test_a_second_ctrl_c_leaves_it_running_and_says_so(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """The escape hatch, and the one that has to be typed twice."""
    fake = Node_()
    _interrupting(monkeypatch, fake)

    real_post = Client.post

    def interrupt_the_stop(self, path, **kwargs):  # noqa: ANN001, ANN002
        # Only the stop: the deploy has to land first, or there is no session
        # for the second Ctrl-C to be about.
        if path.endswith("/stop"):
            raise KeyboardInterrupt
        return real_post(self, path, **kwargs)

    monkeypatch.setattr(Client, "post", interrupt_the_stop)
    dest = _tree(tmp_path)

    code = main(["run", str(dest)])

    out = capsys.readouterr().out
    assert "still trading" in out
    assert "mftik stop sess-1" in out
    assert code == EXIT_INTERRUPTED


def test_a_stop_that_does_not_land_is_not_reported_as_stopped(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """The worst outcome of pressing ^C would be being told it worked."""
    fake = Node_()
    _interrupting(monkeypatch, fake)
    real_post = Client.post

    def post(self, path, **kwargs):  # noqa: ANN001, ANN002
        if path.endswith("/stop"):
            raise CliError("502 from the node")
        return real_post(self, path, **kwargs)

    monkeypatch.setattr(Client, "post", post)
    dest = _tree(tmp_path)

    code = main(["run", str(dest)])

    err = capsys.readouterr().err
    assert "could not stop sess-1" in err
    assert "may still be running" in err
    assert code == EXIT_ERROR


def test_logs_follow_does_not_stop_anything(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """``logs -f`` did not start the session and has no business ending it."""
    fake = Node_()
    _interrupting(monkeypatch, fake)

    code = main(["logs", "sess-1", "-f"])

    assert not any(p.endswith("/stop") for p in fake.paths)
    assert code == EXIT_INTERRUPTED


def test_no_follow_leaves_it_up_and_says_how_to_end_it(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    dest = _tree(tmp_path)

    assert main(["run", str(dest), "--no-follow"]) == 0

    out = capsys.readouterr().out
    assert "left running" in out
    assert "mftik stop sess-1" in out
    assert not any(p.endswith("/stop") for p in fake.paths)


def test_a_session_that_failed_during_deploy_prints_the_page(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """``failed`` on the 202 is settled. Print the reason and the stored
    page, do not open the socket, exit non-zero."""

    class Refusing(Node_):
        def __call__(self, request: httpx.Request) -> httpx.Response:
            if request.url.path.startswith("/sts/deploy/"):
                self.paths.append(request.url.path)
                return httpx.Response(
                    202,
                    json={
                        "session_id": "sess-1",
                        "status": "failed",
                        "reason": "on_start blew up",
                    },
                )
            if request.url.path == "/logs/sts/sess-1":
                self.paths.append(request.url.path)
                return httpx.Response(
                    200,
                    json={
                        "logs": [
                            {"level": "error", "message": "on_start blew up"}
                        ]
                    },
                )
            return super().__call__(request)

    fake = Refusing()
    followed = _install(monkeypatch, fake)
    dest = _tree(tmp_path)

    assert main(["run", str(dest), "--no-push"]) == EXIT_ERROR

    assert followed == []
    out = capsys.readouterr().out
    assert "session failed: on_start blew up" in out
    assert "error  on_start blew up" in out
    assert "nothing to follow" not in out
    assert "/logs/sts/sess-1" in fake.paths


class _FailingDeploy(Node_):
    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/sts/deploy/"):
            self.paths.append(request.url.path)
            return httpx.Response(
                500, json={"detail": "Internal Server Error"}
            )
        return super().__call__(request)


class _FailingPush(Node_):
    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/registry/v1/add":
            self.paths.append(request.url.path)
            return httpx.Response(
                500, json={"detail": "Internal Server Error"}
            )
        return super().__call__(request)


def test_run_prints_missing_extras_and_does_not_invent_a_session(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    class Missing(Node_):
        def __call__(self, request: httpx.Request) -> httpx.Response:
            if request.url.path.startswith("/sts/deploy/"):
                self.paths.append(request.url.path)
                return httpx.Response(
                    409,
                    json={
                        "detail": (
                            "incompatible_environment: private::Tiny requires "
                            "numpy which this node does not have"
                        )
                    },
                )
            return super().__call__(request)

    _install(monkeypatch, Missing())
    dest = _tree(tmp_path)

    assert main(["run", str(dest), "--no-push", "--no-follow"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "numpy" in err
    assert "incompatible_environment" in err
    assert "unknown_strategy" not in err
    assert "may already be live" not in err
    assert "Traceback" not in err


def test_a_failed_deploy_names_the_route_and_says_to_check_ps(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    fake = _FailingDeploy()
    _install(monkeypatch, fake)
    dest = _tree(tmp_path)

    assert main(["run", str(dest), "--no-follow"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "POST /sts/deploy/private::Tiny 500" in err
    assert "mftik ps" in err
    assert "Do not run again" in err


def test_a_failed_push_does_not_claim_a_session_started(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    fake = _FailingPush()
    followed = _install(monkeypatch, fake)
    dest = _tree(tmp_path)

    assert main(["run", str(dest), "--no-follow"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "POST /registry/v1/add 500" in err
    assert "mftik ps" not in err
    assert "/sts/deploy/private::Tiny" not in fake.paths
    assert followed == []


def test_ctrl_c_during_deploy_says_to_check_ps(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    real_post = Client.post

    def interrupt_deploy(self, path, **kwargs):  # noqa: ANN001, ANN002
        if "/sts/deploy/" in path:
            raise KeyboardInterrupt
        return real_post(self, path, **kwargs)

    monkeypatch.setattr(Client, "post", interrupt_deploy)
    dest = _tree(tmp_path)

    assert main(["run", str(dest), "--no-follow"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "interrupted during deploy" in err
    assert "mftik ps" in err
    assert "Do not run again" in err


def _use_clock(monkeypatch: pytest.MonkeyPatch, clock: FakeClock | SystemClock) -> None:
    monkeypatch.setattr(run_module, "_clock", clock)


class _Phases(Node_):
    """Deploy answers ``starting``. Later GETs walk ``script``."""

    def __init__(self, script: list[dict]) -> None:
        super().__init__()
        self.script = list(script)
        self.gets = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/sts/deploy/private::Tiny":
            self.paths.append(path)
            return httpx.Response(
                202,
                json={"session_id": "sess-1", "status": "starting"},
            )
        if path == "/sts/sessions/sess-1":
            self.paths.append(path)
            self.gets += 1
            body = self.script.pop(0) if self.script else {"phase": "running"}
            return httpx.Response(200, json=body)
        if path == "/logs/sts/sess-1":
            self.paths.append(path)
            return httpx.Response(
                200,
                json={
                    "logs": [
                        {"level": "info", "message": "newer"},
                        {"level": "error", "message": "older"},
                    ]
                },
            )
        if path.endswith("/stop"):
            self.paths.append(path)
            return httpx.Response(
                200,
                json={
                    "session_id": "sess-1",
                    "status": "done",
                    "reason": "operator_stop",
                },
            )
        return httpx.Response(404, json={"detail": "nope"})


def test_wait_follows_once_the_phase_reaches_running(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """``starting`` → ``starting`` → ``running`` tails once. Sleep is the clock."""
    clock = FakeClock()
    _use_clock(monkeypatch, clock)
    fake = _Phases(
        [
            {
                "phase": "starting",
                "status": "live",
                "conditions": {"phase": "starting"},
            },
            {
                "phase": "running",
                "status": "live",
                "conditions": {"phase": "running", "MdReady": "2/2"},
            },
        ]
    )
    followed = _install(monkeypatch, fake)

    assert main(["run", str(_tree(tmp_path)), "--no-push"]) == 0

    assert followed == ["sess-1"]
    assert fake.gets == 2
    assert clock.monotonic() == 2 * run_module._WAIT_POLL_S
    out = capsys.readouterr().out
    assert out.count("\n  starting\n") == 1
    assert "  running  MdReady=2/2" in out
    assert "^C stops this session" in out


def test_a_failed_phase_prints_the_stored_page_and_does_not_open_the_socket(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    clock = FakeClock()
    _use_clock(monkeypatch, clock)
    fake = _Phases(
        [
            {
                "phase": "failed",
                "status": "failed",
                "reason": "td not ready",
                "conditions": {"phase": "failed"},
            }
        ]
    )
    followed = _install(monkeypatch, fake)

    assert main(["run", str(_tree(tmp_path)), "--no-push"]) == EXIT_ERROR

    assert followed == []
    out = capsys.readouterr().out
    assert "session failed: td not ready" in out
    # Newest-first on the wire, oldest-first on the page.
    assert out.index("error  older") < out.index("info  newer")
    assert "/ws/" not in "".join(fake.paths)


def test_a_done_phase_prints_the_status_and_the_stored_page(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    clock = FakeClock()
    _use_clock(monkeypatch, clock)
    fake = _Phases(
        [
            {
                "phase": "done",
                "status": "done",
                "conditions": {"phase": "done"},
            }
        ]
    )
    followed = _install(monkeypatch, fake)

    assert main(["run", str(_tree(tmp_path)), "--no-push"]) == 0

    assert followed == []
    out = capsys.readouterr().out
    assert "session done" in out
    assert "error  older" in out
    assert clock.monotonic() == run_module._WAIT_POLL_S


def test_no_follow_watches_then_prints_how_to_stop(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    clock = FakeClock()
    _use_clock(monkeypatch, clock)
    fake = _Phases(
        [{"phase": "running", "status": "live", "conditions": {"phase": "running"}}]
    )
    followed = _install(monkeypatch, fake)

    code = main(["run", str(_tree(tmp_path)), "--no-push", "--no-follow"])

    assert code == 0
    assert followed == []
    out = capsys.readouterr().out
    assert "left running" in out
    assert "mftik stop sess-1" in out
    assert "/logs/" not in "".join(fake.paths)


def test_ctrl_c_during_the_watch_sends_one_stop(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    class _Interrupt(SystemClock):
        async def sleep(self, seconds: float) -> None:
            del seconds
            raise KeyboardInterrupt

    _use_clock(monkeypatch, _Interrupt())
    fake = _Phases([])
    followed = _install(monkeypatch, fake)

    code = main(["run", str(_tree(tmp_path)), "--no-push"])

    assert code == EXIT_INTERRUPTED
    assert followed == []
    assert fake.paths == [
        "/sts/deploy/private::Tiny",
        "/sts/sessions/sess-1/stop",
    ]
    assert "stopped sess-1" in capsys.readouterr().out


def test_an_unknown_phase_is_printed_once_and_keeps_watching(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    clock = FakeClock()
    _use_clock(monkeypatch, clock)
    fake = _Phases(
        [
            {"phase": "sideways", "status": "live"},
            {"phase": "sideways", "status": "live"},
            {"phase": "running", "status": "live"},
        ]
    )
    followed = _install(monkeypatch, fake)

    assert main(["run", str(_tree(tmp_path)), "--no-push"]) == 0

    assert followed == ["sess-1"]
    assert capsys.readouterr().out.count("\n  sideways\n") == 1
    assert clock.monotonic() == 3 * run_module._WAIT_POLL_S


def test_a_missing_session_while_waiting_is_an_error_at_once(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    clock = FakeClock()
    _use_clock(monkeypatch, clock)

    class _Gone(_Phases):
        def __call__(self, request: httpx.Request) -> httpx.Response:
            if request.url.path == "/sts/sessions/sess-1":
                self.paths.append(request.url.path)
                self.gets += 1
                return httpx.Response(404, json={"detail": "missing"})
            return super().__call__(request)

    fake = _Gone([])
    _install(monkeypatch, fake)

    assert main(["run", str(_tree(tmp_path)), "--no-push"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "was not found" in err
    assert fake.gets == 1
    assert clock.monotonic() == run_module._WAIT_POLL_S


def test_poll_misses_give_up_and_say_the_session_may_still_be_running(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    clock = FakeClock()
    _use_clock(monkeypatch, clock)

    class _Down(_Phases):
        def __call__(self, request: httpx.Request) -> httpx.Response:
            if request.url.path == "/sts/sessions/sess-1":
                self.paths.append(request.url.path)
                self.gets += 1
                return httpx.Response(503, json={"detail": "busy"})
            return super().__call__(request)

    fake = _Down([])
    _install(monkeypatch, fake)

    assert main(["run", str(_tree(tmp_path)), "--no-push"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "may still be running" in err
    assert "mftik stop sess-1" in err
    assert fake.gets == run_module._WAIT_MAX_MISSES
    assert clock.monotonic() == run_module._WAIT_MAX_MISSES * run_module._WAIT_POLL_S
