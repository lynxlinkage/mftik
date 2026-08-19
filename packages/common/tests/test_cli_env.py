"""``mftik env`` — the extras surface from a terminal."""

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

_REAL_HTTPX = httpx.Client


@pytest.fixture(autouse=True)
def config_file(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "config.toml"
    monkeypatch.setenv(config.CONFIG_ENV, str(path))
    monkeypatch.delenv(config.PROFILE_ENV, raising=False)
    config.put(Profile(name="local", url="http://node.test", token="mftik_ak_t"))
    return path


def _environment(**over: object) -> dict[str, object]:
    body: dict[str, object] = {
        "generation": 1,
        "python": [3, 12],
        "platform": "linux-x86_64",
        "bytes": 41231,
        "packages": {
            "pandas": {"version": "2.2.3", "dist": "pandas", "source": "manual"}
        },
        "installed": [
            {
                "dist": "pandas",
                "version": "2.2.3",
                "approved": True,
                "suggested_name": "pandas",
                "needed_by": [],
            },
            {
                "dist": "numpy",
                "version": "2.1.3",
                "approved": False,
                "suggested_name": "numpy",
                "needed_by": ["pandas"],
            },
            {
                "dist": "six",
                "version": "1.16.0",
                "approved": False,
                "suggested_name": "six",
                "needed_by": ["python-dateutil"],
            },
            {
                "dist": "python-dateutil",
                "version": "2.9.0",
                "approved": False,
                # Off the wheel's top_level.txt. The guess from the dist name
                # would be ``python_dateutil``, which imports nothing.
                "suggested_name": "dateutil",
                "needed_by": ["pandas"],
            },
            {
                "dist": "ambiguous-pkg",
                "version": "1.0",
                "approved": False,
                "suggested_name": None,
                "needed_by": ["pandas"],
            },
        ],
        "abi_ok": True,
        "runtime_python": [3, 12],
        "runtime_platform": "linux-x86_64",
        "restart_required": False,
        "loaded": True,
        "load_error": None,
        "broken": [],
    }
    body.update(over)
    return body


class Node_:
    def __init__(self, **over: object) -> None:
        self.env = _environment(**over)
        self.paths: list[str] = []
        self.bodies: list[object] = []
        self.methods: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        self.methods.append(request.method)
        if request.content:
            self.bodies.append(json.loads(request.content))
        if request.url.path.startswith("/environment"):
            return httpx.Response(200, json=self.env)
        return httpx.Response(404, json={"detail": "nope"})


def _install(monkeypatch: pytest.MonkeyPatch, fake: Node_) -> None:
    def build(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs.pop("transport", None)
        return _REAL_HTTPX(*args, transport=httpx.MockTransport(fake), **kwargs)

    monkeypatch.setattr(httpx, "Client", build)
    monkeypatch.setattr(client_module, "Client", Client)


def test_list_shows_the_stamp(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_())
    assert main(["env", "list"]) == 0
    out = capsys.readouterr().out
    assert "generation 1" in out
    assert "pandas" in out and "2.2.3" in out


def test_list_says_when_the_overlay_is_for_another_interpreter(
    monkeypatch, capsys
) -> None:
    """Otherwise "my extras vanished" is all the operator has to go on."""
    _install(
        monkeypatch,
        Node_(abi_ok=False, python=[3, 11], runtime_python=[3, 12]),
    )
    assert main(["env", "list"]) == 0
    out = capsys.readouterr().out
    assert "ABI MISMATCH" in out
    assert "3.11" in out and "3.12" in out
    assert "apply again" in out


def test_deps_names_who_pulled_each_one_in(monkeypatch, capsys) -> None:
    """six next to numpy reads as equally the Owner's business. It is not."""
    _install(monkeypatch, Node_())
    assert main(["env", "deps"]) == 0
    out = capsys.readouterr().out
    assert "pandas" not in out.split("NEEDED BY")[0], "approved rows are not listed"
    lines = {line.split()[0]: line for line in out.splitlines() if line.strip()}
    assert "pandas" in lines["numpy"]
    assert "python-dateutil" in lines["six"]
    # Read off the wheel, not guessed from the hyphen.
    assert "dateutil" in lines["python-dateutil"]
    assert "python_dateutil" not in lines["python-dateutil"]
    # Several top-level names is a choice; the CLI does not make it.
    assert "import name differs" in lines["ambiguous-pkg"]


def test_add_may_omit_the_version(monkeypatch, capsys) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    assert main(["env", "add", "pandas"]) == 0
    assert fake.bodies[-1] == {"name": "pandas"}
    assert "applied pandas==2.2.3" in capsys.readouterr().out


def test_add_sends_what_it_was_given(monkeypatch) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    argv = ["env", "add", "sklearn", "--version", "1.6.1", "--dist", "scikit-learn"]
    assert main(argv) == 0
    assert fake.bodies[-1] == {
        "name": "sklearn",
        "version": "1.6.1",
        "dist": "scikit-learn",
    }


def test_approve_pins_at_the_version_already_installed(monkeypatch, capsys) -> None:
    """A no-op for the installer, which is the point: nothing moves."""
    fake = Node_()
    _install(monkeypatch, fake)
    assert main(["env", "approve", "numpy"]) == 0
    assert fake.bodies[-1] == {
        "name": "numpy",
        "version": "2.1.3",
        "dist": "numpy",
        "source": "dependency",
    }
    assert "approved numpy==2.1.3" in capsys.readouterr().out


def test_approve_refuses_a_dist_with_no_usable_import_name(
    monkeypatch, capsys
) -> None:
    _install(monkeypatch, Node_())
    assert main(["env", "approve", "ambiguous-pkg"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "--name" in err


def test_approve_uses_the_import_name_the_wheel_declares(monkeypatch) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    assert main(["env", "approve", "python-dateutil"]) == 0
    assert fake.bodies[-1]["name"] == "dateutil"
    assert fake.bodies[-1]["dist"] == "python-dateutil"


def test_approve_takes_the_import_name_when_told(monkeypatch) -> None:
    fake = Node_()
    _install(monkeypatch, fake)
    assert main(["env", "approve", "python-dateutil", "--name", "dateutil"]) == 0
    assert fake.bodies[-1]["name"] == "dateutil"
    assert fake.bodies[-1]["dist"] == "python-dateutil"


def test_approve_refuses_something_already_approved(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_())
    assert main(["env", "approve", "pandas"]) == EXIT_ERROR
    assert "already an approved extra" in capsys.readouterr().err


def test_rm_reports_the_trees_it_broke(monkeypatch, capsys) -> None:
    _install(
        monkeypatch,
        Node_(
            broken=[
                {
                    "name": "signal",
                    "type": "Signal",
                    "origin": "private",
                    "requires": ["pandas"],
                }
            ]
        ),
    )
    assert main(["env", "rm", "pandas"]) == 0
    out = capsys.readouterr().out
    assert "no longer deployable" in out
    assert "private::Signal" in out


def test_a_write_says_when_a_restart_is_owed(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_(restart_required=True))
    assert main(["env", "add", "numpy"]) == 0
    assert "restart the STS container" in capsys.readouterr().out


class ImportNode_(Node_):
    """Answers ``/environment/import`` with a fixed preview."""

    def __init__(self, preview: dict[str, object], **over: object) -> None:
        super().__init__(**over)
        self.preview = preview

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/environment/import":
            self.paths.append(request.url.path)
            if request.content:
                self.bodies.append(json.loads(request.content))
            return httpx.Response(200, json=self.preview)
        return super().__call__(request)


def _row(name: str, **over: object) -> dict[str, object]:
    row: dict[str, object] = {
        "name": name,
        "version": "1.0",
        "dist": name,
        "status": "added",
        "guessed": False,
        "pinned": True,
        "local_version": None,
        "local_dist": None,
    }
    row.update(over)
    return row


def test_import_without_confirm_only_reads(monkeypatch, capsys) -> None:
    """The names come from another node. Installing on its say-so alone is
    how a typosquat reaches this node's trading sys.path."""
    fake = ImportNode_(
        {
            "added": [_row("numpy")],
            "kept": [],
            "conflicts": [],
            "guessed": [],
            "unpinned": [],
            "applied": False,
            "environment": None,
        }
    )
    _install(monkeypatch, fake)
    assert main(["env", "import", "http://peer.test"]) == 0
    assert fake.bodies[-1] == {"url": "http://peer.test"}
    out = capsys.readouterr().out
    assert "numpy" in out
    assert "--confirm" in out


def test_import_explains_a_withheld_pin(monkeypatch, capsys) -> None:
    fake = ImportNode_(
        {
            "added": [_row("numpy", pinned=False, version="")],
            "kept": [],
            "conflicts": [],
            "guessed": [],
            "unpinned": ["numpy"],
            "applied": False,
            "environment": None,
        }
    )
    _install(monkeypatch, fake)
    assert main(["env", "import", "http://peer.test"]) == 0
    out = capsys.readouterr().out
    assert "registry key" in out
    assert "--token" in out


def test_import_explains_a_guessed_dist(monkeypatch, capsys) -> None:
    fake = ImportNode_(
        {
            "added": [_row("sklearn", guessed=True)],
            "kept": [],
            "conflicts": [],
            "guessed": ["sklearn"],
            "unpinned": [],
            "applied": False,
            "environment": None,
        }
    )
    _install(monkeypatch, fake)
    assert main(["env", "import", "http://peer.test"]) == 0
    assert "--dist" in capsys.readouterr().out


def test_import_confirm_sends_the_dist_corrections(monkeypatch, capsys) -> None:
    fake = ImportNode_(
        {
            "added": [_row("sklearn", dist="scikit-learn")],
            "kept": [],
            "conflicts": [],
            "guessed": [],
            "unpinned": [],
            "applied": True,
            "environment": _environment(generation=2),
        }
    )
    _install(monkeypatch, fake)
    assert (
        main(
            [
                "env",
                "import",
                "http://peer.test",
                "--token",
                "mftik_rk_x",
                "--dist",
                "sklearn=scikit-learn",
                "--confirm",
            ]
        )
        == 0
    )
    assert fake.bodies[-1] == {
        "url": "http://peer.test",
        "token": "mftik_rk_x",
        "dist": {"sklearn": "scikit-learn"},
        "confirm": True,
    }
    assert "applied — generation 2" in capsys.readouterr().out


def test_a_malformed_dist_pair_is_refused(monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_())
    argv = ["env", "import", "http://peer.test", "--dist", "sklearn"]
    assert main(argv) == EXIT_ERROR
    assert "name=pypi-name" in capsys.readouterr().err


#: Declares numpy without importing it. ``check`` imports the tree, so a
#: strategy that really uses numpy can only be checked on a machine that has
#: it — ``--against`` is about the *node*, and that is a separate question.
_NUMPY_TREE = """\
from mftik.strategy import Strategy

class UsesNumpy(Strategy):
    name = "uses_numpy"
    requires = ("numpy",)
"""


def _tree(tmp_path: Path, source: str = _NUMPY_TREE) -> Path:
    dest = tmp_path / "signal"
    dest.mkdir()
    (dest / "strategy.py").write_text(source)
    return dest


def test_check_stays_offline_without_against(tmp_path, monkeypatch, capsys) -> None:
    """The gate is a local fact. Whether a node has numpy is not, and asking
    would make this command useless on a plane."""
    fake = Node_()
    _install(monkeypatch, fake)
    assert main(["check", str(_tree(tmp_path))]) == 0
    assert fake.paths == []
    assert "requires numpy" in capsys.readouterr().out


def test_check_against_names_what_the_node_lacks(tmp_path, monkeypatch, capsys) -> None:
    _install(monkeypatch, Node_(packages={}, installed=[]))
    assert main(["check", str(_tree(tmp_path)), "--against"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "not on that node" in err
    assert "mftik env add numpy" in err


def test_check_against_says_approve_when_it_is_already_there(
    tmp_path, monkeypatch, capsys
) -> None:
    """The refusal push would give, said before the files are sent — and it
    has to name the right fix, which here is approving, not installing."""
    _install(monkeypatch, Node_(packages={}))
    assert main(["check", str(_tree(tmp_path)), "--against"]) == EXIT_ERROR
    err = capsys.readouterr().err
    assert "as a dependency but not approved" in err
    assert "mftik env approve numpy" in err


def test_check_against_passes_when_the_node_has_it(
    tmp_path, monkeypatch, capsys
) -> None:
    _install(
        monkeypatch,
        Node_(
            packages={
                "numpy": {
                    "version": "2.1.3",
                    "dist": "numpy",
                    "source": "manual",
                }
            }
        ),
    )
    assert main(["check", str(_tree(tmp_path)), "--against"]) == 0
    assert "nothing missing" in capsys.readouterr().out


def test_check_against_refuses_a_node_whose_overlay_is_unusable(
    tmp_path, monkeypatch, capsys
) -> None:
    _install(monkeypatch, Node_(abi_ok=False))
    assert main(["check", str(_tree(tmp_path)), "--against"]) == EXIT_ERROR
    assert "different interpreter" in capsys.readouterr().err
