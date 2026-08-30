"""``mftik init`` — a scaffold that runs as generated.

The template is not the interesting part; the ``strategy.yml`` is. One
carrying a placeholder account has to be corrected before it does
anything, and correcting it means finding out what this node calls its
accounts and which instruments its symbol plane knows. So these check that
what gets written is real, and that when it cannot be, the file says so
instead of looking finished.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from mftik.cli import client as client_module
from mftik.cli import config
from mftik.cli.app import EXIT_ERROR, main
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
        accounts: list[tuple[str, str]] | None = None,
        tickers: list[str] | None = None,
    ) -> None:
        self.accounts = (
            [("paper trader", "Paper")] if accounts is None else accounts
        )
        self.tickers = (
            ["Paper_Spot_BTCUSDT", "Paper_Spot_ETHUSDT"]
            if tickers is None
            else tickers
        )
        self.venue_asked: str | None = None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/apis":
            return httpx.Response(
                200,
                json={
                    "apis": [
                        {
                            "id": i + 1,
                            "account_id": i + 1,
                            "name": name,
                            "venue": venue,
                            "api_key": "k",
                            "type": "HMAC",
                            "created_at": 0.0,
                            "created_by": 1,
                        }
                        for i, (name, venue) in enumerate(self.accounts)
                    ]
                },
            )
        if request.url.path == "/sym/symbols":
            self.venue_asked = request.url.params.get("venue")
            return httpx.Response(
                200,
                json={
                    "symbols": [
                        {
                            "universal_ticker": t,
                            "base": "BTC",
                            "quote": "USDT",
                            "exch_ticker": "BTCUSDT",
                        }
                        for t in self.tickers
                    ],
                    "total": len(self.tickers),
                },
            )
        raise AssertionError(f"unexpected path: {request.url.path}")


@pytest.fixture
def a_node(monkeypatch):
    def install(node: Node_) -> Node_:
        def build(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
            kwargs.pop("transport", None)
            return _REAL_HTTPX(*args, transport=httpx.MockTransport(node), **kwargs)

        monkeypatch.setattr(client_module.httpx, "Client", build)
        return node

    return install


def _yml(root: Path) -> str:
    return (root / "strategy.yml").read_text()


# --- what it writes --------------------------------------------------------


def test_the_document_names_a_real_account_and_instrument(
    tmp_path: Path, a_node
) -> None:
    node = a_node(Node_())
    root = tmp_path / "hello"

    assert main(["init", str(root)]) == 0

    body = _yml(root)
    assert '"paper trader":' in body
    assert '"orderbook.Paper_Spot_BTCUSDT"' in body
    # The instruments asked for are the ones that account can trade.
    assert node.venue_asked == "Paper"


def test_the_scaffold_passes_its_own_check(tmp_path: Path, a_node, capsys) -> None:
    """A template `mftik check` refuses is a template that teaches the wrong thing."""
    a_node(Node_())
    root = tmp_path / "hello"
    main(["init", str(root)])
    capsys.readouterr()

    assert main(["check", str(root)]) == 0
    assert "accepted by on_initialized" in capsys.readouterr().out


def test_the_directory_name_becomes_a_legal_strategy_name(
    tmp_path: Path, a_node
) -> None:
    """The registry wants [a-z][a-z0-9_]*; a directory is not asked to be one."""
    a_node(Node_())
    root = tmp_path / "My-First Strategy"

    assert main(["init", str(root)]) == 0

    source = (root / "strategy.py").read_text()
    assert 'name = "my_first_strategy"' in source
    assert "class MyFirstStrategy(Strategy):" in source


def test_the_name_can_be_given(tmp_path: Path, a_node) -> None:
    a_node(Node_())
    root = tmp_path / "whatever"

    main(["init", str(root), "--name", "macd_dollar"])

    assert 'name = "macd_dollar"' in (root / "strategy.py").read_text()


def test_btcusdt_is_preferred_so_a_first_run_is_legible(
    tmp_path: Path, a_node
) -> None:
    a_node(Node_(tickers=["Paper_Spot_ZZZUSDT", "Paper_Spot_BTCUSDT"]))
    root = tmp_path / "hello"

    main(["init", str(root)])

    assert "orderbook.Paper_Spot_BTCUSDT" in _yml(root)


def test_orderbook_is_the_topic_every_venue_serves(tmp_path: Path, a_node) -> None:
    """paper serves neither ticker nor bestquote, and paper is what a new node has."""
    a_node(Node_())
    root = tmp_path / "hello"

    main(["init", str(root)])

    assert _yml(root).count("orderbook.") == 1
    for absent in ("bestquote.", "ticker.", "aggtrade."):
        assert absent not in _yml(root)


# --- when there is nothing real to write -----------------------------------


def test_offline_says_the_document_is_not_finished(
    tmp_path: Path, capsys
) -> None:
    """Placeholders that looked finished would be worse than no document."""
    root = tmp_path / "hello"

    assert main(["init", str(root), "--offline"]) == 0

    out = capsys.readouterr().out
    assert "may not exist" in out
    assert "no account on this node" in _yml(root)


def test_a_node_with_no_accounts_says_so(tmp_path: Path, a_node, capsys) -> None:
    a_node(Node_(accounts=[]))
    root = tmp_path / "hello"

    assert main(["init", str(root)]) == 0

    assert "no trading accounts" in capsys.readouterr().out
    assert "no account on this node" in _yml(root)


def test_a_venue_with_no_instruments_keeps_the_real_account(
    tmp_path: Path, a_node, capsys
) -> None:
    """Half known is still worth writing; the half that is a guess is named."""
    a_node(Node_(tickers=[]))
    root = tmp_path / "hello"

    assert main(["init", str(root)]) == 0

    out = capsys.readouterr().out
    assert "knows no instruments" in out
    assert '"paper trader":' in _yml(root)


# --- not clobbering anything ----------------------------------------------


def test_existing_files_are_not_overwritten(tmp_path: Path, a_node, capsys) -> None:
    a_node(Node_())
    root = tmp_path / "hello"
    root.mkdir()
    (root / "strategy.py").write_text("# mine\n")

    assert main(["init", str(root)]) == EXIT_ERROR

    assert (root / "strategy.py").read_text() == "# mine\n"
    assert "--force" in capsys.readouterr().err


def test_force_overwrites(tmp_path: Path, a_node) -> None:
    a_node(Node_())
    root = tmp_path / "hello"
    root.mkdir()
    (root / "strategy.py").write_text("# mine\n")

    assert main(["init", str(root), "--force"]) == 0

    assert "class Hello(Strategy):" in (root / "strategy.py").read_text()
