"""GET /sts/strategies/{id}/yaml — the strategy.yml behind a past deploy.

Two paths to cover: a deploy whose submitted text was kept, which must come
back byte for byte, and one from before the text was stored, where the
guarantee is only that the rebuild parses to the spec that *was* stored. The
repositories are stubbed: none of this needs a database.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException
from mft.protocol import parse_strategy_yml
from mft_api.routes import sts as sts_routes


class _FakeDb:
    pass


def _session_scope_stub():
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def scope() -> Any:
        yield _FakeDb()

    return scope


def _install(
    monkeypatch: pytest.MonkeyPatch,
    *,
    strategy: Any,
    accounts: dict[int, str],
) -> None:
    """Point the handler at canned rows instead of Postgres."""

    class FakeStrategyRepo:
        def __init__(self, _db: Any) -> None:
            pass

        async def get_with_session(self, strategy_id: int) -> Any:
            if strategy is None or strategy.id != strategy_id:
                return None
            return strategy

    class FakeAccountRepo:
        def __init__(self, _db: Any) -> None:
            pass

        async def get_by_api_id(self, api_id: int) -> Any:
            name = accounts.get(api_id)
            return None if name is None else SimpleNamespace(name=name)

    monkeypatch.setattr(sts_routes, "session_scope", _session_scope_stub())
    monkeypatch.setattr(sts_routes, "StrategyRepository", FakeStrategyRepo)
    monkeypatch.setattr(sts_routes, "AccountRepository", FakeAccountRepo)


def _strategy(
    *,
    id: int = 7,
    type: str = "NoopStrategy",
    config: dict[str, Any] | None = None,
    td_api_ids: list[int] | None = None,
    md_ids: list[str] | None = None,
    restart: str = "always",
    #: Default None — most cases here exercise the rebuild, which is what a
    #: row without stored text falls back to.
    yaml_text: str | None = None,
    session: Any = ...,
) -> SimpleNamespace:
    if session is ...:
        session = SimpleNamespace(
            td_api_ids=list(td_api_ids or []),
            md_ids=list(md_ids or []),
            restart=restart,
        )
    return SimpleNamespace(
        id=id,
        type=type,
        yaml_text=yaml_text,
        config=dict(config or {}),
        sts_session="sess-abc",
        session=session,
    )


SUBMITTED = """\
# how the desk runs this one
td:
  - paper trader      # the only live credential
md: [orderbook.Paper_Spot_BTCUSDT]

sts:
  gap_bps: 10
"""


async def test_submitted_document_comes_back_verbatim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(
        monkeypatch,
        strategy=_strategy(
            yaml_text=SUBMITTED,
            # Deliberately disagreeing with the text: nothing derived may be
            # allowed to leak into what is served back.
            config={"gap_bps": 999},
            td_api_ids=[3],
        ),
        accounts={3: "renamed since"},
    )

    result = await sts_routes.strategy_yaml(7)

    assert result.yaml == SUBMITTED
    assert result.reconstructed is False
    # No account lookup happened, so there is nothing to report unresolved.
    assert result.unresolved_td == []


async def test_rebuild_is_flagged_as_reconstructed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, strategy=_strategy(td_api_ids=[3]), accounts={3: "a"})

    result = await sts_routes.strategy_yaml(7)

    assert result.reconstructed is True


async def test_rebuild_keeps_the_restart_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, strategy=_strategy(restart="never"), accounts={})

    result = await sts_routes.strategy_yaml(7)

    # A one-shot rebuilt as ``always`` would come back as a document that
    # redeploys into a different run than the one it describes.
    assert parse_strategy_yml(result.yaml).restart == "never"


async def test_rebuilt_yaml_round_trips_to_the_stored_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(
        monkeypatch,
        strategy=_strategy(
            config={"exec_interval_ms": 1000, "gap_bps": 10},
            td_api_ids=[3],
            md_ids=["orderbook.Paper_Spot_BTCUSDT"],
        ),
        accounts={3: "paper trader"},
    )

    result = await sts_routes.strategy_yaml(7)

    assert result.id == 7
    # The type is reported alongside, not baked into the document.
    assert result.type == "NoopStrategy"
    assert result.sts_session == "sess-abc"
    assert result.unresolved_td == []

    spec = parse_strategy_yml(result.yaml)
    assert spec.td == ["paper trader"]
    assert spec.md == ["orderbook.Paper_Spot_BTCUSDT"]
    assert spec.sts == {"exec_interval_ms": 1000, "gap_bps": 10}


async def test_td_order_is_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(
        monkeypatch,
        strategy=_strategy(td_api_ids=[9, 2, 5]),
        accounts={2: "b", 5: "c", 9: "a"},
    )

    result = await sts_routes.strategy_yaml(7)

    # td is positional in strategy.yml; the rebuilt list must not be reordered.
    assert parse_strategy_yml(result.yaml).td == ["a", "b", "c"]


async def test_deleted_credential_becomes_a_flagged_placeholder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(
        monkeypatch,
        strategy=_strategy(td_api_ids=[3, 4]),
        accounts={3: "paper trader"},
    )

    result = await sts_routes.strategy_yaml(7)

    assert result.unresolved_td == [4]
    spec = parse_strategy_yml(result.yaml)
    # The document stays valid so it can still be read, but the entry names no
    # real account — redeploying it 404s rather than silently dropping the leg.
    assert spec.td == ["paper trader", "<deleted api_id=4>"]


async def test_missing_session_yields_an_empty_attach_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, strategy=_strategy(session=None), accounts={})

    result = await sts_routes.strategy_yaml(7)

    spec = parse_strategy_yml(result.yaml)
    assert spec.td == []
    assert spec.md == []
    assert result.type == "NoopStrategy"


async def test_unknown_strategy_is_404(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, strategy=None, accounts={})

    with pytest.raises(HTTPException) as exc:
        await sts_routes.strategy_yaml(999)

    assert exc.value.status_code == 404
    assert "strategy not found" in exc.value.detail
