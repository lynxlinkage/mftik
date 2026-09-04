"""TapeKeeper — the strategy whose whole job is to keep a subscription alive.

There is almost nothing to test in what it does, which is the point. What is
worth testing is the one way it can fail: looking healthy while holding nothing.
"""

from __future__ import annotations

import pytest
from mftik.exchange.models import AggTrade, Side
from mftik.protocol import parse_strategy_yml, strategy_catalog
from mftik_sts.impl import resolve, resolve_class
from mftik_sts.impl.tape_keeper import TapeKeeper

TICKER = "BinanceUM_Perp_BTCUSDT"


class FakeSession:
    def __init__(self, md_ids: list[str]) -> None:
        self.td_api_ids: list[int] = []
        self.md_ids = md_ids
        self.failures: list[str] = []

    def request_exit(self, reason: str, *, failed: bool = False) -> None:
        if failed:
            self.failures.append(reason)

    def td_sole(self) -> int:
        ids = list(self.td_api_ids)
        if len(ids) != 1:
            raise RuntimeError(f"needs exactly one td account, got {ids}")
        return ids[0]


def _keeper(md_ids: list[str]) -> TapeKeeper:
    strat = TapeKeeper()
    strat.paras = TapeKeeper.on_initialized({})
    strat.session = FakeSession(md_ids)  # type: ignore[assignment]

    async def _noop_log(message: str, *, level: str = "info", **extra) -> None:
        return None

    strat.log = _noop_log  # type: ignore[method-assign]
    return strat


@pytest.mark.asyncio
async def test_holding_no_feeds_is_a_failure() -> None:
    """Nothing subscribed means nothing recorded, and nobody would notice."""
    strat = _keeper([])

    await strat.on_start()

    assert strat.session.failures
    assert "no md feeds" in strat.session.failures[0]


@pytest.mark.asyncio
async def test_holding_feeds_starts_cleanly() -> None:
    strat = _keeper([f"aggtrade.{TICKER}"])

    await strat.on_start()

    assert not strat.session.failures


@pytest.mark.asyncio
async def test_it_counts_prints_and_places_nothing() -> None:
    strat = _keeper([f"aggtrade.{TICKER}"])
    await strat.on_start()

    await strat.on_agg_trade(
        AggTrade(
            universal_ticker=TICKER,
            price=1,
            qty=1,
            side=Side.BUY,
        )
    )

    assert strat._prints == 1


def test_it_is_rebuildable() -> None:
    """The one strategy for which coming back really is starting."""
    assert TapeKeeper.rebuildable is True


def test_report_interval_must_be_positive() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        TapeKeeper.on_initialized({"report_interval_ms": 0})


def test_it_is_registered_and_catalogued() -> None:
    assert isinstance(resolve("tape_keeper"), TapeKeeper)
    assert resolve_class("TapeKeeper") is TapeKeeper
    template = strategy_catalog.get_template("TapeKeeper")
    assert template is not None
    # The template must not hand anyone an account: this strategy's safety is
    # that it structurally cannot trade, and a td entry would undo that.
    assert parse_strategy_yml(template.yaml).td == {}
