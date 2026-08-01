"""strategy.yml parse / dump tests."""

from __future__ import annotations

import pytest
from mft.protocol import (
    DEFAULT_STRATEGY_YML,
    StrategyYamlError,
    dump_strategy_yml,
    parse_strategy_yml,
)


def test_parse_default_template() -> None:
    spec = parse_strategy_yml(DEFAULT_STRATEGY_YML)
    assert spec.td == ["paper trader"]
    assert spec.md == ["paper.orderbook.BTCUSDT"]
    assert spec.sts.type == "NoopStrategy"
    assert spec.sts.config["mid"] == 50000


def test_roundtrip_dump() -> None:
    spec = parse_strategy_yml(DEFAULT_STRATEGY_YML)
    again = parse_strategy_yml(dump_strategy_yml(spec))
    assert again == spec


def test_rejects_bad_md_feed() -> None:
    with pytest.raises(StrategyYamlError, match="venue.topic.symbol"):
        parse_strategy_yml(
            """
td: [paper trader]
md: [not-a-feed]
sts:
  type: NoopStrategy
  config: {}
"""
        )


def test_rejects_td_api_id() -> None:
    with pytest.raises(StrategyYamlError, match="account name"):
        parse_strategy_yml(
            """
td: [1]
md: []
sts:
  type: NoopStrategy
  config: {}
"""
        )


def test_rejects_duplicate_td_name() -> None:
    with pytest.raises(StrategyYamlError, match="duplicate"):
        parse_strategy_yml(
            """
td:
  - paper trader
  - paper trader
md: []
sts:
  type: NoopStrategy
"""
        )


def test_rejects_missing_sts() -> None:
    with pytest.raises(StrategyYamlError):
        parse_strategy_yml(
            """
td: [paper trader]
md: []
"""
        )


def test_config_defaults_empty() -> None:
    spec = parse_strategy_yml(
        """
td: []
md: []
sts:
  type: NoopStrategy
"""
    )
    assert spec.sts.config == {}
