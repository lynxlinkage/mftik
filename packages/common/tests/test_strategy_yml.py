"""strategy.yml parse / dump tests.

The document describes *where* a strategy runs (td / md) and *how* it is
configured (sts) — but not *which* strategy. That is chosen at deploy time, so
the two shapes that used to carry it are now errors with a message saying so.
"""

from __future__ import annotations

import pytest
from mft.protocol import (
    StrategyYamlError,
    all_templates,
    default_template,
    dump_strategy_yml,
    get_template,
    parse_strategy_yml,
    strategy_types,
)


def test_parse_default_template() -> None:
    spec = parse_strategy_yml(default_template().yaml)
    assert spec.td == ["paper trader"]
    assert spec.md == ["orderbook.Paper_Spot_BTCUSDT"]
    # No mid: the strategy reads one from the order book feed above.
    assert "mid" not in spec.sts
    assert spec.sts["exec_interval_ms"] == 1000
    assert spec.sts["gap_bps"] == 10
    assert spec.sts["qty_quote"] == 100


@pytest.mark.parametrize("type_name", strategy_types())
def test_every_template_parses(type_name: str) -> None:
    """A template that does not parse is a broken starting point for the UI."""
    template = get_template(type_name)
    assert template is not None
    spec = parse_strategy_yml(template.yaml)
    assert spec.td, f"{type_name} template has no td account"
    assert spec.md, f"{type_name} template has no md feed"
    assert spec.sts, f"{type_name} template has no sts config"


def test_templates_are_keyed_by_their_own_type() -> None:
    for template in all_templates():
        assert get_template(template.type) is template


def test_roundtrip_dump() -> None:
    spec = parse_strategy_yml(default_template().yaml)
    again = parse_strategy_yml(dump_strategy_yml(spec))
    assert again == spec


def test_rejects_bad_md_feed() -> None:
    with pytest.raises(StrategyYamlError, match="topic.UniversalTicker"):
        parse_strategy_yml(
            """
td: [paper trader]
md: [not-a-feed]
sts: {}
"""
        )


def test_rejects_td_api_id() -> None:
    with pytest.raises(StrategyYamlError, match="account name"):
        parse_strategy_yml(
            """
td: [1]
md: []
sts: {}
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
sts: {}
"""
        )


def test_a_type_in_the_document_is_refused_with_a_pointer() -> None:
    """The old shape must fail loudly, not deploy the wrong strategy."""
    with pytest.raises(StrategyYamlError, match="chosen at deploy time"):
        parse_strategy_yml(
            """
td: [paper trader]
md: []
sts:
  type: NoopStrategy
"""
        )


def test_a_nested_config_block_is_refused_with_a_pointer() -> None:
    with pytest.raises(StrategyYamlError, match="directly under sts"):
        parse_strategy_yml(
            """
td: [paper trader]
md: []
sts:
  config:
    gap_bps: 10
"""
        )


def test_sts_may_be_omitted_entirely() -> None:
    """A strategy with no parameters still deploys."""
    spec = parse_strategy_yml(
        """
td: []
md: []
"""
    )
    assert spec.sts == {}


def test_sts_must_be_a_mapping() -> None:
    with pytest.raises(StrategyYamlError, match="mapping"):
        parse_strategy_yml(
            """
td: []
md: []
sts: [1, 2]
"""
        )


def test_restart_defaults_to_always() -> None:
    """Two gates already stand in front of a rebuild — the operator enabling
    it and the strategy class supporting it. A deploy that reaches this
    question is one whose run was cut short and would rather continue.
    """
    spec = parse_strategy_yml("td: []\nmd: []\nsts: {}\n")
    assert spec.restart == "always"


def test_restart_never_is_kept_through_a_round_trip() -> None:
    spec = parse_strategy_yml("td: []\nmd: []\nrestart: never\nsts: {}\n")
    assert spec.restart == "never"
    assert parse_strategy_yml(dump_strategy_yml(spec)).restart == "never"


def test_an_unknown_restart_mode_is_refused() -> None:
    """Silently treating a typo as `always` would resume a run that asked not
    to be, which is the one direction this must not fail in."""
    with pytest.raises(StrategyYamlError, match="restart must be one of"):
        parse_strategy_yml("td: []\nmd: []\nrestart: maybe\nsts: {}\n")
