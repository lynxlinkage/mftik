"""strategy.yml parse tests.

The document describes *where* a strategy runs (td / md) and *how* it is
configured (sts) — but not *which* strategy. That is chosen at deploy time, so
the two shapes that used to carry it are now errors with a message saying so.
``td`` is account name → settings, not a list.
"""

from __future__ import annotations

import pytest
from mftik.protocol import (
    StrategyYamlError,
    TdAccountRef,
    TdSettings,
    all_templates,
    attached_api_ids,
    default_template,
    dump_td,
    get_template,
    load_td,
    parse_strategy_yml,
    strategy_types,
    td_api_ids_of,
)


def test_parse_default_template() -> None:
    spec = parse_strategy_yml(default_template().yaml)
    assert spec.td == {"paper trader": TdSettings()}
    assert spec.md == ["orderbook.Paper_Spot_BTCUSDT"]
    # No mid: the strategy reads one from the order book feed above.
    assert "mid" not in spec.sts
    assert spec.sts["exec_interval_ms"] == 1000
    assert spec.sts["gap_bps"] == 10
    assert spec.sts["qty_quote"] == 100


#: Templates that deliberately attach to no trading account. Listing them here
#: rather than dropping the assertion keeps it doing its job for the strategies
#: that do trade: a missing ``td`` is a broken template for every one of those,
#: and it should stay a test failure rather than a session that deploys and
#: then cannot place the order it exists to place.
NO_ACCOUNT_TYPES = frozenset({"TapeKeeper"})


@pytest.mark.parametrize("type_name", strategy_types())
def test_every_template_parses(type_name: str) -> None:
    """A template that does not parse is a broken starting point for the UI."""
    template = get_template(type_name)
    assert template is not None
    spec = parse_strategy_yml(template.yaml)
    if type_name in NO_ACCOUNT_TYPES:
        assert not spec.td, (
            f"{type_name} is meant to hold feeds without an account; a td here "
            "would give it the power to trade that its whole design is to lack"
        )
    else:
        assert spec.td, f"{type_name} template has no td account"
    assert spec.md, f"{type_name} template has no md feed"
    assert spec.sts, f"{type_name} template has no sts config"


def test_templates_are_keyed_by_their_own_type() -> None:
    for template in all_templates():
        assert get_template(template.type) is template


def test_bundled_templates_are_marked_bundled() -> None:
    for template in all_templates():
        assert template.source == "bundled"


def test_null_settings_are_empty() -> None:
    spec = parse_strategy_yml(
        """
td:
  paper trader:
md: []
sts: {}
"""
    )
    assert spec.td == {"paper trader": TdSettings()}


def test_rejects_a_td_list() -> None:
    with pytest.raises(StrategyYamlError, match="mapping of account name") as caught:
        parse_strategy_yml(
            """
td: [paper trader]
md: []
sts: {}
"""
        )
    message = str(caught.value)
    assert message.startswith("td: ")
    assert "td: [paper trader]" in message
    assert "paper trader:" in message


def test_rejects_a_non_string_td_key() -> None:
    with pytest.raises(StrategyYamlError, match="must be a string") as caught:
        parse_strategy_yml(
            """
td:
  1234:
md: []
sts: {}
"""
        )
    assert "1234" in str(caught.value)


def test_rejects_an_empty_td_key() -> None:
    with pytest.raises(StrategyYamlError, match="non-empty"):
        parse_strategy_yml(
            """
td:
  "  ":
md: []
sts: {}
"""
        )


def test_rejects_duplicate_td_keys() -> None:
    with pytest.raises(StrategyYamlError, match="duplicate account name"):
        parse_strategy_yml(
            """
td:
  paper trader:
  paper trader:
md: []
sts: {}
"""
        )


def test_rejects_td_keys_that_collide_after_strip() -> None:
    """The event scan used to compare raw keys; strip happens later."""
    with pytest.raises(StrategyYamlError, match="duplicate account name"):
        parse_strategy_yml(
            """
td:
  paper trader:
  "paper trader ":
md: []
sts: {}
"""
        )


def test_rejects_duplicate_td_keys_when_the_value_is_an_alias() -> None:
    """``*anchor`` as a value used to desync the key scan."""
    with pytest.raises(StrategyYamlError, match="duplicate account name"):
        parse_strategy_yml(
            """
td:
  paper trader: &s {}
  binance quoter: *s
  paper trader: *s
md: []
sts: {}
"""
        )


def test_shared_td_settings_via_anchor_are_fine() -> None:
    spec = parse_strategy_yml(
        """
td:
  paper trader: &s {}
  binance quoter: *s
md: []
sts: {}
"""
    )
    assert set(spec.td) == {"paper trader", "binance quoter"}


def test_rejects_unknown_td_settings() -> None:
    with pytest.raises(StrategyYamlError, match="Extra inputs"):
        parse_strategy_yml(
            """
td:
  paper trader:
    leverage: 5
md: []
sts: {}
"""
        )


def test_rejects_bad_md_feed() -> None:
    with pytest.raises(StrategyYamlError, match="topic.UniversalTicker"):
        parse_strategy_yml(
            """
td: {}
md: [not-a-feed]
sts: {}
"""
        )


def test_a_type_in_the_document_is_refused_with_a_pointer() -> None:
    """The old shape must fail loudly, not deploy the wrong strategy."""
    with pytest.raises(StrategyYamlError, match="chosen at deploy time"):
        parse_strategy_yml(
            """
td: {}
md: []
sts:
  type: NoopStrategy
"""
        )


def test_a_nested_config_block_is_refused_with_a_pointer() -> None:
    with pytest.raises(StrategyYamlError, match="directly under sts"):
        parse_strategy_yml(
            """
td: {}
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
td: {}
md: []
"""
    )
    assert spec.sts == {}


def test_omitted_td_is_empty() -> None:
    spec = parse_strategy_yml("md: []\nsts: {}\n")
    assert spec.td == {}


def test_sts_must_be_a_mapping() -> None:
    with pytest.raises(StrategyYamlError, match="mapping"):
        parse_strategy_yml(
            """
td: {}
md: []
sts: [1, 2]
"""
        )


def test_restart_defaults_to_always() -> None:
    """Two gates already stand in front of a rebuild — the operator enabling
    it and the strategy class supporting it. A deploy that reaches this
    question is one whose run was cut short and would rather continue.
    """
    spec = parse_strategy_yml("td: {}\nmd: []\nsts: {}\n")
    assert spec.restart == "always"


def test_restart_never_is_kept() -> None:
    spec = parse_strategy_yml("td: {}\nmd: []\nrestart: never\nsts: {}\n")
    assert spec.restart == "never"


def test_an_unknown_restart_mode_is_refused() -> None:
    """Silently treating a typo as `always` would resume a run that asked not
    to be, which is the one direction this must not fail in."""
    with pytest.raises(StrategyYamlError, match="restart must be one of"):
        parse_strategy_yml("td: {}\nmd: []\nrestart: maybe\nsts: {}\n")


def test_a_refusal_reads_as_a_sentence_about_the_field() -> None:
    """``str(ValidationError)`` is written for whoever is debugging the model.

    This text is not for them. It reaches the editor as a 400 detail and the
    terminal as ``mftik check`` output, so what has to survive is the field
    and the sentence the validator raised — not a leading count, a repeat of
    the class name, an echo of the input, a type tag and a docs URL.
    """
    with pytest.raises(StrategyYamlError) as caught:
        parse_strategy_yml("td: {}\nmd: ['bestquote.NotATicker']\nsts: {}\n")

    message = str(caught.value)
    assert message.startswith("md: ")
    assert "topic.UniversalTicker" in message
    for noise in (
        "validation error",
        "StrategySpec",
        "[type=",
        "input_value=",
        "pydantic.dev",
        "Value error, ",
    ):
        assert noise not in message, noise


def test_every_bad_field_gets_its_own_line() -> None:
    """One round trip should not have to be spent finding the second mistake."""
    with pytest.raises(StrategyYamlError) as caught:
        parse_strategy_yml("td: [x]\nmd: ['nope']\nsts: {}\n")

    lines = str(caught.value).splitlines()
    # The list-hint spans several lines; the field names still both appear.
    assert any(line.startswith("td:") for line in lines)
    assert any(line.startswith("md:") for line in lines)


def test_load_td_round_trips_named_refs() -> None:
    td = {
        "paper trader": TdAccountRef(api_id=3),
        "binance quoter": TdAccountRef(api_id=7),
    }
    loaded = load_td(dump_td(td))
    assert list(loaded) == ["paper trader", "binance quoter"]
    assert loaded["paper trader"].api_id == 3
    assert td_api_ids_of(loaded) == [3, 7]


def test_attached_api_ids_reads_mapping_or_legacy_list() -> None:
    from types import SimpleNamespace

    assert attached_api_ids(
        SimpleNamespace(td={"paper trader": {"api_id": 3}})
    ) == [3]
    assert attached_api_ids(SimpleNamespace(td_api_ids=[3, 7])) == [3, 7]
