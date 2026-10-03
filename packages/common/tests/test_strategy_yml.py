"""strategy.yml parse tests.

The document describes *where* a strategy runs (td / md), *how* it is
configured (sts), and what the platform is to do with the run — restart policy,
start and readiness deadlines, offload and memory limits. It does not say
*which* strategy. That is chosen at deploy time, so the two shapes that used to
carry it are now errors with a message saying so. ``td`` is account name →
settings, not a list.
"""

from __future__ import annotations

import pytest
from mftik.protocol import (
    ANY_INSTANCE,
    DEFAULT_MAX_RESTARTS,
    DEFAULT_OFFLOAD_PROCESSES,
    DEFAULT_OFFLOAD_THREADS,
    DEFAULT_READY_TIMEOUT_S,
    DEFAULT_RESTART_WINDOW_S,
    DEFAULT_START_TIMEOUT_S,
    MAX_START_TIMEOUT_S,
    RESTART_NEVER,
    RESTART_ON_FAILURE,
    OptionChainSelect,
    RollingFutureSelect,
    StrategySpec,
    StrategyYamlError,
    TdAccountRef,
    TdSettings,
    all_templates,
    attached_api_ids,
    default_template,
    dump_td,
    get_template,
    load_td,
    md_feeds_of,
    md_instances_of,
    md_selects_of,
    parse_strategy_yml,
    strategy_types,
    td_api_ids_of,
)


def test_parse_default_template() -> None:
    spec = parse_strategy_yml(default_template().yaml)
    assert spec.td == {"paper trader": TdSettings()}
    # A plain list under `md:` is every feed, unpinned — which is what the
    # bundled templates say and will keep saying: naming an instance is a
    # deployment's business, and a template is where somebody starts.
    assert spec.md == {ANY_INSTANCE: ["orderbook.Paper_Spot_BTCUSDT"]}
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


def test_rejects_a_merge_key_under_td() -> None:
    """``<<`` folds an anchored account in, and an explicit key wins over it.

    The settings that vanish are the merged ones, and nothing reports it.
    """
    with pytest.raises(StrategyYamlError, match="merge keys"):
        parse_strategy_yml(
            """
sts:
  base: &b
    paper trader:
      unknown_setting: 1
td:
  <<: *b
  paper trader:
md: []
"""
        )


def test_rejects_a_merge_key_under_td_even_alone() -> None:
    """Nothing to collide with yet, but the next edited line is the collision."""
    with pytest.raises(StrategyYamlError, match="merge keys"):
        parse_strategy_yml(
            """
sts:
  base: &b
    paper trader:
td:
  <<: *b
md: []
"""
        )


def test_a_merge_key_elsewhere_is_not_td_business() -> None:
    """Only ``td:`` is scanned — ``sts`` is the strategy's own bag."""
    spec = parse_strategy_yml(
        """
sts:
  base: &b
    x: 1
  more:
    <<: *b
td:
  paper trader:
md: []
"""
    )
    assert set(spec.td) == {"paper trader"}
    assert spec.sts["more"] == {"x": 1}


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


def test_restart_defaults_to_never() -> None:
    """A run that ended is over unless the document asked for otherwise."""
    spec = parse_strategy_yml("td: {}\nmd: []\nsts: {}\n")
    assert spec.restart == RESTART_NEVER


def test_restart_never_is_kept() -> None:
    spec = parse_strategy_yml("td: {}\nmd: []\nrestart: never\nsts: {}\n")
    assert spec.restart == RESTART_NEVER


def test_restart_on_failure_is_accepted_with_its_budget() -> None:
    """The one mode that puts a session back, and the two numbers that stop it
    putting the same session back forever."""
    spec = parse_strategy_yml(
        "td: {}\nmd: []\nrestart: on_failure\n"
        "max_restarts: 3\nrestart_window_s: 120\nsts: {}\n"
    )
    assert spec.restart == RESTART_ON_FAILURE
    assert spec.max_restarts == 3
    assert spec.restart_window_s == 120


def test_the_restart_budget_has_defaults() -> None:
    spec = parse_strategy_yml("td: {}\nmd: []\nrestart: on_failure\nsts: {}\n")
    assert spec.max_restarts == DEFAULT_MAX_RESTARTS
    assert spec.restart_window_s == DEFAULT_RESTART_WINDOW_S


def test_a_restart_budget_under_never_is_not_an_error() -> None:
    """It has no effect, and that is not the same as being wrong: a document
    may carry the policy it would use before it turns restarting on."""
    spec = parse_strategy_yml(
        "td: {}\nmd: []\nrestart: never\nmax_restarts: 2\nsts: {}\n"
    )
    assert spec.restart == RESTART_NEVER
    assert spec.max_restarts == 2


def test_max_restarts_may_be_zero_but_not_negative() -> None:
    """Zero is a policy — restart nothing — and a negative number is a typo."""
    assert parse_strategy_yml("md: []\nmax_restarts: 0\n").max_restarts == 0
    with pytest.raises(StrategyYamlError, match="max_restarts: must be at least 0"):
        parse_strategy_yml("md: []\nmax_restarts: -1\n")


def test_a_restart_window_of_zero_is_refused() -> None:
    """A window of nothing holds no restarts, so the budget it bounds could
    never be spent and `max_restarts` would silently mean one."""
    with pytest.raises(
        StrategyYamlError, match="restart_window_s: must be at least 1"
    ):
        parse_strategy_yml("md: []\nrestart_window_s: 0\n")


def test_the_old_always_is_refused_and_points_at_on_failure() -> None:
    """A document written for rebuild is still on somebody's disk. Refusing it
    as an unknown mode would read as a typo; what happened is that the mode it
    names was removed, and what replaced it does something else — a fresh run,
    not the interrupted one carried on."""
    with pytest.raises(StrategyYamlError) as caught:
        parse_strategy_yml("td: {}\nmd: []\nrestart: always\nsts: {}\n")

    message = str(caught.value)
    assert message.startswith("restart: always is gone.")
    assert "on_failure" in message
    assert "inherits none of it" in message
    assert "delete the line for the default, never" in message


def test_an_unknown_restart_mode_is_refused() -> None:
    """Silently treating a typo as a mode would run a deploy under a policy
    nobody asked for, which is the one direction this must not fail in."""
    with pytest.raises(StrategyYamlError, match="restart must be one of"):
        parse_strategy_yml("td: {}\nmd: []\nrestart: maybe\nsts: {}\n")


def test_the_two_timeouts_have_defaults() -> None:
    spec = parse_strategy_yml("td: {}\nmd: []\nsts: {}\n")
    assert spec.start_timeout_s == DEFAULT_START_TIMEOUT_S
    assert spec.ready_timeout_s == DEFAULT_READY_TIMEOUT_S


def test_a_long_warm_up_may_raise_the_start_timeout() -> None:
    """``on_start`` is where a model is loaded and a tape is read, and the
    document is the only thing that knows how long that takes."""
    spec = parse_strategy_yml("md: []\nstart_timeout_s: 1800\nready_timeout_s: 90\n")
    assert spec.start_timeout_s == 1800
    assert spec.ready_timeout_s == 90


def test_a_start_timeout_past_the_cap_is_refused_with_the_cap() -> None:
    with pytest.raises(StrategyYamlError) as caught:
        parse_strategy_yml(f"md: []\nstart_timeout_s: {MAX_START_TIMEOUT_S + 1}\n")
    assert str(caught.value) == (
        f"start_timeout_s: must be between 1 and {MAX_START_TIMEOUT_S}, "
        f"got {MAX_START_TIMEOUT_S + 1}"
    )


def test_a_timeout_written_as_a_duration_is_refused() -> None:
    """The field name ends in ``_s``; ``1m`` under it is somebody expecting the
    duration spelling that ``min_tte`` and ``roll_before`` take."""
    with pytest.raises(
        StrategyYamlError, match="start_timeout_s: must be a whole number"
    ):
        parse_strategy_yml("md: []\nstart_timeout_s: 1m\n")


def test_a_timeout_of_zero_is_refused() -> None:
    """Zero means the hook is over before it is called, which is not a deploy
    anybody wants and is reached by deleting a digit."""
    with pytest.raises(StrategyYamlError, match="ready_timeout_s: must be at least 1"):
        parse_strategy_yml("md: []\nready_timeout_s: 0\n")


def test_limits_default_to_the_offload_pool_sizes_and_no_memory_ceiling() -> None:
    """A memory limit nobody chose is a session killed at a number nobody
    chose, so the default is no limit at all."""
    limits = parse_strategy_yml("td: {}\nmd: []\nsts: {}\n").limits
    assert limits.offload_threads == DEFAULT_OFFLOAD_THREADS
    assert limits.offload_processes == DEFAULT_OFFLOAD_PROCESSES
    assert limits.memory_mb is None
    assert limits.offload_memory_mb is None


def test_limits_are_read() -> None:
    spec = parse_strategy_yml(
        """
md: []
limits:
  memory_mb: 2048
  offload_threads: 4
  offload_processes: 2
  offload_memory_mb: 512
"""
    )
    assert spec.limits.memory_mb == 2048
    assert spec.limits.offload_threads == 4
    assert spec.limits.offload_processes == 2
    assert spec.limits.offload_memory_mb == 512


def test_a_pool_of_no_workers_is_refused() -> None:
    """Not read as "offload is off": the first ``await self.offload(...)`` on a
    pool with no workers never returns."""
    with pytest.raises(
        StrategyYamlError, match="limits.offload_threads: must be at least 1"
    ):
        parse_strategy_yml("md: []\nlimits:\n  offload_threads: 0\n")


def test_an_unknown_limit_is_refused() -> None:
    """A ceiling that was dropped rather than applied is the whole reason this
    block is not a free-form bag."""
    with pytest.raises(StrategyYamlError, match="limits.cpu_millicores"):
        parse_strategy_yml("md: []\nlimits:\n  cpu_millicores: 500\n")


def test_limits_must_be_a_mapping() -> None:
    with pytest.raises(StrategyYamlError, match="limits must be a mapping"):
        parse_strategy_yml("md: []\nlimits: 2048\n")


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


#: The ``md:`` block of ARCHITECTURE_CHANGE_PLAN §6.4 under the smallest
#: document that can hold it, copied rather than paraphrased: it is the worked
#: example the selector design was written against — comments, inline mappings,
#: duration spellings and all — and a schema that cannot read the document the
#: plan shows is a schema that disagrees with the plan. Only the alignment of
#: two trailing comments is changed, to fit the line length.
PLAN_6_4 = """\
td: {}
md:
  md-jp:
    - ticker.Deribit_Perp_BTCUSD
    - select: btc_chain
      kind: option_chain
      venue: Deribit
      underlying: BTC
      ref: ticker.Deribit_Perp_BTCUSD  # 參考價；由 selector 自己持有，策略不必另外宣告
      expiries: {nearest: 2, min_tte: 2h}
      strikes: {atm: 5}  # 每個 expiry 取 ATM ± 5 檔，用該 expiry 實際掛牌的 strike
      sides: [C, P]
      topics: [ticker, greeks]
      recenter: {strikes: 1, min_dwell: 60s}
    - select: btc_q
      kind: rolling_future
      venue: Deribit
      underlying: BTC
      tenor: quarterly                  # weekly | monthly | quarterly
      roll_before: 3d
      topics: [ticker, trade]
sts: {}
"""


def test_the_plans_example_parses() -> None:
    spec = parse_strategy_yml(PLAN_6_4)

    chain, future = md_selects_of(spec.md_select)
    assert isinstance(chain, OptionChainSelect)
    assert chain.name == "btc_chain"
    assert chain.venue == "Deribit"
    assert chain.underlying == "BTC"
    assert chain.ref == "ticker.Deribit_Perp_BTCUSD"
    assert (chain.expiries.nearest, chain.expiries.min_tte_s) == (2, 2 * 3600)
    assert chain.strikes.atm == 5
    assert chain.sides == ("C", "P")
    assert chain.topics == ("ticker", "greeks")
    assert (chain.recenter.strikes, chain.recenter.min_dwell_s) == (1, 60)

    assert isinstance(future, RollingFutureSelect)
    assert future.name == "btc_q"
    assert future.tenor == "quarterly"
    assert future.roll_before_s == 3 * 86400
    assert future.topics == ("ticker", "trade")


def test_a_select_is_not_a_feed_and_the_instance_still_holds_it() -> None:
    """``md`` stays instance → feed keys, because that is what every reader of
    it expects. The selector's instance has to survive anyway: a set of
    instruments MD derives still has to be derived by *an* MD."""
    spec = parse_strategy_yml(PLAN_6_4)

    assert spec.md == {"md-jp": ["ticker.Deribit_Perp_BTCUSD"]}
    assert md_feeds_of(spec.md) == ["ticker.Deribit_Perp_BTCUSD"]
    assert md_instances_of(spec.md) == ["md-jp"]
    assert [s.name for s in spec.md_select["md-jp"]] == ["btc_chain", "btc_q"]


def test_a_select_only_instance_is_still_named() -> None:
    spec = parse_strategy_yml(
        """
md:
  md-jp:
    - select: btc_q
      kind: rolling_future
      venue: Deribit
      underlying: BTC
      tenor: weekly
      topics: [ticker]
"""
    )
    assert spec.md == {"md-jp": []}
    assert md_instances_of(spec.md) == ["md-jp"]
    assert [s.name for s in md_selects_of(spec.md_select)] == ["btc_q"]


def test_an_unpinned_select_lands_under_any_instance() -> None:
    """The plain list form says "any MD", and a selector written in it says the
    same thing about the MD that will derive it."""
    spec = parse_strategy_yml(
        """
md:
  - select: btc_q
    kind: rolling_future
    venue: Deribit
    underlying: BTC
    tenor: monthly
    topics: [ticker]
"""
    )
    assert list(spec.md_select) == [ANY_INSTANCE]
    assert spec.md == {ANY_INSTANCE: []}


def test_a_selects_venue_underlying_and_ref_are_normalized() -> None:
    """Same reason a feed key is: this is YAML a person typed, and two
    spellings of one board would derive two universes."""
    spec = parse_strategy_yml(
        """
md:
  - select: btc_q
    kind: rolling_future
    venue: deribit
    underlying: btc
    tenor: QUARTERLY
    topics: [ticker]
  - select: btc_chain
    kind: option_chain
    venue: DERIBIT
    underlying: BTC
    ref: ticker.deribit_perp_btcusd
    expiries: {nearest: 1}
    strikes: {atm: 1}
    topics: [ticker]
"""
    )
    future, chain = md_selects_of(spec.md_select)
    assert (future.venue, future.underlying, future.tenor) == (
        "Deribit",
        "BTC",
        "quarterly",
    )
    assert chain.ref == "ticker.Deribit_Perp_BTCUSD"


def test_the_optional_halves_of_an_option_chain_have_defaults() -> None:
    """``min_tte`` absent is no skipping, both sides is a chain rather than a
    directional bet, and ``recenter`` absent is the plan's own example."""
    spec = parse_strategy_yml(
        """
md:
  - select: btc_chain
    kind: option_chain
    venue: Deribit
    underlying: BTC
    ref: ticker.Deribit_Perp_BTCUSD
    expiries: {nearest: 1}
    strikes: {atm: 0}
    topics: [ticker]
"""
    )
    chain = md_selects_of(spec.md_select)[0]
    assert chain.expiries.min_tte_s == 0
    assert chain.sides == ("C", "P")
    assert (chain.recenter.strikes, chain.recenter.min_dwell_s) == (1, 60)


def test_an_unknown_select_kind_is_refused() -> None:
    with pytest.raises(StrategyYamlError, match="kind must be one of"):
        parse_strategy_yml(
            "md:\n  - select: btc\n    kind: whole_board\n    venue: Deribit\n"
        )


def test_an_unknown_venue_in_a_select_is_refused_here() -> None:
    """A selector that derives nothing looks exactly like a board with nothing
    on it, so a venue typo would surface as an empty universe hours later."""
    with pytest.raises(StrategyYamlError, match="unknown venue"):
        parse_strategy_yml(
            """
md:
  - select: btc_q
    kind: rolling_future
    venue: Derbit
    underlying: BTC
    tenor: weekly
    topics: [ticker]
"""
        )


def test_an_unknown_tenor_is_refused_with_the_three_there_are() -> None:
    with pytest.raises(StrategyYamlError, match="tenor must be one of"):
        parse_strategy_yml(
            """
md:
  - select: btc_q
    kind: rolling_future
    venue: Deribit
    underlying: BTC
    tenor: biweekly
    topics: [ticker]
"""
        )


def test_a_key_the_kind_does_not_take_is_refused_by_name() -> None:
    """``expiries`` under a rolling future is somebody who meant an option
    chain. Ignoring it would derive one contract and say nothing."""
    with pytest.raises(StrategyYamlError) as caught:
        parse_strategy_yml(
            """
md:
  - select: btc_q
    kind: rolling_future
    venue: Deribit
    underlying: BTC
    tenor: weekly
    topics: [ticker]
    expiries: {nearest: 2}
"""
        )
    message = str(caught.value)
    assert "rolling_future does not take 'expiries'" in message
    assert (
        "it takes ['roll_before', 'tenor', 'topics', 'underlying', 'venue']"
        in message
    )


def test_a_missing_half_of_an_option_chain_is_named() -> None:
    with pytest.raises(StrategyYamlError, match="strikes is required"):
        parse_strategy_yml(
            """
md:
  - select: btc_chain
    kind: option_chain
    venue: Deribit
    underlying: BTC
    ref: ticker.Deribit_Perp_BTCUSD
    expiries: {nearest: 1}
    topics: [ticker]
"""
        )


def test_an_expiry_count_of_zero_is_refused() -> None:
    """A chain over no expiries is a chain over nothing, which deploys, derives
    an empty universe and waits."""
    with pytest.raises(StrategyYamlError, match="expiries.nearest must be at least 1"):
        parse_strategy_yml(
            """
md:
  - select: btc_chain
    kind: option_chain
    venue: Deribit
    underlying: BTC
    ref: ticker.Deribit_Perp_BTCUSD
    expiries: {nearest: 0}
    strikes: {atm: 1}
    topics: [ticker]
"""
        )


def test_a_duration_that_is_not_one_is_refused_with_the_spellings() -> None:
    with pytest.raises(StrategyYamlError) as caught:
        parse_strategy_yml(
            """
md:
  - select: btc_q
    kind: rolling_future
    venue: Deribit
    underlying: BTC
    tenor: weekly
    topics: [ticker]
    roll_before: 3 days
"""
        )
    message = str(caught.value)
    assert "roll_before must be a duration like '2h', '30m', '90s', '3d'" in message
    assert "3 days" in message


def test_a_duration_may_also_be_a_bare_number_of_seconds() -> None:
    spec = parse_strategy_yml(
        """
md:
  - select: btc_q
    kind: rolling_future
    venue: Deribit
    underlying: BTC
    tenor: weekly
    topics: [ticker]
    roll_before: 3600
"""
        )
    assert md_selects_of(spec.md_select)[0].roll_before_s == 3600


def test_an_empty_topic_list_is_refused() -> None:
    """A selector with no topics subscribes to nothing for every instrument it
    selects, which is a deploy that costs listings and delivers no data."""
    with pytest.raises(StrategyYamlError, match="topics must be a non-empty list"):
        parse_strategy_yml(
            """
md:
  - select: btc_q
    kind: rolling_future
    venue: Deribit
    underlying: BTC
    tenor: weekly
    topics: []
"""
        )


def test_a_select_name_a_strategy_could_not_look_up_is_refused() -> None:
    """The name is the argument to ``self.md.universe``, and it is carried in
    status and log lines keyed by it."""
    with pytest.raises(StrategyYamlError, match="select must be a name of letters"):
        parse_strategy_yml(
            "md:\n  - select: btc chain\n    kind: option_chain\n"
        )


def test_two_selects_may_not_share_a_name() -> None:
    """One name, one universe — otherwise ``self.md.universe('x')`` has two
    answers and the strategy gets whichever was registered last."""
    with pytest.raises(StrategyYamlError, match="is already declared under"):
        parse_strategy_yml(
            """
md:
  md-jp:
    - select: x
      kind: rolling_future
      venue: Deribit
      underlying: BTC
      tenor: weekly
      topics: [ticker]
  md-de:
    - select: x
      kind: rolling_future
      venue: Deribit
      underlying: ETH
      tenor: weekly
      topics: [ticker]
"""
        )


def test_a_feed_may_override_its_delivery() -> None:
    """Each topic has a default (§5.3). A feed that wants the other one says
    so on its own entry, so the override sits next to what it overrides."""
    spec = parse_strategy_yml(
        """
md:
  md-jp:
    - ticker.Deribit_Perp_BTCUSD
    - feed: trade.Deribit_Perp_BTCUSD
      delivery: latest
"""
    )
    assert spec.md == {
        "md-jp": ["ticker.Deribit_Perp_BTCUSD", "trade.Deribit_Perp_BTCUSD"]
    }
    assert spec.md_delivery == {"trade.Deribit_Perp_BTCUSD": "latest"}


def test_a_feed_entry_without_a_delivery_is_just_a_feed() -> None:
    spec = parse_strategy_yml("md:\n  - feed: trade.Deribit_Perp_BTCUSD\n")
    assert md_feeds_of(spec.md) == ["trade.Deribit_Perp_BTCUSD"]
    assert spec.md_delivery == {}


def test_a_delivery_override_keys_on_the_canonical_feed() -> None:
    """Whoever reads this mapping has the normalized feed key, not the one
    somebody typed."""
    spec = parse_strategy_yml(
        "md:\n  - feed: trade.deribit_perp_btcusd\n    delivery: all\n"
    )
    assert spec.md_delivery == {"trade.Deribit_Perp_BTCUSD": "all"}


def test_an_unknown_delivery_mode_is_refused_with_the_three_there_are() -> None:
    with pytest.raises(StrategyYamlError) as caught:
        parse_strategy_yml(
            "md:\n  - feed: trade.Deribit_Perp_BTCUSD\n    delivery: newest\n"
        )
    message = str(caught.value)
    assert "delivery must be one of ['all', 'kline', 'latest']" in message
    assert "newest" in message


def test_a_mapping_under_md_that_is_neither_is_refused() -> None:
    with pytest.raises(StrategyYamlError) as caught:
        parse_strategy_yml("md:\n  - delivery: latest\n")
    assert "no 'feed' and no 'select'" in str(caught.value)


def test_one_entry_is_a_feed_or_a_select_not_both() -> None:
    with pytest.raises(StrategyYamlError, match="names both a feed and a select"):
        parse_strategy_yml(
            "md:\n  - feed: trade.Deribit_Perp_BTCUSD\n    select: x\n"
        )


def test_a_stray_key_on_a_feed_entry_is_refused() -> None:
    """``conflate: true`` would be read as nothing, and the feed would be
    delivered the way the author was trying to change."""
    with pytest.raises(StrategyYamlError, match="a feed entry takes"):
        parse_strategy_yml(
            "md:\n  - feed: trade.Deribit_Perp_BTCUSD\n    conflate: true\n"
        )


def test_an_entry_is_still_refused_when_it_is_not_a_feed_key() -> None:
    """The plain-string form has not changed, and neither has its refusal."""
    with pytest.raises(StrategyYamlError, match="topic.UniversalTicker"):
        parse_strategy_yml("md:\n  - feed: not-a-feed\n")


def test_the_normalized_form_reads_back_into_the_same_spec() -> None:
    """``md_select`` is refused in a *document* and accepted as a field, because
    the second is what a parsed spec dumps to — a row, a payload, a round trip
    through JSON. A union that could not read its own dump back would be a spec
    that only exists for as long as the process that parsed it."""
    spec = parse_strategy_yml(PLAN_6_4)
    assert StrategySpec.model_validate(spec.model_dump()) == spec


def test_the_lifted_fields_are_not_document_keys() -> None:
    """``md_select`` and ``md_delivery`` are halves of ``md:`` the parser lifts
    out, not a second way to write them. Accepting them at the root would be two
    places to declare one selector."""
    with pytest.raises(StrategyYamlError, match="are not document keys"):
        parse_strategy_yml("md: []\nmd_select: {}\n")
