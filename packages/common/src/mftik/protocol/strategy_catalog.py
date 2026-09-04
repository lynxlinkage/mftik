"""What each strategy is, and a strategy.yml that actually runs it.

The deploy document no longer names a strategy, so something has to tell the
UI which types exist and what a working config for each looks like. That is
this catalogue.

It lives in ``mftik`` rather than in ``apps/sts`` because the API serves
it to the UI and does not depend on the STS package. The STS registry stays
the authority on which classes exist; a test asserts the two agree, so a
strategy cannot be added in one place and quietly missing from the other.

Templates are whole documents on purpose. ``md`` is part of the contract: the
chase needs ``bestquote`` and the noop walk needs ``orderbook``, so a
shared skeleton with only the ``sts:`` block swapped would hand the user a
document that deploys and then never receives the data it waits for.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class StrategyTemplate(BaseModel):
    """One deployable strategy: what it is, and a document that runs it."""

    model_config = ConfigDict(frozen=True)

    #: Class name, and the ``{type}`` in ``POST /sts/deploy/{type}``.
    type: str
    #: Short human-readable name for the picker.
    label: str
    #: One line on what it does, shown next to the picker.
    description: str
    #: A complete strategy.yml — td / md / sts — with no type in it.
    yaml: str
    #: Where the type comes from. The picker groups on this — bundled
    #: examples sink below registry strategies the operator actually runs.
    source: Literal["bundled", "registry"] = "bundled"
    #: Import names the tree declared. Empty for bundled strategies.
    requires: list[str] = Field(default_factory=list)
    #: Whether this node's applied extras cover ``requires``.
    env_ok: bool = True


NOOP = StrategyTemplate(
    type="NoopStrategy",
    label="Noop walk",
    description=(
        "Walks three quote levels per side as BUY then SELL, cancelling each, "
        "then exits. A smoke test for the whole path, not a trading strategy."
    ),
    yaml="""\
td:
  paper trader:
md:
  - orderbook.Paper_Spot_BTCUSDT
sts:
  # BUY mid-gap/mid/mid+gap (place→cancel each), flip to SELL, then exit.
  # 100 of the quote currency (USDT here) per order; mid from the book.
  exec_interval_ms: 1000
  gap_bps: 10
  qty_quote: 100
""",
)

CHASE = StrategyTemplate(
    type="ChaseOrder",
    label="Chase order",
    description=(
        "Rests a post-only order near the touch and reprices it as the book "
        "moves, until the size is filled. Needs a bestquote feed."
    ),
    yaml="""\
td:
  paper trader:
md:
  # Chases top of book, so it needs bestquote rather than orderbook.
  - bestquote.Paper_Spot_BTCUSDT
sts:
  # BUY posts gap_bps below the ask; SELL posts gap_bps above the bid.
  # Repriced whenever it drifts more than gap_bps from where it belongs.
  side: buy
  qty_quote: 100
  gap_bps: 10
  # Give up after this long, or once the price has run this far against us.
  expiry_s: 30
  extreme_bps: 50
  # true finishes the unfilled remainder with a market order; false leaves it.
  must_exec: false
  refresh_interval_ms: 1000
""",
)

OCO = StrategyTemplate(
    type="OneCancelOther",
    label="One-cancel-other",
    description=(
        "Rests two orders at once and keeps whichever fills first, cancelling "
        "the other. Needs a bestquote feed, and reads one message from it."
    ),
    yaml="""\
td:
  paper trader:
md:
  # One quote is read — the pair is checked against it, then it is ignored.
  - bestquote.Paper_Spot_BTCUSDT
sts:
  # Exactly two legs. Neither may be able to trade on arrival: a BUY at or
  # above the ask, or a SELL at or below the bid, is refused before anything
  # is sent. Both legs on the same side is fine.
  orders:
    - side: buy
      price: 49000
      qty: 0.001
    - side: sell
      price: 51000
      qty: 0.001
  # Give up if TD recon or the first quote never arrives.
  arm_timeout_s: 30
""",
)

CROSS_ARB = StrategyTemplate(
    type="CrossArb",
    label="Cross-venue arb",
    description=(
        "Posts PostOnly quotes on one account from another venue's best "
        "quote, and IOC-hedges full size on the first partial/fill."
    ),
    yaml="""\
td:
  # Two different venue accounts. CrossArb names them in sts.
  binance quoter:
  gate hedger:
md:
  - bestquote.Binance_Spot_BTCUSDT
  - bestquote.Gate_Spot_BTCUSDT
sts:
  quote_account: binance quoter
  hedge_account: gate hedger
  quote_ticker: Binance_Spot_BTCUSDT
  hedge_ticker: Gate_Spot_BTCUSDT
  # One or both sides.
  side: [buy, sell]
  qty: 0.001
  # Fee-blind edge band in bps; quotes sit at the midpoint.
  x_lo_bps: 5
  x_hi_bps: 15
""",
)

TWAP = StrategyTemplate(
    type="TwapStrategy",
    label="TWAP",
    description=(
        "Takes liquidity in evenly spaced IOC slices at the touch until "
        "num_round successes land or the window ends. Spot or Perp; needs a "
        "bestquote feed. Perp ensures leverage before arming."
    ),
    yaml="""\
td:
  paper trader:
md:
  # Spot or Perp (e.g. bestquote.BinanceUM_Perp_BTCUSDT).
  - bestquote.Paper_Spot_BTCUSDT
sts:
  side: buy
  # Seconds between slices; total window is exec_interval_s * num_round.
  exec_interval_s: 5
  num_round: 6
  # Exactly one sizing knob — base units or quote currency per round.
  qty_per_round: 0.001
""",
)

TAPE_KEEPER = StrategyTemplate(
    type="TapeKeeper",
    label="Tape keeper",
    description=(
        "Subscribes to trade feeds and holds them open so MD keeps recording "
        "their tape. Places no orders — it exists so another strategy can "
        "warm up on history from before it started."
    ),
    yaml="""\
# No td: this session attaches to no account and cannot place an order.
md:
  # One entry per feed to keep recorded. Only aggtrade and trade are recorded;
  # subscribing to anything else here holds the feed but records nothing.
  - aggtrade.BinanceUM_Perp_BTCUSDT
sts:
  # How often to log that the feeds are still held (ms). It does nothing else,
  # so this line is the difference between "quiet" and "died an hour ago".
  report_interval_ms: 300000
""",
)

MACD_DOLLAR = StrategyTemplate(
    type="MacdDollarBars",
    label="MACD on dollar bars",
    description=(
        "Aggregates trade prints into dollar bars — one bar per fixed amount of "
        "quote currency traded — runs MACD over their closes, and goes long "
        "on a bullish cross / flat on a bearish one. Warms up from MD's "
        "recorded tape, then from live prints."
    ),
    yaml="""\
td:
  paper trader:
md:
  # Two feeds, two jobs. The trade feed builds the bars; the quote feed prices
  # the orders. Both are required — a session with only the first computes its
  # signals and then has no book to cross (refused at start).
  #
  # Exactly one of aggtrade / trade: they report the same matches, so
  # subscribing to both would count every bar's volume twice (also refused).
  - aggtrade.BinanceUM_Perp_BTCUSDT
  - bestquote.BinanceUM_Perp_BTCUSDT
sts:
  feed: aggtrade
  # A bar closes once its prints carry this much of the quote currency, so it
  # sets the strategy's whole timeframe. Sized against how fast the instrument
  # actually trades: on a venue turning over ~$170k/s, 10M is roughly a minute
  # a bar. Too small and MACD crosses faster than the fees it pays; too large
  # and the recorded tape cannot produce the slow + signal = 35 bars warm-up
  # needs, so the session waits on live prints instead.
  bar_quote_volume: 10000000
  # MACD periods over bar closes. fast must be shorter than slow.
  fast: 12
  slow: 26
  signal: 9
  # Quote currency per entry. Long only, one order in flight at a time.
  qty_quote: 100
  # How far through the touch each IOC is priced — the slippage this agrees to
  # in advance. Limit IOC rather than MARKET because a market order's quantity
  # means different things on different venues, while this one is exactly the
  # size that was asked for.
  cross_bps: 5
""",
)

#: Every deployable strategy, keyed by the type used on the wire.
TEMPLATES: dict[str, StrategyTemplate] = {
    t.type: t
    for t in (NOOP, CHASE, OCO, CROSS_ARB, TWAP, TAPE_KEEPER, MACD_DOLLAR)
}

#: Type offered when the caller does not choose one.
DEFAULT_STRATEGY_TYPE = NOOP.type


def strategy_types() -> list[str]:
    """Deployable type names, sorted."""
    return sorted(TEMPLATES)


def all_templates() -> list[StrategyTemplate]:
    """Every template, ordered by type."""
    return [TEMPLATES[name] for name in strategy_types()]


def get_template(type_name: str) -> StrategyTemplate | None:
    return TEMPLATES.get((type_name or "").strip())


def default_template() -> StrategyTemplate:
    return TEMPLATES[DEFAULT_STRATEGY_TYPE]
