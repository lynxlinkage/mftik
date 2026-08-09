"""What each strategy is, and a strategy.yml that actually runs it.

The deploy document no longer names a strategy, so something has to tell the
UI which types exist and what a working config for each looks like. That is
this catalogue.

It lives in ``mft-common`` rather than in ``apps/sts`` because the API serves
it to the UI and does not depend on the STS package. The STS registry stays
the authority on which classes exist; a test asserts the two agree, so a
strategy cannot be added in one place and quietly missing from the other.

Templates are whole documents on purpose. ``md`` is part of the contract: the
chase needs ``bestquote`` and the noop walk needs ``orderbook``, so a
shared skeleton with only the ``sts:`` block swapped would hand the user a
document that deploys and then never receives the data it waits for.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


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


NOOP = StrategyTemplate(
    type="NoopStrategy",
    label="Noop walk",
    description=(
        "Walks three quote levels per side as BUY then SELL, cancelling each, "
        "then exits. A smoke test for the whole path, not a trading strategy."
    ),
    yaml="""\
td:
  - paper trader
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
  - paper trader
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
  - paper trader
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
  # td[0] quotes; td[1] hedges. Two different venue accounts.
  - binance quoter
  - gate hedger
md:
  - bestquote.Binance_Spot_BTCUSDT
  - bestquote.Gate_Spot_BTCUSDT
sts:
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

#: Every deployable strategy, keyed by the type used on the wire.
TEMPLATES: dict[str, StrategyTemplate] = {
    t.type: t for t in (NOOP, CHASE, OCO, CROSS_ARB)
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
