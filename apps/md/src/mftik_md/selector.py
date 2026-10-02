"""Selectors: a pure function from a listing and a reference price to a universe.

A ``select:`` block in ``strategy.yml`` names a shape — the two nearest
BTC expiries, ATM ± 5 strikes — rather than the contracts. This module
is the function that turns the shape into the contracts (F33, §6.4).
The MD controller calls it. The strategy hears the difference through
``on_universe_change`` and reads it back with ``self.md.universe``.

**State authority (§3.3).** None of it lives here. The function is pure:
listing, reference, clock and the previous answer in, the next answer
out. The universe, the epoch and the recentring state are the MD
controller's, persisted so a restart continues instead of re-centring.
What gets stored is :class:`Selection` (and :class:`Center` inside it).
The session does not keep a second copy; it is told on
``md.universe.{session_id}``.

Two selectors with the same :func:`spec_hash` are one derivation. The
hash covers the shape and not the name, so two sessions that asked for
the same chain share a universe and an epoch. The name is what each
strategy passes to ``self.md.universe``.

**Invariants.**

* **S1 — pure.** No database, no clock, no connection. ``now`` is an
  argument. A stale listing is a flag on :class:`Listing`, set by the
  caller; this function does not decide how old a listing may be. SYM
  refreshes hourly (§3.3).
* **S2 — ``min_tte``.** An option expiry is skipped when it is already
  at or past settlement (``expiry <= now``), and when it is closer than
  the window (``expiry - now < min_tte``). Exactly ``min_tte`` is kept.
  That is why the chain has moved on before the old expiry ends. The
  first ``nearest`` expiries that survive, soonest first, are the ones
  that are selected. Fewer than ``nearest`` means the board has fewer;
  nothing is invented to fill the count.
* **S3 — strikes.** The centre is a listed strike. Each selected expiry
  takes that strike's neighbours: ``atm`` listed strikes each side, so
  ``2 * atm + 1`` when the board has them (the plan's 5 is 11 strikes).
  The board's edge is the edge. A side that is not listed is not
  invented. An inactive instrument is not a candidate.
* **S4 — debounce.** Distance is in listed strikes, not in price: the
  index gap on that expiry's strike grid between the strike nearest the
  stored centre and the strike nearest the reference. Re-centre only
  when the gap is **greater than** ``recenter_strikes`` and at least
  ``min_dwell`` has passed since :attr:`Center.at`. Exactly the band
  does not move. Exactly ``min_dwell`` may. The tests measure this on
  one expiry whose grid does not change underneath the comparison, and
  they put the reference on a listed strike.
* **S5 — dwell does not freeze an expiry rotation.** Dropping an expiry
  because of ``min_tte`` still changes the universe when the reference
  has not earned a re-centre. The centre's strike and its timestamp stay.
* **S6 — a roll keeps the old future until it settles.** Contracts are
  classified from the UTC date of their expiry, longest tenor first.
  Quarterly is the last Friday of March, June, September or December.
  Monthly is the last Friday of any other month. Weekly is any other
  Friday. A date that is not a Friday is not in a series. ``current``
  is the soonest contract of the requested tenor whose time-to-expiry
  is still greater than ``roll_before``; at exactly ``roll_before`` it
  has already switched to the next one. Every contract of that tenor
  that is still before settlement and expires at or before ``current``
  stays in the universe, so across the roll both books are live. At
  ``expiry <= now`` the old one leaves. Other tenors, and anything that
  is not a dated future, are not members. A ``rolling_future`` has no
  reference price and ignores ``ref``.
* **S7 — fail-static (P5).** A listing marked stale returns
  :class:`Hold` with reason ``listing_stale``, and the previous
  selection is kept, including when the reference is also down and
  including when a roll would otherwise be due. An option chain whose
  reference is ``None`` or not positive returns ``ref_down``. Neither
  case re-centres. The controller being down is not an input: workers
  keep the last desired set, which is P5, and the controller has no
  call that clears one.
* **S8 — unchanged work does not bump the epoch.** A derivation that
  matches ``prev`` returns :class:`Hold`, not a new :class:`Selection`.
  ``unchanged`` when the reference is still on the centre.
  ``within_band`` when it has moved, but not past ``recenter_strikes``.
  ``min_dwell`` when it is past the band and the dwell has not elapsed.
  A real change increments the epoch by one. The first selection, with
  no ``prev``, is epoch 1 and centres immediately — there is no dwell
  to wait out.
* **S9 — no pin.** Nothing a strategy holds can keep a contract in the
  universe. The old future stays because it has not expired, not
  because someone asked.

**Not settled here.** The contract tests avoid these on purpose:

* A reference exactly halfway between two listed strikes: which strike
  is nearer. §6.4 says 最近 and nothing else.
* A qualifying expiry that was not in ``prev``, while debounce is
  holding the centre: whether its strikes follow the stored centre or
  the reference. The persisted state is one centre (§8.4), and the
  tested expiries are either a first selection or already in ``prev``.
* Which expiry's grid measures the distance when the front expiry
  changes in the same evaluation the reference moves.
* A fresh listing with no instruments at all. That can be a real empty
  board or a read that failed, and those two should not share an
  outcome. The caller has ``Listing.stale`` for the read it does not
  trust. An option board that was read and whose every expiry is inside
  ``min_tte`` is specified: the universe becomes empty, the epoch
  increments, and a previous centre is kept.
* Venues whose futures do not expire on Friday. S6 is the calendar of
  the plan's example, which is Deribit.

Null data until B9: :func:`evaluate` raises ``NotImplementedError("IF-09")``.
The specs, the hash and the result types are real.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256

from mftik.exchange.tickers import UniversalTicker
from mftik.protocol.strategy_yml import (
    TENORS,
    OptionChainSelect,
    RollingFutureSelect,
)

_TICKET = "IF-09"


class SelectorError(Exception):
    """A spec or a clock this function will not evaluate.

    Distinct from :class:`NotImplementedError`, which is the null data,
    and from :class:`TypeError`, which is a value of the wrong type.
    """


class HoldReason(StrEnum):
    """Why :func:`evaluate` kept the previous selection.

    The three debounce reasons are distinct so a caller can see that a
    move was considered and refused, which ``unchanged`` does not say.
    """

    LISTING_STALE = "listing_stale"
    REF_DOWN = "ref_down"
    WITHIN_BAND = "within_band"
    MIN_DWELL = "min_dwell"
    UNCHANGED = "unchanged"


@dataclass(frozen=True)
class Hold:
    """Do not replace the previous selection.

    The caller keeps ``prev``, including its epoch and its centre. A
    hold is not a publication: ``md.universe`` is for a selection that
    changed.
    """

    reason: HoldReason


@dataclass(frozen=True)
class Center:
    """The chain's one centre, and when it was chosen.

    ``at`` is the dwell clock. It moves when the strike moves and not
    otherwise, so a later evaluation that holds — or a row rewritten
    because something else changed — must not be used in its place.
    ``updated_at`` on the selector's database row is not this value
    unless it is defined to survive a hold.
    """

    strike: Decimal
    at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.strike, Decimal) or self.strike <= 0:
            raise SelectorError("a centre strike must be a positive Decimal")
        if not isinstance(self.at, datetime) or self.at.tzinfo is None:
            raise SelectorError("center.at must be a timezone-aware datetime")


@dataclass(frozen=True)
class Selection:
    """One derivation that differs from ``prev`` and is worth publishing.

    ``members`` is the universe. ``current`` is the front contract of a
    rolling future and None for an option chain; when it is set it is a
    member. ``epoch`` starts at 1 and increments by one on each change.
    It survives a controller restart because the whole selection does.
    ``center`` is the option chain's recentring state and None for a
    rolling future.
    """

    members: frozenset[UniversalTicker]
    epoch: int
    current: UniversalTicker | None = None
    center: Center | None = None

    def __post_init__(self) -> None:
        if isinstance(self.epoch, bool) or not isinstance(self.epoch, int):
            raise TypeError("epoch must be an int")
        if self.epoch < 1:
            raise SelectorError("epoch starts at 1")
        if self.current is not None and self.current not in self.members:
            raise SelectorError("current must be a member of the selection")
        for ticker in self.members:
            if not isinstance(ticker, UniversalTicker):
                raise TypeError(f"{ticker!r} is not a UniversalTicker")


@dataclass(frozen=True)
class Listed:
    """One instrument as SYM listed it, detached from the database row.

    The authority for the row is SYM (§3.3). This is the copy the pure
    function reads. ``expiry`` is None for an instrument that does not
    settle. ``strike`` and ``option_type`` (``C`` or ``P``) are set on
    options. ``underlying`` is the asset (``BTC``), not an instrument.
    ``active`` is False once the venue stops listing it; those rows are
    not candidates.
    """

    ticker: UniversalTicker
    underlying: str
    expiry: datetime | None = None
    strike: Decimal | None = None
    option_type: str | None = None
    active: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.ticker, UniversalTicker):
            raise TypeError("ticker must be a UniversalTicker")
        if self.expiry is not None and (
            not isinstance(self.expiry, datetime) or self.expiry.tzinfo is None
        ):
            raise SelectorError("expiry must be a timezone-aware datetime")
        if self.strike is not None and not isinstance(self.strike, Decimal):
            raise TypeError("strike must be a Decimal")
        if self.option_type is not None and self.option_type not in {"C", "P"}:
            raise SelectorError("option_type must be 'C', 'P', or None")


@dataclass(frozen=True)
class Listing:
    """A snapshot of the board, and whether the caller trusts it.

    ``stale`` is the fail-static input (S7). This function does not
    infer it from the age of the rows or from the snapshot being empty.
    """

    instruments: tuple[Listed, ...] = ()
    stale: bool = False

    def __post_init__(self) -> None:
        for row in self.instruments:
            if not isinstance(row, Listed):
                raise TypeError(f"{row!r} is not a Listed instrument")


def _text(name: str, value: str) -> str:
    if not isinstance(value, str) or not value:
        raise SelectorError(f"{name} must be a non-empty string")
    return value


def _whole(name: str, value: int, *, low: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int")
    if value < low:
        raise SelectorError(f"{name} must be >= {low}, got {value}")
    return value


@dataclass(frozen=True)
class OptionChainSpec:
    """The shape :func:`evaluate` reads for ``kind: option_chain``.

    Built from :class:`~mftik.protocol.strategy_yml.OptionChainSelect`,
    which is the parsed document. The fields are the same decisions; the
    nesting is flattened because the function should not know about YAML.
    ``name`` is not part of :func:`spec_hash`. ``ref`` is the feed key of
    the reference price, which the controller subscribes to for the
    selector and which the strategy does not receive unless it asked.
    """

    name: str
    venue: str
    underlying: str
    ref: str
    nearest: int
    min_tte_s: int
    atm: int
    sides: tuple[str, ...]
    topics: tuple[str, ...]
    recenter_strikes: int
    min_dwell_s: int

    def __post_init__(self) -> None:
        _text("name", self.name)
        _text("venue", self.venue)
        _text("underlying", self.underlying)
        _text("ref", self.ref)
        _whole("nearest", self.nearest, low=1)
        _whole("min_tte_s", self.min_tte_s, low=0)
        _whole("atm", self.atm, low=0)
        _whole("recenter_strikes", self.recenter_strikes, low=1)
        _whole("min_dwell_s", self.min_dwell_s, low=0)
        if (
            isinstance(self.sides, str)
            or not isinstance(self.sides, tuple)
            or not self.sides
            or any(side not in {"C", "P"} for side in self.sides)
        ):
            raise SelectorError("sides must be a non-empty tuple drawn from ('C', 'P')")
        if (
            isinstance(self.topics, str)
            or not isinstance(self.topics, tuple)
            or not self.topics
            or any(not isinstance(topic, str) or not topic for topic in self.topics)
        ):
            raise SelectorError("topics must be a non-empty tuple of topic names")

    @classmethod
    def from_select(cls, select: OptionChainSelect) -> OptionChainSpec:
        """The spec the parsed document named."""
        if not isinstance(select, OptionChainSelect):
            raise TypeError("from_select takes an OptionChainSelect")
        return cls(
            name=select.name,
            venue=select.venue,
            underlying=select.underlying,
            ref=select.ref,
            nearest=select.expiries.nearest,
            min_tte_s=select.expiries.min_tte_s,
            atm=select.strikes.atm,
            sides=tuple(select.sides),
            topics=tuple(select.topics),
            recenter_strikes=select.recenter.strikes,
            min_dwell_s=select.recenter.min_dwell_s,
        )


@dataclass(frozen=True)
class RollingFutureSpec:
    """The shape :func:`evaluate` reads for ``kind: rolling_future``.

    No reference feed: a roll is the calendar (S6), not a price. ``name``
    is not part of :func:`spec_hash`.
    """

    name: str
    venue: str
    underlying: str
    tenor: str
    roll_before_s: int
    topics: tuple[str, ...]

    def __post_init__(self) -> None:
        _text("name", self.name)
        _text("venue", self.venue)
        _text("underlying", self.underlying)
        if self.tenor not in TENORS:
            raise SelectorError(f"tenor must be one of {sorted(TENORS)}")
        _whole("roll_before_s", self.roll_before_s, low=0)
        if (
            isinstance(self.topics, str)
            or not isinstance(self.topics, tuple)
            or not self.topics
            or any(not isinstance(topic, str) or not topic for topic in self.topics)
        ):
            raise SelectorError("topics must be a non-empty tuple of topic names")

    @classmethod
    def from_select(cls, select: RollingFutureSelect) -> RollingFutureSpec:
        """The spec the parsed document named."""
        if not isinstance(select, RollingFutureSelect):
            raise TypeError("from_select takes a RollingFutureSelect")
        return cls(
            name=select.name,
            venue=select.venue,
            underlying=select.underlying,
            tenor=select.tenor,
            roll_before_s=select.roll_before_s,
            topics=tuple(select.topics),
        )


#: What :func:`evaluate` accepts. The document's discriminated union,
#: flattened, so the function does not import the YAML models at the
#: call — :meth:`OptionChainSpec.from_select` is the boundary.
SelectorSpec = OptionChainSpec | RollingFutureSpec


def spec_hash(spec: SelectorSpec) -> str:
    """Identity of a shape. Same hash, one derivation, one shared epoch (§6.4).

    The name is excluded: two sessions may call one chain by two names
    and still share it. Side order and topic order are not part of the
    identity either; both are sets. The digest is SHA-256, hex, so it
    is stable across processes and safe to store as IF-14's ``spec_hash``.
    """
    if isinstance(spec, OptionChainSpec):
        fields = (
            "option_chain",
            spec.venue,
            spec.underlying,
            spec.ref,
            str(spec.nearest),
            str(spec.min_tte_s),
            str(spec.atm),
            ",".join(sorted(spec.sides)),
            ",".join(sorted(spec.topics)),
            str(spec.recenter_strikes),
            str(spec.min_dwell_s),
        )
    elif isinstance(spec, RollingFutureSpec):
        fields = (
            "rolling_future",
            spec.venue,
            spec.underlying,
            spec.tenor,
            str(spec.roll_before_s),
            ",".join(sorted(spec.topics)),
        )
    else:
        raise TypeError("spec_hash takes an OptionChainSpec or a RollingFutureSpec")
    return sha256("\n".join(fields).encode()).hexdigest()


def evaluate(
    listing: Listing,
    ref: Decimal | None,
    now: datetime,
    prev: Selection | None,
    *,
    spec: SelectorSpec,
) -> Selection | Hold:
    """Derive the universe, or hold the previous one (S1).

    Positional arguments are the ones the ticket names. ``spec`` is
    keyword-only because a derivation without a shape is not a call.

    The option-chain order, once this is implemented:

    1. ``listing.stale`` → :attr:`HoldReason.LISTING_STALE`, without
       reading ``ref``.
    2. ``ref is None`` or ``ref <= 0`` → :attr:`HoldReason.REF_DOWN`.
    3. Otherwise derive (S2, S3) and apply debounce (S4, S5, S8).

    A rolling future skips step 2. Stale still holds, and a fresh
    listing rolls on the calendar alone (S6).

    B9-01 (``option_chain``) and B9-02 (``rolling_future``).
    """
    if not isinstance(spec, (OptionChainSpec, RollingFutureSpec)):
        raise TypeError("spec must be an OptionChainSpec or a RollingFutureSpec")
    if not isinstance(listing, Listing):
        raise TypeError("listing must be a Listing")
    if ref is not None and not isinstance(ref, Decimal):
        raise TypeError("ref must be a Decimal price, or None when it is down")
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise SelectorError("now must be a timezone-aware datetime")
    if prev is not None and not isinstance(prev, Selection):
        raise TypeError("prev must be a Selection or None")
    raise NotImplementedError(f"{_TICKET}: evaluate is B9-01 / B9-02")


__all__ = [
    "Center",
    "Hold",
    "HoldReason",
    "Listed",
    "Listing",
    "OptionChainSpec",
    "RollingFutureSpec",
    "Selection",
    "SelectorError",
    "SelectorSpec",
    "evaluate",
    "spec_hash",
]
