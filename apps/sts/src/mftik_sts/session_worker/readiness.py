"""Which atoms a declared feed is, and when that feed is ready (§5.2).

MdReady is local. The venue's ``atoms_for`` is the same pure function
the connection worker uses. MD answers ``atoms={}`` and the API does not
forward atoms, so there is no MD round trip. A feed whose adapter cannot
resolve it is missing. This module does not invent a resolution.

TdReady is not here. It is one unsettled ``oms.view`` and one
``ledger.view`` per declared account, and the process asks those.
"""

from __future__ import annotations

import importlib
import threading
from dataclasses import dataclass

from mftik.exchange.atoms import Atom, AtomOptions, JoinPolicy, kline_interval
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import Topics

#: Venue name → the module that implements ``atoms_for`` / ``join_policy``.
#: A venue that is not in this map, or whose module still raises, is a
#: missing feed. Paper is the one that resolves today.
_VENUE_ATOMS: dict[str, str] = {
    "Paper": "mftik.exchange.paper.atoms",
    "Binance": "mftik.exchange.binance.spot.atoms",
    "BinanceUM": "mftik.exchange.binance.future.atoms",
    "BinanceCM": "mftik.exchange.binance.delivery.atoms",
    "Bybit": "mftik.exchange.bybit.atoms",
    "Okx": "mftik.exchange.okx.atoms",
    "Bitget": "mftik.exchange.bitget.atoms",
    "Deribit": "mftik.exchange.deribit.atoms",
    "Gate": "mftik.exchange.gate.spot.atoms",
    "GateFutures": "mftik.exchange.gate.future.atoms",
}


@dataclass(frozen=True)
class ResolvedAtom:
    """One atom of one declared feed, and how a late joiner becomes ready."""

    feed: str
    atom: Atom
    policy: JoinPolicy

    @property
    def atom_id(self) -> str:
        return self.atom.atom_id

    @property
    def subject(self) -> str:
        """The fan-out topic :meth:`Topics.atom_subject` names."""
        return Topics.atom_subject(self.atom.atom_id)


@dataclass(frozen=True)
class ResolvedFeed:
    feed: str
    atoms: tuple[ResolvedAtom, ...]


def resolve_feeds(
    feeds: list[str],
) -> tuple[tuple[ResolvedFeed, ...], tuple[str, ...]]:
    """Split ``feeds`` into plans and the ones that cannot be resolved.

    Order of ``feeds`` is kept in both results. A feed is missing when
    the key does not parse, the venue has no atoms module, or ``atoms_for``
    / ``join_policy`` raises — including the ``NotImplementedError`` the
    unwired venues still raise.
    """
    resolved: list[ResolvedFeed] = []
    missing: list[str] = []
    for feed in feeds:
        plan = _resolve_one(feed)
        if plan is None:
            missing.append(feed)
        else:
            resolved.append(plan)
    return tuple(resolved), tuple(missing)


def _resolve_one(feed: str) -> ResolvedFeed | None:
    topic, separator, ticker_text = feed.partition(".")
    if not separator or not topic or not ticker_text:
        return None
    try:
        ticker = UniversalTicker.parse(ticker_text)
    except (TypeError, ValueError):
        return None
    module_name = _VENUE_ATOMS.get(ticker.venue)
    if module_name is None:
        return None
    try:
        module = importlib.import_module(module_name)
        interval = kline_interval(topic)
        opts = AtomOptions(interval=interval) if interval else AtomOptions()
        plan = module.atoms_for(topic, ticker, opts)
        atoms = tuple(
            ResolvedAtom(feed=feed, atom=atom, policy=module.join_policy(atom))
            for atom in plan.atoms
        )
    except Exception:
        return None
    if not atoms:
        return None
    return ResolvedFeed(feed=feed, atoms=atoms)


class FeedReady:
    """Which declared feeds have joined, counted the way §5.2 describes.

    ``NEXT_PUSH`` and ``SNAPSHOT_REPLAY`` become ready on the first event
    for that atom. ``SILENT`` becomes ready when the subscription is up,
    because the venue has nothing to replay. A feed is ready when every
    one of its atoms is. A feed that did not resolve is missing from the
    start and is not waited on.
    """

    def __init__(
        self,
        feeds: tuple[ResolvedFeed, ...],
        missing: tuple[str, ...],
    ) -> None:
        self.feeds = feeds
        self.missing_declared = missing
        self._seen: set[str] = set()
        self._subscribed: set[str] = set()
        self._lock = threading.Lock()
        self._by_subject: dict[str, list[ResolvedAtom]] = {}
        for feed in feeds:
            for atom in feed.atoms:
                self._by_subject.setdefault(atom.subject, []).append(atom)

    def note_subscribed(self, atom_id: str) -> None:
        with self._lock:
            self._subscribed.add(atom_id)

    def note_event(self, subject: str) -> str | None:
        """Record an event on ``subject``. Return the feed it belongs to."""
        atoms = self._by_subject.get(subject)
        if not atoms:
            return None
        with self._lock:
            for atom in atoms:
                self._seen.add(atom.atom_id)
        return atoms[0].feed

    def unresolved_pending(self) -> bool:
        """True while a resolved feed is still waiting on an atom."""
        return any(not self._feed_ready(feed) for feed in self.feeds)

    def missing_feeds(self) -> tuple[str, ...]:
        """Declared misses, then resolved feeds that are not ready yet."""
        pending = tuple(
            feed.feed for feed in self.feeds if not self._feed_ready(feed)
        )
        return self.missing_declared + pending

    def counts(self) -> tuple[int, int]:
        """``(ready, total)`` including feeds that could not be resolved."""
        total = len(self.feeds) + len(self.missing_declared)
        ready = total - len(self.missing_feeds())
        return ready, total

    def _feed_ready(self, feed: ResolvedFeed) -> bool:
        with self._lock:
            return all(self._atom_ready(atom) for atom in feed.atoms)

    def _atom_ready(self, atom: ResolvedAtom) -> bool:
        if atom.policy is JoinPolicy.SILENT:
            return atom.atom_id in self._subscribed
        return atom.atom_id in self._seen
