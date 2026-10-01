"""What ``on_ready`` is handed: the session as the platform could assemble it.

``on_ready`` fires once, and it fires whether or not every feed turned up (F12).
TD is a hard condition — an account that has not reconciled inside
``ready_timeout_s`` fails the session, because trading an account whose
contents are unknown is worse than not starting. MD is a soft one: a feed that
is late, or a selector that has not derived a universe yet, is named in
:attr:`Ready.missing_feeds` and the strategy decides what that is worth.

So this object is the one place a strategy learns it is starting degraded. A
strategy that needs all of its feeds checks ``missing_feeds`` and calls
:meth:`~mftik.strategy.base.Strategy.fail`; one that can trade the legs it has
goes on; one with an opinion in between — hedge only, quote wider — acts on it
here rather than discovering it from silence on a hook.

**State authority (§3.3):** none of it is the SDK's. Readiness is computed by
the STS session worker's ingress from the MD connection workers' feed state and
the TD account workers' recon, and this is the snapshot it hands over. A
strategy that wants the live picture afterwards reads
:meth:`~mftik.strategy.md.StrategyMd.state` and
:meth:`~mftik.strategy.td.StrategyTd.state`, which follow the same authorities;
``missing_feeds`` is not kept up to date behind the strategy's back.

IF-06 defines the shape. The ingress that fills it in lands in B4-02 / B5-01;
until then a session passes an empty one.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Ready:
    """The readiness report passed to ``on_ready`` (F12, §5.2).

    Empty means everything that was declared is ready, which is the ordinary
    case — the report is only interesting when it is not empty.
    """

    #: Feeds declared in ``strategy.yml`` that are not live yet, as feed keys
    #: (``ticker.Deribit_Perp_BTCUSD``). A selector that could not derive a
    #: universe inside ``ready_timeout_s``, or whose members have produced
    #: nothing, is listed under its ``select:`` name (F33) — there is no
    #: per-feed ``required`` flag to consult, by decision, so this list is the
    #: whole of what the platform will say about it.
    #:
    #: A feed here is late, not refused. A feed that cannot be subscribed at
    #: all — unknown symbol, a topic the venue does not carry — is rejected
    #: when the MD intent is registered and the deploy fails there, so it
    #: never reaches a running session.
    missing_feeds: tuple[str, ...] = ()
