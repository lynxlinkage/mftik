"""What the SDK raises at a strategy rather than reporting to it.

Two exceptions, and they are raised for opposite reasons. :class:`NotReady` is
the strategy being early: the platform has not finished putting the session
together, so there is nothing to refuse it *with* yet.
:class:`OffloadWorkerLost` is the platform being honest about a worker that
died underneath a call the strategy is still awaiting — the computation has no
result and never will, and that has to arrive as an exception rather than as a
value the caller might mistake for one.

Neither is a venue refusal. An order the venue turns down is a ``False`` from
:meth:`~mftik.strategy.oms.StrategyOms.submit_order` with a code on
:attr:`~mftik.strategy.oms.StrategyOms.last_reject_code`, or an
``on_order_reject`` later; see :mod:`mftik.protocol.reject_codes`. These two
say the call could not be made at all.

**State authority (§3.3):** neither exception carries state. The lifecycle
phase :class:`NotReady` is raised from belongs to the STS session worker, and
the pool :class:`OffloadWorkerLost` reports on belongs to the session worker
too — the SDK only reads both.

IF-06 defines these. Nothing raises them yet: the order-entry gate lands with
the session worker (B4, B5-01) and ``offload`` lands in B5-03.
"""

from __future__ import annotations


class NotReady(RuntimeError):
    """Order entry was called before ``on_ready`` (F12).

    ``on_start`` runs while MD is already being consumed but TD has not been
    subscribed and no account has reconciled, so the strategy cannot know what
    it holds. A submit there is refused locally and loudly rather than sent on
    a book nobody has checked: a strategy that loads a model for two minutes
    and then trades against positions it never read is the failure this
    prevents.

    Raised by the SDK, not answered by TD — nothing reaches the wire. It is
    distinct from the ``unavailable`` refusal in §5.6, which is an account
    that *was* ready and has gone away: that one is a ``False`` with
    :attr:`~mftik.protocol.reject_codes.RejectCode.TD_UNAVAILABLE`, because by
    then the strategy is trading and a refusal is a normal outcome. Before
    ``on_ready`` it is a programming error.
    """


class OffloadWorkerLost(RuntimeError):
    """A process-mode offload worker died while the call was outstanding.

    OOM, a segfault, an unpicklable result, a worker killed from outside —
    the reason is in the message. The call has no result; the pool is rebuilt
    on the next call and :meth:`~mftik.strategy.base.Strategy.offload_pool`
    runs its ``init`` again, so the state a pool was carrying is gone with it
    (§5.5).

    A session does not die with its worker. Isolation is the point of the
    process mode — the child's ``oom_score_adj`` is raised so it is sacrificed
    before the session is — and a strategy that catches this can fall back,
    retry on smaller input, or give up through
    :meth:`~mftik.strategy.base.Strategy.fail`. Thread mode never raises it: a
    thread cannot be lost without the process going with it.
    """
