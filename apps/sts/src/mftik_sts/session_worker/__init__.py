"""STS session worker — ingress, strategy thread, delivery (IF-05, §3.4).

One process, one session, two threads (F8). The :class:`Ingress` is the
main thread and the receive connection. The :class:`StrategyRunner` is
the second thread, the strategy's loop, and the send connection. They
are born with the process and they die with it. Nothing here restarts
them, and nothing here spans a second session (I2).

**This layer is the authority for (§3.3, STS session):**

* the strategy's own state, in the strategy object, in memory, not
  persisted (F10). A relaunch starts again at ``on_start`` with nothing
  carried over. The object lives here; the worker does not snapshot it.
* the ``client_order_id`` sequence (``session | ts_sec | seq``). The
  minter still lives in :mod:`mftik.strategy.client_order_id`; the order
  path moves with B4-03. This package does not mint one yet.
* the event log, written by the ingress's writer thread
  (``STS_EVENTLOG_DIR``). The controller reads it; it does not write it
  (F40).
* hook progress, offload progress, and the delivery drop count, on
  ``sts.status.{session_id}``.

**It is not the authority for:**

* the session spec or the session row. The API writes the spec. The
  controller's Supervisor writes the status. This process is handed a
  spec and reports; it does not decide the row.
* which code it is running (F39). Code identity is three axes —
  ``code_ref``, ``strategy_digest``, ``env_generation`` — pinned by the
  API at start and checked by the controller without importing. This
  package does not carry those fields, does not import the platform
  registry, and does not choose a tree. IF-16 owns the fields.
* market data, including the per-atom ``seq`` it forwards (the MD
  connection worker, F21, F25). A missing print is the strategy's to
  notice. This layer does not record a gap and does not offer
  ``on_feed_gap`` (F23).
* the OMS and the ledger (the TD account worker).
* operator paths on the host disk — registry, extras, artifact RPC,
  event-log reads (the controller, F40). The strategy still writes
  artifacts from this process; that path is the one it already has,
  and this package does not add a second one.

**Invariants.**

* **I1** The ingress starts before the strategy thread and ends after
  it. ``on_stop`` still receives the replies to its cancels, and the
  fills that beat them.
* **I2** The ingress's lifetime is the process, which is the session.
  It is not restarted inside the process. An ingress that dies
  fail-fasts the process with a non-zero exit.
* **I3** The ingress runs on the main thread, because that is where
  signal handlers run. The strategy is not allowed to install one.
* **I4** The ingress does not run user code and does not do blocking
  I/O. Decoding and hooks run on the strategy thread. The event log's
  write runs on the writer thread.

**Null until the ticket that builds it.** The table lookups (which
mode a kind has, what an overflow does, the F15 numbers, the phase
enum) are data and they are live. Every operation that moves a thread,
a queue or a measurement raises ``NotImplementedError("IF-05")`` or
returns ``None`` or an empty collection. B4-03 builds the threads and
turns I1–I4 green. B5-01 builds the queues. B5-02 persists the log
marks. B5-04 classifies hook time.

**Not decided here.**

* There is no ``SessionSpec`` type yet (IF-14). :class:`Ingress` takes
  :class:`~mftik.protocol.messages.StsCreateSessionRequest`, which IF-01
  already describes as the session spec and which deliberately has no
  digest fields.
* §5.3 says the ``all`` queue is bounded and does not say how long.
  ``capacity`` is an argument. It also groups TD, ``feed_end`` and RPC
  replies on one row and does not say whether they share a queue.
* F25 says the strategy reads ``event.seq`` and ``event.age``. IF-06's
  hooks take the platform model (``Ticker`` and the rest), which has
  neither field. Both live on :class:`Inbound`, the object the strategy
  thread pulls and then decodes. How a hook parameter grows those
  fields is B5-01's to settle with IF-06. This package does not change
  the hook signatures and does not add the fields to the models.
* The kline key is ``(feed, bar_open)``, and I4 says the ingress does
  not decode. :class:`Inbound.bar_open` is therefore a header the
  builder fills in, not something :class:`Delivery` parses out of
  ``body``. Who stamps the header — MD, on the envelope, or a read that
  is not a decode — is open.
"""

from mftik_sts.session_worker.budget import (
    HOOK_HARD_S,
    HOOK_WARN_S,
    ON_READY_LIMIT_S,
    ON_STOP_LIMIT_S,
    Disposition,
    HookBudgetReport,
    Measure,
    assess_hook,
    is_lifecycle,
)
from mftik_sts.session_worker.delivery import (
    DEFAULT_DELIVERY,
    MUST_DELIVER,
    Delivery,
    Overflow,
    delivery_mode,
    kind_of_feed,
    kind_of_topic,
    overflow_policy,
    topic_of,
)
from mftik_sts.session_worker.errors import (
    IngressEnded,
    IngressNotMainThread,
    IngressNotStarted,
    SessionFailed,
    SignalHandlersReserved,
    StrategyStillRunning,
)
from mftik_sts.session_worker.events import Inbound, LogMark, LogRecord, StreamKind
from mftik_sts.session_worker.ingress import Ingress
from mftik_sts.session_worker.phase import Phase
from mftik_sts.session_worker.runner import (
    StrategyRunner,
    refuse_strategy_signal_handler,
)

__all__ = [
    "DEFAULT_DELIVERY",
    "HOOK_HARD_S",
    "HOOK_WARN_S",
    "MUST_DELIVER",
    "ON_READY_LIMIT_S",
    "ON_STOP_LIMIT_S",
    "Delivery",
    "Disposition",
    "HookBudgetReport",
    "Inbound",
    "Ingress",
    "IngressEnded",
    "IngressNotMainThread",
    "IngressNotStarted",
    "LogMark",
    "LogRecord",
    "Measure",
    "Overflow",
    "Phase",
    "SessionFailed",
    "SignalHandlersReserved",
    "StrategyRunner",
    "StrategyStillRunning",
    "StreamKind",
    "assess_hook",
    "delivery_mode",
    "is_lifecycle",
    "kind_of_feed",
    "kind_of_topic",
    "overflow_policy",
    "refuse_strategy_signal_handler",
    "topic_of",
]
