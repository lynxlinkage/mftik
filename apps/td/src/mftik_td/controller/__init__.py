"""TD controller: desired accounts, the trading-layer bit, drain-replace.

This is the layer §3.4 names ``mftik_td.controller``. It replaces the
lease and refcount half of ``session/manager.py``. The account set, the
trading level, the spawn gate and the intent reply are real (B4-07).
Drain-replace still raises ``NotImplementedError("IF-12")``.
:func:`td_reattach` is procman's table (B3-03). ``pid_gone`` is
:func:`~mftik.procman.previous_worker_gone`. ``MARK_FAILED``, and
``NONE`` while a shim is still waiting, name ``RELEASE`` for
:meth:`~mftik.procman.Supervisor.release_slot`. This package does not
change :meth:`~mftik.procman.Supervisor.spawn`.

The running process registers :func:`intent_handler` on
``td.{instance}``, subscribes to ``procman.report.sts.*``, and
constructs :class:`TdOrchestrator` with the provisional
:class:`~mftik.procman.RestartIntensity` built from
:mod:`mftik_td.controller.defaults` (issue #286). Held intents are a
:class:`TdIntentBook`. ``PUSH_TRADING`` is named and not delivered
(P5, B6-02). B6-04 does drain-replace. The pid fence is
:func:`~mftik.procman.previous_worker_gone` on :class:`AccountView`,
and :meth:`~mftik.procman.Supervisor.spawn` still enforces it (B3-03,
F36). This package does not import strategy code and does not carry
``strategy_digest`` or ``env_generation`` (F39, IF-16).

**State authority (§3.3).**

* Desired accounts for this instance: this controller, in memory,
  recomputed from the bindings it was given. The user, through the API,
  is the authority for the ``apis`` binding.
* Desired trading layer, open or closed: this controller, from intents,
  level-triggered (F35). Intent rows belong to the API and the STS
  controller. The observed bit belongs to the account worker. Controller
  silence leaves the worker's last bit in place (P5).
* Worker existence, exit code, signal: the shim. This layer reads the
  supervisor's report of them. Whether the previous pid is gone is
  :func:`~mftik.procman.previous_worker_gone`.
* ``code_ref`` on the worker spec: the platform release of this
  controller (§4.5). Not a strategy digest.

**Invariants.**

* **W1–W2** The worker set is every account bound to this instance
  (F35). An intent does not add or remove one. Another instance's
  account is not started here (F36). See :func:`desired_accounts`.
* **L1–L3** The trading bit is level-triggered: on when any held intent
  names the account, off when the last one is gone, with no linger
  (F35). A put replaces that owner's set. Nothing is pushed while the
  controller is not publishing, and close names no trading push (P5).
  See :func:`trading_active` and :func:`trading_pushes`.
* **N1–N2** A new incarnation is named only after the supervisor
  reports the previous pid gone, or when there is no previous
  incarnation (F36). That report is
  :func:`~mftik.procman.previous_worker_gone`. The §4.4 cell is
  :func:`td_reattach`, which is procman's
  :func:`~mftik.procman.reattach_action` for ``td``. The spawn it
  names is :meth:`~mftik.procman.Supervisor.spawn`. ``MARK_FAILED``
  and a waiting shim on ``NONE`` name
  :meth:`~mftik.procman.Supervisor.release_slot`. This package does
  not open ``/proc`` and does not keep a second table.
  See :func:`spawn_allowed` and :func:`release_named`.
* **D1** :meth:`TdOrchestrator.drain_replace` is one account, asked for
  by an operator (F27). A release upgrade does not call it. While the
  pid is alive the actions extend the dead-man's switch, drain, and
  stop. The next incarnation is named only once the pid is gone. The
  trading bit is not cleared by a drain.
* **K1** The worker spec says ``restart="on_failure"``. The intensity
  passed to :func:`~mftik.procman.plan_restart` is the provisional one
  built from :mod:`mftik_td.controller.defaults` (issue #286). There is no crash
  class (P6). See :meth:`TdOrchestrator.account_restart`.

Procman knows processes. Account membership, the trading bit and
drain-replace stay here (P6).
"""

from mftik_td.controller._ticket import TICKET
from mftik_td.controller.decisions import (
    account_pid_gone,
    apply_delete,
    apply_put,
    close_actions,
    desired_accounts,
    observation_view,
    plan_account_restart,
    release_named,
    spawn_allowed,
    td_reattach,
    trading_active,
    trading_pushes,
)
from mftik_td.controller.handlers import (
    INTENT_TYPES,
    TdIntentBook,
    control_subject,
    intent_book,
    intent_handler,
)
from mftik_td.controller.orchestrator import TdOrchestrator
from mftik_td.controller.types import (
    ACCOUNT_KIND,
    FIRST_INCARNATION,
    AccountView,
    ActionKind,
    BoundAccount,
    OrchestratorAction,
    TradingDesired,
    account_worker_id,
)
from mftik_td.controller.worker import account_worker_spec

__all__ = [
    "ACCOUNT_KIND",
    "FIRST_INCARNATION",
    "INTENT_TYPES",
    "TICKET",
    "AccountView",
    "ActionKind",
    "BoundAccount",
    "OrchestratorAction",
    "TdIntentBook",
    "TdOrchestrator",
    "TradingDesired",
    "account_pid_gone",
    "account_worker_id",
    "account_worker_spec",
    "apply_delete",
    "apply_put",
    "close_actions",
    "control_subject",
    "desired_accounts",
    "intent_book",
    "intent_handler",
    "observation_view",
    "plan_account_restart",
    "release_named",
    "spawn_allowed",
    "td_reattach",
    "trading_active",
    "trading_pushes",
]
