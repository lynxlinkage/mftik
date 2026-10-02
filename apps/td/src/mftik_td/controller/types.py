"""Facts the TD controller reads, and the actions it will name.

Constructing these is real. Deciding anything from them is
:mod:`mftik_td.controller.decisions`, and that raises.

No ``strategy_digest`` and no ``env_generation``. Those axes are IF-16
(F39). Code identity on a worker spec is ``code_ref``, the platform
release of the controller that spawns it (§4.5).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from mftik.exchange.venues import require
from mftik.instance import validate_instance_name
from mftik.procman import ObservedWorker, validate_worker_id

#: ``WorkerSpec.kind`` for a TD account worker (§3.1, §4.3).
ACCOUNT_KIND = "account"

#: Incarnation of the first worker spawned for an account. A replacement
#: uses the previous incarnation plus one. ``None`` on a view means no
#: worker has been spawned yet.
FIRST_INCARNATION = 1


class ActionKind(StrEnum):
    """One step :meth:`mftik_td.controller.TdOrchestrator.reconcile` names.

    Applying the step — spawn, stop, the push, the drain — is not this
    ticket. Spawn and stop are :class:`mftik.procman.Supervisor` calls.
    """

    SPAWN = "spawn"
    STOP = "stop"
    #: Deliver one account's trading-layer desired bit (F35, P2).
    PUSH_TRADING = "push_trading"
    #: Lengthen the dead-man's-switch countdown before a drain (F37).
    EXTEND_DEADMAN = "extend_deadman"
    #: Refuse new orders while this account is drain-replacing (F27, §4.6).
    DRAIN = "drain"


def _as_int(value: object, name: str) -> int:
    # ``bool`` is an ``int`` subclass; a flag here would be a spec bug.
    if type(value) is not int:
        raise ValueError(f"{name} must be an int")
    return value


def positive_api_id(value: object) -> int:
    """An ``api_id`` the account worker can be named with.

    The same rule as :class:`mftik_td.account.AccountWorker`: a positive
    int. ``True`` is not one.
    """
    api_id = _as_int(value, "api_id")
    if api_id <= 0:
        raise ValueError(f"api_id must be a positive int, got {api_id!r}")
    return api_id


def account_worker_id(api_id: int) -> str:
    """``td/account/<api_id>`` (§4.3). One worker, one account."""
    worker_id = f"td/account/{positive_api_id(api_id)}"
    validate_worker_id(worker_id)
    return worker_id


@dataclass(frozen=True)
class BoundAccount:
    """One account bound to a TD instance (F35, F36).

    The binding lives on ``apis`` (``ApiRepository.instance_name``). The
    user, through the API, is the authority. This object is the copy the
    controller was given. It does not read the database and it does not
    write the row.

    There is no account-level enabled column. An account this instance
    runs is one whose binding names this instance. ``desired_accounts``
    is that filter. A later flag, if one is added, is the caller's to
    apply before this object is built.

    ``cancel_on_disconnect`` is not here. The account worker holds the
    flag it was given (F37). This layer does not keep a second copy.
    """

    api_id: int
    venue: str
    instance: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "api_id", positive_api_id(self.api_id))
        object.__setattr__(self, "venue", require(self.venue).name)
        object.__setattr__(self, "instance", validate_instance_name(self.instance))


@dataclass(frozen=True)
class TradingDesired:
    """The level-triggered trading-layer bit for one account (F35, P2).

    ``active`` is true when at least one held intent names ``api_id``.
    It is the whole answer, not a delta and not a count. The account
    worker is the authority for the observed switch. This value is what
    the controller pushes. When nothing is pushed, the worker keeps the
    last one (P5).
    """

    api_id: int
    active: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "api_id", positive_api_id(self.api_id))
        if not isinstance(self.active, bool):
            raise ValueError("active must be a bool")


@dataclass(frozen=True)
class AccountView:
    """What the supervisor reports for one account worker.

    ``observed`` is the §4.4 category from the supervisor's reattach
    scan. ``pid_gone`` is that supervisor's observation that the
    previous pid is no longer there (F36, B3-03). This layer does not
    open ``/proc``. ``incarnation`` is the one the supervisor still
    holds, or ``None`` when it holds none.

    The shim is the authority for whether the process exists and for
    the exit code and signal. The supervisor reads both and reports
    them here.
    """

    api_id: int
    observed: ObservedWorker
    pid_gone: bool
    incarnation: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "api_id", positive_api_id(self.api_id))
        try:
            observed = ObservedWorker(self.observed)
        except ValueError as exc:
            raise ValueError(
                f"observed {self.observed!r} is not a worker state"
            ) from exc
        if not isinstance(self.pid_gone, bool):
            raise ValueError("pid_gone must be a bool")
        if self.incarnation is None:
            incarnation = None
        else:
            incarnation = _as_int(self.incarnation, "incarnation")
            if incarnation < 0:
                raise ValueError("incarnation must be >= 0")
        object.__setattr__(self, "observed", observed)
        object.__setattr__(self, "incarnation", incarnation)


@dataclass(frozen=True)
class OrchestratorAction:
    """One named step. The supervisor, or the account worker, applies it.

    ``SPAWN`` carries the incarnation to spawn and is applied with
    :meth:`mftik.procman.Supervisor.spawn`, which waits until the
    previous pid is gone (F36). ``STOP`` is
    :meth:`~mftik.procman.Supervisor.stop`. ``PUSH_TRADING`` carries
    ``active``. ``EXTEND_DEADMAN`` and ``DRAIN`` carry only ``api_id``.
    """

    kind: ActionKind
    api_id: int
    incarnation: int | None = None
    active: bool | None = None

    def __post_init__(self) -> None:
        try:
            kind = ActionKind(self.kind)
        except ValueError as exc:
            raise ValueError(f"kind {self.kind!r} is not an action") from exc
        api_id = positive_api_id(self.api_id)
        if kind is ActionKind.SPAWN:
            incarnation_ok = (
                type(self.incarnation) is int and self.incarnation >= FIRST_INCARNATION
            )
            if not incarnation_ok:
                raise ValueError("spawn incarnation must be an int >= 1")
            if self.active is not None:
                raise ValueError("spawn does not carry a trading bit")
            incarnation: int | None = self.incarnation
            active: bool | None = None
        elif kind is ActionKind.PUSH_TRADING:
            if not isinstance(self.active, bool):
                raise ValueError("push_trading requires active to be a bool")
            if self.incarnation is not None:
                raise ValueError("push_trading does not carry an incarnation")
            incarnation = None
            active = self.active
        else:
            if self.incarnation is not None or self.active is not None:
                raise ValueError(f"{kind.value} carries only api_id")
            incarnation = None
            active = None
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "api_id", api_id)
        object.__setattr__(self, "incarnation", incarnation)
        object.__setattr__(self, "active", active)
