"""Operator commands the plan adds (IF-15).

``mftik workers``, ``mftik md restart``, ``mftik td drain`` and
``mftik intents gc``. ``td drain`` performs a drain-replace of one
account (B6-04, F27). The others parse, print that they are not
implemented, and exit non-zero. Their decision functions still raise
``NotImplementedError("IF-15")``.

**State authority (§3.3): this layer holds none.** It does not write
session status, worker sets, intents, connection placement, or the
trading layer. It reads, and it names an operator request:

* worker rows come from the Supervisor's ``procman.report``. B3-07
  serves the latest report of every plane on ``GET /workers``; this
  command prints that list. The S-2 pin file is not read here.
  ``code_ref`` on those rows is the platform release the worker was
  spawned from. This module does not
  carry ``strategy_digest`` or ``env_generation``; those axes are F39
  and belong to IF-16. B5-10 is what extends ``--stale`` to the digest.
* an MD restart asks for one connection to come back in place. Placement
  stays the MD controller's (F22); this command does not move an atom.
* a TD drain names one account. Whether its trading layer is up is the
  account worker's, desired by the TD controller (F27, F35).
* an intent GC names one STS instance. The intents themselves stay the
  API's and the STS controller's. A stopped ``procman.report`` reclaims
  nothing (F32); this command is the operator path for a machine that
  is gone, and reclaiming an intent does not cancel orders (F37).

**Invariants.**

* **O1 — ``workers`` without ``--stale`` lists every reported worker.**
  Order is the order the caller handed in. Nothing is dropped because
  of its release.
* **O2 — ``--stale`` lists, and does not restart (F24).** A worker is
  stale when its ``code_ref`` is not the latest release. The platform
  does not restart old MD connections or TD accounts on its own; this
  command only prints the ones a person might then restart, or leave
  until they finish. ``latest is None`` is refused: a release that
  could not be read is not "every worker is stale".
* **O3 — ``md restart`` is one connection, in place (F24).** The result
  names that conn and no other. It is not a placement change.
* **O4 — ``td drain`` is one account (F27).** The result names that
  ``api_id`` and no other. The platform does not drain on upgrade by
  itself. New orders during the drain are refused ``td_draining``.
  The worker logs ``TdReady`` false, then true on the new incarnation.
  Sessions see that transition when B6-06 and B5-05 land.
* **O5 — ``intents gc`` names one instance, and only an operator runs
  it (F32).** A blank name is refused: it would not name a machine.
  The result does not cancel orders. Nothing here treats a missing
  ``procman.report`` as a reason to reclaim.
* **O6 — code identity on this surface is ``code_ref`` only.** Comparing
  a session's pinned strategy digest to the current tree is F39. This
  module does not read it, store it, or decide it.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass

from mftik.cli.client import CliError, connected
from mftik.cli.exits import EXIT_ERROR
from mftik.cli.output import fail, table
from mftik.protocol import ProcmanWorker

__all__ = [
    "IntentGc",
    "MdRestart",
    "TdDrain",
    "drain",
    "gc",
    "intent_gc",
    "md_restart",
    "restart",
    "select_workers",
    "td_drain",
    "workers",
]


def _not_implemented(command: str) -> int:
    """The IF-15 stub: one line on stderr, exit 1, no traceback.

    Same shape as every other refusal this CLI prints (``mftik: …``).
    The command has not talked to a node.
    """
    fail(f"{command} is not implemented (IF-15)")
    return EXIT_ERROR


@dataclass(frozen=True, slots=True)
class MdRestart:
    """An in-place restart of exactly one MD connection (O3, F24)."""

    conn: str


@dataclass(frozen=True, slots=True)
class TdDrain:
    """A drain-replace of exactly one TD account (O4, F27)."""

    api_id: int


@dataclass(frozen=True, slots=True)
class IntentGc:
    """Reclaim intents owned by one STS instance (O5, F32).

    Reclaiming does not cancel resting orders. There is no field for an
    order, and no field for a second instance.
    """

    instance: str


def select_workers(
    reported: Sequence[ProcmanWorker],
    *,
    stale: bool,
    latest: str | None = None,
) -> list[ProcmanWorker]:
    """Which rows ``mftik workers`` prints (O1, O2).

    ``stale`` false: every worker in ``reported``, in that order.
    ``latest`` is ignored, and may be ``None``.
    ``stale`` true: only those whose ``code_ref`` is not ``latest``,
    still in that order. ``latest is None`` is a
    :class:`~mftik.cli.client.CliError`, not an empty list and not the
    full list.

    ``reported`` is what the Supervisor already published. This function
    does not read ``STRATEGON_RELEASE_VERSION`` and does not restart
    anything. ``stale`` false lists every worker (B3-07). ``stale`` true
    is B8-06 and still raises ``NotImplementedError("IF-15")``. When
    that filter lands, ``latest`` has to be
    :func:`mftik.procman.current_release` on both sides: Strategon
    spells a release ``v0.9.5`` and the distribution spells it
    ``0.9.5``.
    """
    if stale:
        del reported, latest
        raise NotImplementedError("IF-15")
    return list(reported)


def md_restart(conn: str) -> MdRestart:
    """The restart ``mftik md restart`` asks for (O3).

    Not implemented (IF-15). B8-06 performs it and records the tape gap.
    """
    del conn
    raise NotImplementedError("IF-15")


#: Longer than the API's drain wait, so the API can answer before the
#: client gives up. The worker's own wait is 30s, then a stop and a start.
DRAIN_HTTP_TIMEOUT_S = 50.0


def td_drain(api_id: int) -> TdDrain:
    """The drain ``mftik td drain`` asks for (O4).

    One positive ``api_id``. The command posts that account and no other.
    A bool is not an id.
    """
    if type(api_id) is not int or api_id <= 0:
        raise CliError("td drain needs a positive api_id")
    return TdDrain(api_id=api_id)


def intent_gc(instance: str) -> IntentGc:
    """The reclaim ``mftik intents gc`` asks for (O5).

    A blank ``instance`` (empty or only whitespace) is a
    :class:`~mftik.cli.client.CliError`. Not implemented (IF-15). No later
    ticket is named on the plan for the body of this command; B4-07 is
    what stops reclaiming while reports are paused, and this function is
    the manual path F32 leaves beside that.
    """
    del instance
    raise NotImplementedError("IF-15")


def _age(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return "-"
    return f"{float(value):.1f}"


def _worker_from_row(row: object) -> ProcmanWorker:
    if not isinstance(row, dict):
        raise CliError("/workers returned a row this client cannot read")
    try:
        return ProcmanWorker(
            id=row["id"],
            code_ref=row["code_ref"],
            rss_bytes=row.get("rss_bytes"),
            phase=row["phase"],
            ready=row["ready"],
            incarnation=row["incarnation"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise CliError(
            f"/workers returned a row this client cannot read: {exc}"
        ) from exc


def _rows(body: object) -> list[tuple[ProcmanWorker, dict[str, object]]]:
    if not isinstance(body, dict) or not isinstance(body.get("workers"), list):
        raise CliError("/workers did not return a worker list")
    raw = body["workers"]
    return [(_worker_from_row(row), row) for row in raw]


def workers(args: argparse.Namespace) -> int:
    """``mftik workers [--stale]``.

    Without ``--stale``, print every worker ``GET /workers`` returned,
    in that order. :func:`select_workers` is called with ``stale=False``
    and does not drop a row because of its release (O1). ``--stale`` is
    B8-06 and still refuses. Listing does not restart anything (F24).
    """
    if args.stale:
        fail(
            "workers --stale is not implemented (IF-15); "
            "B8-06 lists workers not on the latest release"
        )
        return EXIT_ERROR
    _profile, client = connected(args.profile)
    with client:
        body = client.get("/workers")
    parsed = _rows(body)
    chosen = select_workers(
        [worker for worker, _row in parsed], stale=False, latest=None
    )
    # ``select_workers`` returns the same objects it was given. The API
    # row still carries plane, instance and age, which the worker does
    # not.
    extra = {id(worker): row for worker, row in parsed}
    if not chosen:
        print("no workers")
        return 0
    lines = []
    for worker in chosen:
        row = extra[id(worker)]
        rss = worker.rss_bytes
        lines.append(
            (
                str(row.get("plane") or ""),
                str(row.get("instance") or ""),
                worker.id,
                str(worker.incarnation),
                worker.phase,
                "yes" if worker.ready else "no",
                worker.code_ref,
                "-" if rss is None else str(rss),
                _age(row.get("age_s")),
            )
        )
    print(
        table(
            (
                "PLANE",
                "INSTANCE",
                "WORKER",
                "INCARNATION",
                "PHASE",
                "READY",
                "RELEASE",
                "RSS",
                "AGE",
            ),
            lines,
        )
    )
    return 0


def restart(args: argparse.Namespace) -> int:
    """``mftik md restart <conn>``. Does not call :func:`md_restart` yet."""
    del args
    return _not_implemented("md restart")


def drain(args: argparse.Namespace) -> int:
    """``mftik td drain <api_id>``.

    ``POST /td/accounts/{api_id}/drain``. Exit 0 when the new incarnation
    is up. Exit 1 when the API refuses, the account is unknown, or the
    replace did not finish. The body is printed either way.
    """
    named = td_drain(args.api_id)
    _profile, client = connected(args.profile, timeout=DRAIN_HTTP_TIMEOUT_S)
    with client:
        body = client.post(f"/td/accounts/{named.api_id}/drain")
    if not isinstance(body, dict) or type(body.get("ok")) is not bool:
        raise CliError("td drain did not return a result")
    if body["ok"]:
        incarnation = body.get("incarnation")
        shown = "-" if incarnation is None else str(incarnation)
        print(f"api_id={named.api_id} incarnation={shown}")
        return 0
    reason = body.get("reason") or "not drained"
    fail(f"td drain api_id={named.api_id} refused: {reason}")
    return EXIT_ERROR


def gc(args: argparse.Namespace) -> int:
    """``mftik intents gc --instance <name>``. Does not call :func:`intent_gc` yet."""
    del args
    return _not_implemented("intents gc")
