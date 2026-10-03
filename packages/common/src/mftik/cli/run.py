"""Push, deploy, and tail. The loop a person is in when they edit a strategy.

A separate push is the step they forget, so this does it unless asked not to.

Ctrl-C stops the session. That is the whole reason this command reads the way
it does: a strategy is placing orders, somebody is watching it in the
foreground, and the key they reach for when they want it to stop has to stop
it. Detaching instead would leave a live position behind on the strength of a
keystroke that every other program treats as "end this". A second Ctrl-C, once
the stop is already going out, leaves the session running and says so loudly —
that is the escape hatch, and it is the one that has to be typed twice.

``--no-follow`` never attaches, so it never stops anything either. With
``--wait`` (the default) it still watches until the session is running,
failed, or done, and prints that outcome. ``--no-wait`` wins over it: the
id and how to stop it, then return. No watch and no tail.

``--wait`` / ``--no-wait`` are the F12 surface (§5.2). ``--wait`` is the
default. The deploy answers 202 with ``starting``; this command watches
the session phase until :func:`run_wait_action` says to stop, then tails.
``running`` follows the live log. ``failed`` prints the reason and the
stored page and exits with an error, without opening the socket. ``done``
prints the status and the stored page and exits 0. ``--no-wait`` prints
the session id and returns.

**State authority (§3.3): this command holds none.** Session status belongs
to the STS controller's Supervisor. This command reads that status and then
tails a log. It does not write the status, and it does not store
``strategy_digest`` or ``env_generation`` (F39, IF-16).
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Literal

from mftik.cli.client import (
    DEFAULT_TIMEOUT_S,
    Client,
    CliError,
    NodeUnreachable,
    connected,
    is_environment_refusal,
)
from mftik.cli.exits import EXIT_ERROR, EXIT_INTERRUPTED
from mftik.cli.push import push_tree, report_push
from mftik.cli.sessions import follow_logs, render_log_page
from mftik.cli.tree import inspect_tree, read_yaml, require_tree
from mftik.clock import Clock, FakeClock, SystemClock
from mftik.protocol.strategy_yml import StrategyYamlError, parse_strategy_yml
from mftik.registry.qualify import PRIVATE_ORIGIN, qualify

#: How long the deploy POST may take. One ordinary HTTP hop, the same
#: budget every other command uses. F12 makes deploy answer 202 once the
#: session is accepted, so this is not a budget for ``on_start``.
#: Watching until the phase settles is ``--wait``, and that watch is not
#: this timer.
_DEPLOY_HTTP_TIMEOUT_S = DEFAULT_TIMEOUT_S

#: How often ``--wait`` polls ``GET /sts/sessions/{id}``. The watch has
#: no client-side deadline: ``on_start`` may run up to 3600s (F12), and
#: the controller is what ends a start that hangs.
#: Default; adjust from measurement (Appendix D).
_WAIT_POLL_S = 1.0

#: Consecutive failed polls (unreachable or 5xx) before ``--wait`` gives
#: up. The count that adds up to
#: :data:`~mftik.cli.client.DEFAULT_TIMEOUT_S` at :data:`_WAIT_POLL_S`.
#: Default; adjust from measurement (Appendix D).
_WAIT_MAX_MISSES = round(DEFAULT_TIMEOUT_S / _WAIT_POLL_S)

#: What :func:`run_wait_action` returns. ``wait`` means keep watching,
#: ``tail`` means the phase has settled, ``return_id`` means ``--no-wait``
#: is done.
RunWaitStep = Literal["wait", "tail", "return_id"]

#: What :func:`run_disposition` returns once the flags are applied.
#: ``report`` prints the outcome and does not open the log socket.
RunDisposition = Literal["watch", "tail", "report", "return_id"]

_WAIT_PHASES = frozenset({"pending", "starting", "restarting", "stopping"})
_TAIL_PHASES = frozenset({"running", "failed", "done"})

#: The clock the watch sleeps on. Tests replace it with a
#: :class:`~mftik.clock.FakeClock`. Production uses the process clock.
_clock: Clock = SystemClock()


def run_wait_action(status: str, *, wait: bool) -> RunWaitStep:
    """What ``mftik run`` does with one status snapshot (F12, §5.2).

    * **W1.** ``wait`` true: ``running``, ``failed`` and ``done`` →
      ``"tail"``. ``pending``, ``starting``, ``restarting`` and
      ``stopping`` → ``"wait"``. Any other word also → ``"wait"``: an
      unknown phase keeps watching, and the command prints it once.
      The deploy's 202 is ``starting``; that is not the end of the watch.
    * **W2.** ``wait`` false (``--no-wait``): ``"return_id"`` for every
      status. No tail.
    * **W3.** This is not the deploy POST's timer. That timer is
      :data:`_DEPLOY_HTTP_TIMEOUT_S`, one HTTP hop.

    ``stopping`` keeps watching and ``done`` tails. The plan says the
    watch ends on ``running`` or ``failed`` and does not name these two;
    this is the choice #293 recorded.
    """
    if not wait:
        return "return_id"
    if status in _TAIL_PHASES:
        return "tail"
    # Known watch words and any unknown word both keep watching. The
    # unknown word is printed once by the watch, not on every poll.
    if status in _WAIT_PHASES:
        return "wait"
    return "wait"


def run_disposition(
    phase: str, *, wait: bool, no_follow: bool
) -> RunDisposition:
    """Flags applied to one snapshot (#293).

    ``--no-wait`` (``wait`` false) returns the id. ``--no-follow`` does
    not change that. ``--wait`` with ``--no-follow`` still watches; a
    settled ``running`` is reported rather than tailed. ``failed`` and
    ``done`` are always reported from the stored log, not tailed.
    """
    step = run_wait_action(phase, wait=wait)
    if step == "return_id":
        return "return_id"
    if step == "wait":
        return "watch"
    if no_follow or phase != "running":
        return "report"
    return "tail"


def _sleep_sync(clock: Clock, seconds: float) -> None:
    """Sleep ``seconds`` on ``clock`` from this synchronous command.

    A :class:`~mftik.clock.FakeClock` is advanced by the sleep it was
    asked for, so a test does not wait on the wall clock. The process
    clock sleeps for real.
    """
    asyncio.run(_sleep_on(clock, seconds))


async def _sleep_on(clock: Clock, seconds: float) -> None:
    if isinstance(clock, FakeClock):
        task = asyncio.create_task(clock.sleep(seconds))
        await asyncio.sleep(0)
        if not task.done():
            clock.advance(seconds)
        await task
        return
    await clock.sleep(seconds)


def _deploy_may_be_live(exc: BaseException) -> str:
    return (
        f"{exc}\n"
        "A session may already be live — check with: mftik ps\n"
        "Do not run again until you know."
    )


def _phase_of(body: object, fallback: str) -> str:
    """Prefer the v2 ``phase`` field. The column word is the fallback.

    A live row's ``status`` stays ``live`` for every non-terminal phase,
    so reading it instead of ``phase`` would watch forever.
    """
    if isinstance(body, dict):
        raw = body.get("phase")
        if isinstance(raw, str) and raw:
            return raw
        status = body.get("status")
        if isinstance(status, str) and status:
            return status
    return fallback


def _condition_pairs(conditions: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(conditions, dict):
        return ()
    return tuple(
        sorted(
            (str(key), str(value))
            for key, value in conditions.items()
            if key != "phase"
        )
    )


def _progress_line(phase: str, conditions: object) -> str:
    pairs = _condition_pairs(conditions)
    if not pairs:
        return f"  {phase}"
    detail = " ".join(f"{key}={value}" for key, value in pairs)
    return f"  {phase}  {detail}"


def _lost_contact(session_id: str) -> CliError:
    return CliError(
        f"lost contact while waiting for session {session_id}.\n"
        "It may still be running.\n"
        f"Stop it with: mftik stop {session_id}"
    )


def _watch(
    client: Client, session_id: str, phase: str, body: dict
) -> tuple[str, dict]:
    """Poll until :func:`run_wait_action` says the phase has settled.

    The deploy body is the first snapshot. ``running`` / ``failed`` /
    ``done`` on that body return without a poll: a node that only
    answers the deploy must still be followable. A 404 on the session
    is an error at once. An unreachable node or a 5xx is retried; after
    :data:`_WAIT_MAX_MISSES` consecutive misses the command stops and
    says the session may still be running.
    """
    seen: tuple[str, tuple[tuple[str, str], ...]] | None = None
    misses = 0
    while True:
        phase = _phase_of(body, phase)
        conditions = body.get("conditions")
        key = (phase, _condition_pairs(conditions))
        if key != seen:
            print(_progress_line(phase, conditions))
            seen = key
        if run_wait_action(phase, wait=True) == "tail":
            return phase, body
        _sleep_sync(_clock, _WAIT_POLL_S)
        try:
            nxt = client.get(f"/sts/sessions/{session_id}")
        except NodeUnreachable:
            misses += 1
            if misses >= _WAIT_MAX_MISSES:
                raise _lost_contact(session_id) from None
            continue
        except CliError as exc:
            if exc.status == 404:
                raise CliError(
                    f"session {session_id} was not found while waiting",
                    status=404,
                ) from exc
            if exc.status is not None and exc.status >= 500:
                misses += 1
                if misses >= _WAIT_MAX_MISSES:
                    raise _lost_contact(session_id) from exc
                continue
            raise
        if not isinstance(nxt, dict):
            raise CliError(
                f"session {session_id} did not answer with an object"
            )
        misses = 0
        body = nxt


def _report_settled(
    client: Client, session_id: str, phase: str, snapshot: dict
) -> int:
    """Print a settled session that this command is not going to tail."""
    if phase == "running":
        print(f"  left running — stop it with: mftik stop {session_id}")
        return 0
    reason = snapshot.get("reason")
    reason_text = reason.strip() if isinstance(reason, str) else ""
    if phase == "failed":
        if reason_text:
            print(f"  session failed: {reason_text}")
        else:
            print("  session failed")
    elif reason_text:
        print(f"  session {phase}: {reason_text}")
    else:
        print(f"  session {phase}")
    render_log_page(client.get(f"/logs/sts/{session_id}"))
    if phase == "failed":
        return EXIT_ERROR
    return 0


def _finish(
    client: Client,
    session_id: str,
    phase: str,
    snapshot: dict,
    *,
    no_follow: bool,
) -> int:
    action = run_disposition(phase, wait=True, no_follow=no_follow)
    if action == "tail":
        print("  ^C stops this session")
        try:
            follow_logs(client, session_id)
        except KeyboardInterrupt:
            return _stop_on_interrupt(client, session_id)
        return 0
    return _report_settled(client, session_id, phase, snapshot)


def run(args: argparse.Namespace) -> int:
    # ``False`` is ``--no-wait``. Anything else, including a caller that
    # left the attribute unset, is ``--wait``: that is the default.
    wait = getattr(args, "wait", True) is not False
    root = require_tree(args.path)
    inspected = inspect_tree(root)
    yaml_text = read_yaml(args.cfg, root)
    # Parsed here as well as there. The node would refuse the same document,
    # but only after the tree has been copied into its registry — and a push
    # that lands for a deploy that cannot is a confusing half-step.
    try:
        parse_strategy_yml(yaml_text)
    except StrategyYamlError as exc:
        raise CliError(str(exc)) from exc

    key = qualify(PRIVATE_ORIGIN, inspected.cls.type)
    _, client = connected(args.profile, timeout=_DEPLOY_HTTP_TIMEOUT_S)
    with client:
        if not args.no_push:
            report_push(push_tree(client, root))

        try:
            deployed = client.post(
                f"/sts/deploy/{key}", json_body={"yaml": yaml_text}
            )
        except KeyboardInterrupt:
            # The POST is in flight. STS may already have created the
            # session; this side will never hear the id.
            raise CliError(
                "interrupted during deploy.\n"
                "A session may already be live — check with: mftik ps\n"
                "Do not run again until you know."
            ) from None
        except NodeUnreachable as exc:
            raise NodeUnreachable(_deploy_may_be_live(exc)) from exc
        except CliError as exc:
            # A missing extra never created a session. Saying one may already
            # be live turns an environment problem into a hunt for a ghost.
            if is_environment_refusal(exc):
                raise
            raise CliError(_deploy_may_be_live(exc)) from exc
        if not isinstance(deployed, dict) or not deployed.get("session_id"):
            raise CliError("deploy did not return a session id")
        session_id = str(deployed["session_id"])
        status = _phase_of(deployed, "")
        print(f"running {key} session={session_id}")

        if not wait:
            print(f"  stop it with: mftik stop {session_id}")
            return 0

        try:
            phase, snapshot = _watch(client, session_id, status, deployed)
        except KeyboardInterrupt:
            return _stop_on_interrupt(client, session_id)
        return _finish(
            client,
            session_id,
            phase,
            snapshot,
            no_follow=bool(args.no_follow),
        )


def _stop_on_interrupt(client: Client, session_id: str) -> int:
    """Send the stop the Ctrl-C asked for, and report what became of it.

    The second Ctrl-C lands here, while the stop is in flight. It leaves the
    session running, which is worth saying at length: the strategy is still
    holding whatever it was holding, and nothing else is going to mention it.
    """
    print(f"\nstopping {session_id} (^C again to leave it running)")
    try:
        out = client.post(f"/sts/sessions/{session_id}/stop")
    except KeyboardInterrupt:
        print(
            f"\nleft {session_id} running. It is still trading.\n"
            f"Stop it with: mftik stop {session_id}"
        )
        return EXIT_INTERRUPTED
    except CliError as exc:
        # The stop did not land, and the session is presumed up. Saying so is
        # the whole value here — a failure that read as "stopped" would be the
        # worst possible outcome of pressing Ctrl-C.
        raise CliError(
            f"could not stop {session_id}: {exc}\n"
            f"It may still be running — check with: mftik ps"
        ) from exc
    status = out.get("status") if isinstance(out, dict) else None
    print(f"stopped {session_id} status={status}")
    return EXIT_INTERRUPTED
