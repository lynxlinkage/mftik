"""Failures the session worker names.

The lifecycle refusals are raised by the ingress and the runner.
:class:`SessionFailed` is raised by :meth:`Delivery.accept` when the
shared must-deliver queue is past its capacity.
"""

from __future__ import annotations


class SessionFailed(Exception):
    """A must-deliver queue overflowed, so the session fails (§5.3).

    TD events, ``feed_end`` and RPC replies are ``all`` and are not
    dropped. The event that did not fit is not marked ``dropped``; it was
    never accepted. ``reason`` is ``"{kind}_overflow"`` — ``td_overflow``,
    ``feed_end_overflow``, ``rpc_reply_overflow``.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


class IngressNotStarted(Exception):
    """The strategy thread was started before the ingress (I1)."""


class IngressEnded(Exception):
    """This ingress has already ended.

    It is not started again inside the process (I2). A new incarnation is
    a new process, and the controller is who decides to spawn one.
    """


class IngressNotMainThread(Exception):
    """``Ingress.start`` ran off the main thread (I3).

    Signal handlers only run on the main thread, so the ingress — the
    thing that has to see SIGTERM — has to be that thread.
    """


class StrategyStillRunning(Exception):
    """``Ingress.close`` ran while the strategy thread was still alive (I1).

    The ingress ends after the strategy, so ``on_stop`` still has a
    receiver for its cancels.
    """


class SignalHandlersReserved(Exception):
    """A strategy tried to install a signal handler (I3).

    Handlers would run on the main thread, and the main thread is the
    ingress. The SDK refuses; the strategy does not get the call.
    """
