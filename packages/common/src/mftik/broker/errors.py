"""Broker IPC errors."""


class BrokerError(Exception):
    """Base error for broker operations."""


class BrokerNotConnectedError(BrokerError):
    """Raised when an operation is attempted before connect()."""


class StateReadIncompleteError(BrokerError):
    """Raised when a state read could not deliver everything the store held.

    Its own error rather than an empty or partial answer, because the callers are
    a strategy's ledger and its open orders: a book missing rows is
    indistinguishable from a book that small, and acting on the difference is
    placing an order twice or hedging a position that is already flat.
    """


class RequestTimeoutError(BrokerError):
    """Raised when a request-reply call exceeds its timeout."""

    def __init__(self, subject: str, request_id: str, timeout: float) -> None:
        self.subject = subject
        self.request_id = request_id
        self.timeout = timeout
        super().__init__(
            f"request to {subject!r} timed out after {timeout}s (id={request_id})"
        )


class StreamShapeError(BrokerError):
    """Raised when a live stream's shape is not one this build can move it to.

    ``_ensure_stream`` widens a stream that already exists, which is what an
    operator raising a retention limit wants. Some differences are not widenings
    and the server refuses them outright — a TTL flag cannot be turned off once
    a stream has it, whatever the config says next.

    Its own error because of where the refusal lands. It comes out of
    ``connect()``, so it is not one call failing but every plane failing to
    start, and only on servers that already hold the stream — never on the fresh
    one a developer tests against. The message names the fields that differ so
    that is the first thing read rather than the last thing deduced.
    """
