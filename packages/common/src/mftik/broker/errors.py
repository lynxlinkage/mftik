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
