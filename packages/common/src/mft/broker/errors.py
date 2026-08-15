"""Broker IPC errors."""


class BrokerError(Exception):
    """Base error for broker operations."""


class BrokerNotConnectedError(BrokerError):
    """Raised when an operation is attempted before connect()."""


class RequestTimeoutError(BrokerError):
    """Raised when a request-reply call exceeds its timeout."""

    def __init__(self, subject: str, request_id: str, timeout: float) -> None:
        self.subject = subject
        self.request_id = request_id
        self.timeout = timeout
        super().__init__(
            f"request to {subject!r} timed out after {timeout}s (id={request_id})"
        )
