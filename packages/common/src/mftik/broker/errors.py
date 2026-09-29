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


class NoRespondersError(RequestTimeoutError):
    """Nobody was subscribed, so the ask stopped before the caller's timeout.

    This is not a handler that received the request and failed to answer.
    Callers that kill or give up on a full timeout must not treat it as one.
    """

    def __init__(self, subject: str, request_id: str, timeout: float) -> None:
        self.subject = subject
        self.request_id = request_id
        self.timeout = timeout
        BrokerError.__init__(
            self,
            f"nobody is subscribed to {subject!r} (id={request_id})",
        )
