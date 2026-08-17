"""Exchange connectivity errors."""


class ExchangeError(Exception):
    """Base error for exchange clients."""


class ExchangeNotConnectedError(ExchangeError):
    """Raised when an API is used before connect()."""


class InstrumentNotFoundError(ExchangeError):
    """Raised when a symbol is unknown to the venue."""


class OrderError(ExchangeError):
    """Raised when an order request is rejected."""


class InsufficientBalanceError(OrderError):
    """Raised when free balance cannot cover an order."""
