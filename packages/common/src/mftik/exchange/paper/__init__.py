"""Paper (fake) exchange connectivity for local testing and paper trading."""

from mftik.exchange.paper.engine import PaperExchange
from mftik.exchange.paper.private import PaperAuthError, PaperPrivateClient
from mftik.exchange.paper.public import PaperPublicClient

__all__ = [
    "PaperAuthError",
    "PaperExchange",
    "PaperPrivateClient",
    "PaperPublicClient",
]
