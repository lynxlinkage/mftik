"""API routes."""

from mft_api.routes.apis import router as apis_router
from mft_api.routes.audits import router as audits_router
from mft_api.routes.health import router as health_router
from mft_api.routes.logs import router as logs_router
from mft_api.routes.md import router as md_router
from mft_api.routes.stats import router as stats_router
from mft_api.routes.sts import router as sts_router
from mft_api.routes.sym import router as sym_router
from mft_api.routes.td import router as td_router

__all__ = [
    "apis_router",
    "audits_router",
    "health_router",
    "logs_router",
    "md_router",
    "stats_router",
    "sts_router",
    "sym_router",
    "td_router",
]
