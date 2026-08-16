"""In-app authentication for the API gateway. See docs/Auth.md.

One Owner, proved several ways. This step is the gate itself plus the
username/password proof; machine credentials and OAuth follow.
"""

from mft_api.auth.deps import OwnerId, PrincipalDep, current_user_id, get_principal
from mft_api.auth.middleware import AuthMiddleware, auth_enabled, is_public
from mft_api.auth.principal import ANONYMOUS, SCOPE_OWNER, Principal
from mft_api.auth.routes import router as auth_router

__all__ = [
    "ANONYMOUS",
    "AuthMiddleware",
    "OwnerId",
    "Principal",
    "PrincipalDep",
    "SCOPE_OWNER",
    "auth_enabled",
    "auth_router",
    "current_user_id",
    "get_principal",
    "is_public",
]
