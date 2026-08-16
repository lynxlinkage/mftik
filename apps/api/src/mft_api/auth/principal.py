"""Who is making this request.

One shape for every proof. A handler asks the principal for a user id and
does not care whether a browser cookie, a script's API key or a peer node's
registry key produced it — but the middleware that built it does, and later
steps refuse a registry key on routes that are not the registry.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Everything the Owner can do. Machine credentials get narrower sets; this
#: one implies them all rather than enumerating a list that would drift.
SCOPE_OWNER = "owner"


@dataclass(frozen=True, slots=True)
class Principal:
    #: None only for an anonymous request on a public route.
    user_id: int | None
    #: How it was proved: ``password``, later a provider name, ``key:<name>``
    #: for a machine credential, ``disabled`` when the gate is off. Written
    #: into audits so the trail distinguishes the Owner from a key acting as
    #: the Owner.
    via: str
    scopes: frozenset[str] = frozenset()
    #: Set when a browser session backed this, so logout knows what to delete.
    session_id: str | None = None

    @property
    def authenticated(self) -> bool:
        return self.user_id is not None

    def allows(self, scope: str) -> bool:
        return SCOPE_OWNER in self.scopes or scope in self.scopes

    @classmethod
    def owner(
        cls, user_id: int, *, via: str, session_id: str | None = None
    ) -> Principal:
        return cls(
            user_id=user_id,
            via=via,
            scopes=frozenset({SCOPE_OWNER}),
            session_id=session_id,
        )


ANONYMOUS = Principal(user_id=None, via="anonymous")
