"""Who is making this request.

One shape for every proof. A handler asks the principal for a user id and
does not care whether a browser cookie, a script's API key or a peer node's
registry key produced it — but the middleware that built it does, and later
steps refuse a registry key on routes that are not the registry.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Everything the Owner can do, including changing who the Owner is and
#: minting credentials. Only a browser session gets this; it implies every
#: scope below rather than enumerating a list that would drift.
SCOPE_OWNER = "owner"

#: The domain routes — sessions, strategies, venues, the board. What a script
#: acting for the Owner needs, and nothing that would let it issue itself more
#: power or change how the Owner logs in.
SCOPE_API = "api"

#: Reading what this node publishes. The whole of what another node gets.
SCOPE_REGISTRY_READ = "registry:read"


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
    #: Set when a machine credential backed it, for audits and for revoking
    #: the thing that is currently making requests.
    key_id: int | None = None

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

    @classmethod
    def machine(
        cls,
        user_id: int,
        *,
        name: str,
        scopes: frozenset[str],
        key_id: int,
    ) -> Principal:
        """A key acting for the Owner. Never `owner`, however wide its scopes.

        The distinction is the whole point: audits have to be able to say a
        CI job did something rather than attributing it to a person who was
        asleep, and `/auth/keys` has to refuse a key that would otherwise mint
        itself a replacement nobody could revoke.
        """
        return cls(
            user_id=user_id,
            via=f"key:{name}",
            scopes=scopes,
            key_id=key_id,
        )


ANONYMOUS = Principal(user_id=None, via="anonymous")
