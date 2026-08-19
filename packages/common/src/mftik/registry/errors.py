"""Failures while adding a strategy to a local registry."""

from __future__ import annotations


class RegistryError(ValueError):
    """The files cannot be recorded as a strategy. ``code`` is the HTTP shape."""

    code: str = "registry_error"

    def __init__(self, message: str) -> None:
        super().__init__(message)


class RegistryConflict(RegistryError):
    """A strategy of that name is already in that origin."""

    code = "registry_conflict"


class MissingRemoteExtras(RegistryError):
    """A new connect naming extras this node's stamp does not list.

    Carries the names as data. The message is for a person; a caller that
    wants to offer "import these" must not have to parse prose to find out
    which ones — that parsing broke the moment the sentence learned to
    mention versions.

    ``present`` is the subset already on the volume as somebody's
    dependency, at the version installed. Those need approving, not
    installing, and the difference is the whole of what the operator does
    next.
    """

    code = "missing_extras"

    def __init__(
        self,
        message: str,
        missing: tuple[str, ...],
        present: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.missing = missing
        self.present = dict(present or {})

    def rows(self) -> list[dict[str, str | None]]:
        """``[{name, version}]`` — ``version`` set when it is already here."""
        return [
            {"name": name, "version": self.present.get(name)}
            for name in self.missing
        ]
