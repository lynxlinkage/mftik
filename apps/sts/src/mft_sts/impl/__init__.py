"""Strategy implementations loaded by the STS runtime."""

from __future__ import annotations

from mft_sts.impl.noop import NoopStrategy
from mft_sts.strategy import Strategy

_REGISTRY: dict[str, type[Strategy]] = {
    NoopStrategy.name: NoopStrategy,
}

DEFAULT_STRATEGY = NoopStrategy.name


def register(cls: type[Strategy]) -> type[Strategy]:
    """Register a strategy class by its ``name`` (decorator or direct call)."""
    _REGISTRY[cls.name] = cls
    return cls


def resolve(name: str | None) -> Strategy:
    """Build a strategy instance for the given registered name."""
    key = name or DEFAULT_STRATEGY
    cls = _REGISTRY.get(key)
    if cls is None:
        known = ", ".join(sorted(_REGISTRY)) or "(none)"
        raise KeyError(f"unknown strategy {key!r}; known: {known}")
    return cls()


def known_strategies() -> list[str]:
    return sorted(_REGISTRY)
