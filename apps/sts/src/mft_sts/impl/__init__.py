"""Strategy implementations loaded by the STS runtime."""

from __future__ import annotations

from mft_sts.impl.chase import ChaseOrder
from mft_sts.impl.noop import NoopStrategy
from mft_sts.impl.oco import OneCancelOther
from mft_sts.strategy import Strategy

# Keys: short ``name`` (e.g. noop) and class ``__name__`` (e.g. NoopStrategy).
_REGISTRY: dict[str, type[Strategy]] = {}
DEFAULT_STRATEGY = NoopStrategy.name


def register(cls: type[Strategy]) -> type[Strategy]:
    """Register a strategy class by ``name`` and ``__name__``."""
    _REGISTRY[cls.name] = cls
    _REGISTRY[cls.__name__] = cls
    return cls


register(NoopStrategy)
register(ChaseOrder)
register(OneCancelOther)


def resolve(name: str | None) -> Strategy:
    """Build a strategy instance for a registered name or class type."""
    key = name or DEFAULT_STRATEGY
    cls = _REGISTRY.get(key)
    if cls is None:
        known = ", ".join(sorted(set(_REGISTRY))) or "(none)"
        raise KeyError(f"unknown strategy {key!r}; known: {known}")
    return cls()


def resolve_class(name: str | None) -> type[Strategy]:
    """Return the strategy class for a registered name or class type."""
    key = name or DEFAULT_STRATEGY
    cls = _REGISTRY.get(key)
    if cls is None:
        known = ", ".join(sorted(set(_REGISTRY))) or "(none)"
        raise KeyError(f"unknown strategy {key!r}; known: {known}")
    return cls


def known_strategies() -> list[str]:
    """Return distinct short names (prefer ``Strategy.name`` over class name)."""
    return sorted({cls.name for cls in _REGISTRY.values()})


def known_strategy_types() -> list[str]:
    """Return distinct class type names for strategy.yml ``sts.type``."""
    return sorted({cls.__name__ for cls in _REGISTRY.values()})
