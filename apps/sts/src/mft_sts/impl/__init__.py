"""Strategy implementations loaded by the STS runtime.

Renaming one of these is a migration. Both spellings registered below are
stored in the database — ``sts_sessions.strategy`` keeps the short ``name``
and ``strategies.type`` keeps the class name — so a rename that touches only
this file leaves rows naming a strategy nothing will answer to again, and the
rebuild scan can do nothing with them but skip them. See
``0020_macd_dollar_rename``, which is the one that had to be written after the
fact.
"""

from __future__ import annotations

import logging
from pathlib import Path

from mft.registry import RegistryStore, load_class, qualify

from mft_sts.impl.chase import ChaseOrder
from mft_sts.impl.cross_arb import CrossArb
from mft_sts.impl.macd_dollar import MacdDollarBars
from mft_sts.impl.noop import NoopStrategy
from mft_sts.impl.oco import OneCancelOther
from mft_sts.impl.tape_keeper import TapeKeeper
from mft_sts.impl.twap import TwapStrategy
from mft_sts.strategy import Strategy

# Keys: short ``name`` (e.g. noop) and class ``__name__`` (e.g. NoopStrategy).
_REGISTRY: dict[str, type[Strategy]] = {}
DEFAULT_STRATEGY = NoopStrategy.name


def register(cls: type[Strategy]) -> type[Strategy]:
    """Register a bundled strategy class by ``name`` and ``__name__``."""
    _REGISTRY[cls.name] = cls
    _REGISTRY[cls.__name__] = cls
    return cls


def register_qualified(cls: type[Strategy], key: str) -> type[Strategy]:
    """Register a registry tree under a qualified type key only.

    Bundled strategies keep the short keys. A pulled copy of the same class
    must not also claim ``HelloStrategy``, or the second origin would vanish.
    """
    _REGISTRY[key] = cls
    return cls


register(NoopStrategy)
register(ChaseOrder)
register(OneCancelOther)
register(CrossArb)
register(TwapStrategy)
register(TapeKeeper)
register(MacdDollarBars)

#: Names and class types shipped in this package. A local tree that reuses
#: one of these would silently replace the bundled implementation on
#: ``resolve``, so ``load_local_registry`` refuses rather than overwrite.
_BUILTIN_KEYS: frozenset[str] = frozenset(_REGISTRY)
logger = logging.getLogger(__name__)


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


def load_local_registry(store: RegistryStore | None = None) -> list[str]:
    """Import ``local/`` and ``pulled/`` trees under qualified type keys.

    Called from STS boot, not at import: tests that merely import this module
    must not scan whatever ``MFT_DATA`` the developer has on disk. A tree that
    fails to import is skipped so one broken add cannot take the process down.
    """
    store = store or RegistryStore.from_env()
    loaded: list[str] = []
    for rec in store.list_all():
        key = qualify(rec.origin, rec.type)
        try:
            cls = load_class(
                Path(rec.path),
                type_name=rec.type,
                source=rec.origin,
                name=rec.name,
            )
        except Exception:
            logger.exception(
                "skipped %s strategy %s at %s", rec.origin, rec.name, rec.path
            )
            continue
        if not isinstance(cls, type) or not issubclass(cls, Strategy):
            logger.error(
                "skipped %s strategy %s: %s is not a Strategy",
                rec.origin,
                rec.name,
                rec.type,
            )
            continue
        if cls.name in _BUILTIN_KEYS or cls.__name__ in _BUILTIN_KEYS:
            logger.error(
                "skipped %s strategy %s: name %r / type %r collides with "
                "a bundled strategy",
                rec.origin,
                rec.name,
                cls.name,
                cls.__name__,
            )
            continue
        register_qualified(cls, key)
        loaded.append(key)
        logger.info(
            "loaded %s strategy %s type=%s digest=%s",
            rec.origin,
            rec.name,
            key,
            rec.digest,
        )
    return loaded
