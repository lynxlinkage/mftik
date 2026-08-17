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

from mftik.registry import RegistryStore, load_class, qualify
from mftik.registry.qualify import SEP
from mftik.strategy import Strategy

from mftik_sts.impl.chase import ChaseOrder
from mftik_sts.impl.cross_arb import CrossArb
from mftik_sts.impl.macd_dollar import MacdDollarBars
from mftik_sts.impl.noop import NoopStrategy
from mftik_sts.impl.oco import OneCancelOther
from mftik_sts.impl.tape_keeper import TapeKeeper
from mftik_sts.impl.twap import TwapStrategy

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


def _forget_missing(present: set[str]) -> list[str]:
    """Drop qualified keys whose tree is no longer in the store.

    Registration used to be add-only, which was invisible while the only way
    to lose a tree was to restart the process that had loaded it. It stopped
    being invisible once trees could be deleted and remotes disconnected
    underneath a running STS: the files went, the key stayed, and deploying
    it built a session from a class whose source was not on disk anywhere.

    Only qualified keys — the bundled strategies are this package, not the
    store, and nothing in the store's absence should unregister them.
    """
    stale = [
        key
        for key in _REGISTRY
        if key not in _BUILTIN_KEYS and SEP in key and key not in present
    ]
    for key in stale:
        del _REGISTRY[key]
    return stale


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

    Called from STS boot and again whenever the registry changes under a
    running process, not at import: tests that merely import this module must
    not scan whatever ``MFTIK_DATA`` the developer has on disk. A tree that
    fails to import is skipped so one broken add cannot take the process down.

    Re-registering is how a replaced tree takes effect. ``register_qualified``
    overwrites the key, so the next session built for it gets the new class;
    sessions already running keep the instance they were built with, which is
    the only safe answer — swapping a live strategy's class underneath it
    would leave its state bound to methods that no longer match.
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
                # Without this a reload returns the module the last one left
                # in sys.modules, and a re-pushed strategy runs its old code.
                digest=rec.digest,
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

    # A tree that failed to import is not in ``loaded``, and this drops its
    # key — which is right. It was loadable and now is not, and going on
    # answering to it would deploy the last version that happened to parse.
    for key in _forget_missing(set(loaded)):
        logger.info("unregistered %s: no longer in the registry", key)
    return loaded
