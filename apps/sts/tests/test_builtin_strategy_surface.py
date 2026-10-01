"""Every bundled strategy still loads against the SDK IF-06 defines.

The hooks the SDK grew are overridable, so a strategy that ignores them is
unaffected — but ``on_ready`` changed shape (it now takes the readiness report,
F12), and six of the seven override it. A signature that no longer matches is
not an import error: it is a ``TypeError`` at the one moment a session goes
live, after the deploy was accepted and the feeds were opened.

So this checks what the registry does at deploy time — resolve the key, build an
instance — for every bundled strategy, and that each one's ``on_ready`` can be
called the way the platform will call it. Registry trees on disk are not
covered: they are source somebody else owns, and B5-08 is where the hook rewrite
reaches them.
"""

from __future__ import annotations

import inspect

import pytest
from mftik.strategy import Ready, Strategy
from mftik_sts.impl import registered_keys, resolve, resolve_class


@pytest.mark.parametrize("key", registered_keys())
def test_every_bundled_strategy_instantiates(key: str) -> None:
    """What ``resolve`` does on every deploy, with no session in sight."""
    strategy = resolve(key)
    assert isinstance(strategy, Strategy)
    assert strategy.session is None
    assert strategy.registry_key == key


@pytest.mark.parametrize("key", registered_keys())
def test_every_bundled_on_ready_takes_the_readiness_report(key: str) -> None:
    sig = inspect.signature(resolve_class(key).on_ready)
    bound = sig.bind(resolve(key), Ready())
    assert bound.args[1:] == (Ready(),)


@pytest.mark.parametrize("key", registered_keys())
def test_every_bundled_strategy_has_the_new_accessors(key: str) -> None:
    """``self.md`` / ``self.td`` are bound in ``__init__``, so a strategy that
    never binds a session can still ask and get a null answer."""
    strategy = resolve(key)
    assert strategy.md.state("ticker.Paper_Spot_BTCUSDT") is None
    assert strategy.td.state(1) is None
    assert strategy.hook_slow.count == 0
