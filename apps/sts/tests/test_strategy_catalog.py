"""The catalogue and the registry have to agree.

``mftik.protocol.strategy_catalog`` lives in mftik-common so the API can serve it
without depending on STS, which means nothing in the type system stops the two
from drifting: a template can name a strategy that was never registered, or
carry an ``sts:`` block that the strategy itself would refuse. Either way the
UI hands the user a starting document that does not deploy, and the failure
lands at deploy time rather than here.
"""

from __future__ import annotations

import pytest
from mftik.protocol import all_templates, strategy_types
from mftik.protocol.strategy_yml import parse_strategy_yml
from mftik_sts.impl import resolve_class
from mftik_sts.impl.chase import ChaseOrder
from mftik_sts.impl.cross_arb import CrossArb
from mftik_sts.impl.noop import NoopStrategy
from mftik_sts.impl.oco import OneCancelOther
from mftik_sts.impl.twap import TwapStrategy


@pytest.mark.parametrize("type_name", strategy_types())
def test_every_template_names_a_registered_strategy(type_name: str) -> None:
    assert resolve_class(type_name).__name__ == type_name


@pytest.mark.parametrize("type_name", strategy_types())
def test_every_template_config_survives_its_own_strategy(type_name: str) -> None:
    """The ``sts:`` block is what the UI deploys, so it has to validate."""
    template = next(t for t in all_templates() if t.type == type_name)
    spec = parse_strategy_yml(template.yaml)
    paras = resolve_class(type_name).on_initialized(dict(spec.sts))
    assert paras


@pytest.mark.parametrize(
    "cls", [NoopStrategy, ChaseOrder, OneCancelOther, CrossArb, TwapStrategy]
)
def test_every_strategy_is_offered_in_the_catalogue(cls: type) -> None:
    """A strategy with no template cannot be deployed from the UI at all."""
    assert cls.__name__ in strategy_types()
