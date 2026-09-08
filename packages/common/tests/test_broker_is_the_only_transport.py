"""No domain reaches past the broker to the transport underneath it.

`Broker` has always been the IPC layer in the sense that everything goes
*through* it, and not in the sense that anything stopped a caller going
around it: `broker.redis` was a live client, and three callers used it —
liveness keys, TD's backfill lock, STS's cid slot — each building its own key
out of `config.key_prefix` and each reasoning about Redis semantics on its
own. Two of them documented the same missing compare-and-set separately.

Closing those is what made the NATS transport a change to one package rather
than a hunt through every file, and this is what keeps the second one from
being harder than the first. A leak is not a style complaint: it is a call
site that the next port has to find by reading everything, and the cost of
missing one is a plane still talking to a store nobody else is using.

So the rule is checked rather than agreed. Anything under an `src` tree may
use the broker's own vocabulary and nothing below it:

* no `redis` or `nats` import, so no second client and no store's exception
  types spelled out in a domain's error handling;
* no `.redis`, `.js` or `.nc` attribute access — the escape hatches
  themselves, one per transport;
* no `.key_prefix` attribute access, because a name a caller builds is a name
  the broker cannot change. Under Redis that was a key it could not reshape;
  under NATS the prefix is a subject root, a stream name and a KV bucket at
  once, and a caller's flat string is not any of them.

`packages/common/src/mftik/broker/` is the exception: the transports *are* the
store-specific code, and everything this file forbids elsewhere is what they
are for.

Only `src` trees are read. `scripts/` is deliberately outside: `redacted_url`
is a Redis credential's problem and names it on purpose. Tests are outside too
— several inject failures a transport-neutral surface has no way to express,
and `broker_harness` reaches through on purpose so the tests above it do not
have to.
"""

from __future__ import annotations

import ast
from pathlib import Path

#: The tree, found from this file rather than from the working directory so
#: the check is the same under `pytest packages` and `pytest` at the root.
ROOT = Path(__file__).resolve().parents[3]

#: Where a store's client is allowed to be touched: the transports themselves.
IMPLEMENTATION = ROOT / "packages" / "common" / "src" / "mftik" / "broker"

#: Attribute names that only the implementation may read. ``redis`` is Redis'
#: client; ``js`` and ``nc`` are NATS' JetStream context and connection.
FORBIDDEN_ATTRIBUTES = ("redis", "js", "nc", "key_prefix")

#: Import roots that only the implementation may name.
FORBIDDEN_MODULES = ("redis", "fakeredis", "nats")

#: A floor, so a glob that finds nothing fails instead of passing. Roughly a
#: third of the tree at the time of writing — low enough not to need
#: revisiting, high enough that a broken scan cannot slip through.
MIN_FILES_SCANNED = 100


def _sources() -> list[Path]:
    trees = [*(ROOT / "apps").glob("*/src"), *(ROOT / "packages").glob("*/src")]
    return [
        path
        for tree in trees
        for path in sorted(tree.rglob("*.py"))
        if IMPLEMENTATION not in path.parents
    ]


def _root_module(name: str) -> str:
    return name.partition(".")[0]


def _leaks(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_ATTRIBUTES:
            found.append(
                f"{path.relative_to(ROOT)}:{node.lineno} reads .{node.attr}"
            )
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if _root_module(alias.name) in FORBIDDEN_MODULES:
                    found.append(
                        f"{path.relative_to(ROOT)}:{node.lineno} "
                        f"imports {alias.name}"
                    )
        elif isinstance(node, ast.ImportFrom):
            if node.module and _root_module(node.module) in FORBIDDEN_MODULES:
                found.append(
                    f"{path.relative_to(ROOT)}:{node.lineno} "
                    f"imports from {node.module}"
                )
    return found


def test_the_scan_reaches_the_tree() -> None:
    """A guard that checked nothing would pass every time."""
    files = _sources()

    assert len(files) >= MIN_FILES_SCANNED, (
        f"only {len(files)} source files found under {ROOT} — "
        "the scan is looking in the wrong place"
    )


def test_no_domain_talks_to_a_store_directly() -> None:
    leaks = [leak for path in _sources() for leak in _leaks(path)]

    assert leaks == [], (
        "the transport is the broker's business, and these go around it:\n  "
        + "\n  ".join(leaks)
        + "\n\nThe broker has a family for each of these — leases, counters, "
        "shared state, tape, fan-out, request-reply. If none of them fits, add "
        "one to BrokerTransport and implement it on both sides rather than "
        "reaching for one store's command here: a caller that does is a caller "
        "that only works on the transport it was written against."
    )
