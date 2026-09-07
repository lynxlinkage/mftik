"""No domain reaches past the broker to the transport underneath it.

`Broker` has always been the IPC layer in the sense that everything goes
*through* it, and not in the sense that anything stopped a caller going
around it: `broker.redis` is a live client, and three callers used it —
liveness keys, TD's backfill lock, STS's cid slot — each building its own key
out of `config.key_prefix` and each reasoning about Redis semantics on its
own. Two of them documented the same missing compare-and-set separately.

That matters now for one reason: the transport is meant to become NATS
JetStream. A leak is not a style complaint there, it is a call site that a
port has to find by reading every file, and the cost of missing one is a
plane still talking to a Redis nobody else is using any more.

So the rule is checked rather than agreed. Anything under an `src` tree may
use the broker's own vocabulary and nothing below it:

* no `redis` import, so no second client and no Redis exception types
  spelled out in a domain's error handling;
* no `.redis` attribute access, which is the escape hatch itself;
* no `.key_prefix` attribute access, because a key shape a caller builds is
  a key shape the broker cannot change — and under JetStream, cannot honour
  at all, where names are buckets and streams rather than one flat keyspace.

`packages/common/src/mftik/broker/` is the exception: that *is* the Redis
implementation, and everything this file forbids elsewhere is what it is for.

Only `src` trees are read. `scripts/` is deliberately outside: `loop_bench`
times Redis itself and `redacted_url` is a Redis credential's problem, so
both name it on purpose. Tests are outside too — several of them inject
failures a transport-neutral surface has no way to express, and a test that
mocks `blpop` is describing this broker rather than reaching around it.
"""

from __future__ import annotations

import ast
from pathlib import Path

#: The tree, found from this file rather than from the working directory so
#: the check is the same under `pytest packages` and `pytest` at the root.
ROOT = Path(__file__).resolve().parents[3]

#: Where the Redis client is allowed to be touched: the Redis client.
IMPLEMENTATION = ROOT / "packages" / "common" / "src" / "mftik" / "broker"

#: Attribute names that only the implementation may read.
FORBIDDEN_ATTRIBUTES = ("redis", "key_prefix")

#: Import roots that only the implementation may name.
FORBIDDEN_MODULES = ("redis", "fakeredis")

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


def test_no_domain_talks_to_redis_directly() -> None:
    leaks = [leak for path in _sources() for leak in _leaks(path)]

    assert leaks == [], (
        "the transport is the broker's business, and these go around it:\n  "
        + "\n  ".join(leaks)
        + "\n\nThe broker has a primitive for each of these — leases, "
        "counters, state hashes, tape, pub/sub, request-reply. If none of "
        "them fits, add one there rather than a Redis command here: it is "
        "what the JetStream implementation will be written against."
    )
