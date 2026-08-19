"""The import gate and on_initialized, without a node.

A push copies source a node will import. A deploy then runs that class's
``on_initialized`` against a ``strategy.yml``. Both refusals are local facts
about the tree and the document — reaching the node to hear them is a
round-trip that says nothing the laptop does not already know.

``--against`` is the exception, and it is opt-in for that reason. Whether a
*node* has the extras this tree declares is not a local fact, and the answer
changes when somebody applies one. Without the flag this command still says
nothing about any node, which is what makes it usable on a plane.
"""

from __future__ import annotations

import argparse
import traceback

from mftik.cli.client import CliError
from mftik.cli.tree import cfg_path, inspect_tree, require_tree
from mftik.protocol.strategy_yml import StrategyYamlError, parse_strategy_yml
from mftik.registry.digest import digest_files
from mftik.registry.load import load_class


def check(args: argparse.Namespace) -> int:
    root = require_tree(args.path)
    document = cfg_path(args.cfg, root)
    inspected = inspect_tree(root)

    spec = None
    if document is not None:
        try:
            spec = parse_strategy_yml(document.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError) as exc:
            raise CliError(f"cannot read {document}: {exc}") from exc
        except StrategyYamlError as exc:
            raise CliError(str(exc)) from exc

    digest = digest_files(inspected.files)
    try:
        cls = load_class(
            root,
            type_name=inspected.cls.type,
            source="check",
            name=inspected.name,
            digest=digest,
        )
    except Exception as exc:
        raise CliError(_from_your_code("importing the tree", exc, args)) from exc

    # Imported here so ``mftik connect`` does not pay for the strategy stack.
    from mftik.strategy import Strategy

    if not isinstance(cls, type) or not issubclass(cls, Strategy):
        raise CliError(f"{inspected.cls.type} is not a Strategy")
    if cls.name != inspected.name:
        raise CliError(
            f"{inspected.cls.type}.name is {cls.name!r} but the source "
            f"said {inspected.name!r}"
        )

    if spec is not None:
        try:
            cls.on_initialized(dict(spec.sts))
        except Exception as exc:
            raise CliError(
                _from_your_code(f"{cls.__name__}.on_initialized", exc, args)
            ) from exc

    print(f"ok  {inspected.name}  ({inspected.cls.type})")
    print(f"    {len(inspected.files)} file(s), {digest}")
    requires = tuple(inspected.cls.requires)
    if requires:
        print(f"    requires {', '.join(requires)}")
    if spec is not None:
        print(f"    config {document} accepted by on_initialized")
    else:
        # Silence here would read as "the config is fine", and there was none.
        print(
            "    no strategy.yml — imports and naming checked, "
            "parameters were not"
        )
    if getattr(args, "against", None):
        return _against_node(args, requires)
    return 0


def _against_node(args: argparse.Namespace, requires: tuple[str, ...]) -> int:
    """Compare this tree's ``requires`` with what a node has applied.

    The gate above proves the tree declared what it imports. This proves the
    node can supply it — the other half of the same question, and the one a
    laptop cannot answer on its own. A refusal here is the refusal ``push``
    would give, said before the files are sent.
    """
    from mftik.cli.client import connected

    _, client = connected(args.profile)
    with client:
        env = client.get("/environment")
    if not env.get("abi_ok", True):
        raise CliError(
            "that node's overlay was built for a different interpreter, so it "
            "has no usable extras until it applies again"
        )
    applied = set(env.get("packages") or {})
    missing = [name for name in requires if name not in applied]
    if not missing:
        print(f"    node has {len(applied)} extra(s); nothing missing")
        return 0

    present = {
        row["dist"]: row["version"]
        for row in env.get("installed") or []
        if not row.get("approved")
    }
    lines = []
    for name in missing:
        version = present.get(name) or present.get(name.replace("_", "-"))
        if version:
            lines.append(
                f"{name}: on that node at {version} as a dependency but not "
                f"approved — mftik env approve {name}"
            )
        else:
            lines.append(f"{name}: not on that node — mftik env add {name}")
    raise CliError("that node cannot run this tree:\n  " + "\n  ".join(lines))


def _from_your_code(during: str, exc: Exception, args: argparse.Namespace) -> str:
    """Report a failure raised inside the strategy, not by the platform.

    These two layers run the tree's own code, so the exception type and where
    it came from are the useful part — unlike the gate's refusals, which are
    complete sentences about the tree. ``--traceback`` prints the frames,
    because the message alone rarely locates a line.
    """
    label = f"{during} raised {type(exc).__name__}: {exc}"
    if getattr(args, "traceback", False):
        return label + "\n\n" + "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ).rstrip()
    return label + "\n(run again with --traceback to see where)"
