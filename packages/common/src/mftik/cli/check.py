"""The import gate and on_initialized, without a node.

A push copies source a node will import. A deploy then runs that class's
``on_initialized`` against a ``strategy.yml``. Both refusals are local facts
about the tree and the document — reaching the node to hear them is a
round-trip that says nothing the laptop does not already know.
"""

from __future__ import annotations

import argparse
import traceback
from pathlib import Path

from mftik.cli.client import CliError
from mftik.protocol.strategy_yml import StrategyYamlError, parse_strategy_yml
from mftik.registry.digest import digest_files
from mftik.registry.errors import RegistryError
from mftik.registry.files import read_tree
from mftik.registry.inspect import inspect_files
from mftik.registry.load import load_class


def check(args: argparse.Namespace) -> int:
    root = Path(args.path)
    if root.is_file():
        raise CliError(f"{root} is a file — give the strategy directory")
    if not root.is_dir():
        raise CliError(f"strategy tree does not exist: {root}")

    cfg_path = _cfg_path(args.cfg, root)

    try:
        inspected = inspect_files(read_tree(root))
    except RegistryError as exc:
        raise CliError(str(exc)) from exc

    spec = None
    if cfg_path is not None:
        try:
            spec = parse_strategy_yml(cfg_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError) as exc:
            raise CliError(f"cannot read {cfg_path}: {exc}") from exc
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
    if spec is not None:
        print(f"    config {cfg_path} accepted by on_initialized")
    else:
        # Silence here would read as "the config is fine", and there was none.
        print(
            "    no strategy.yml — imports and naming checked, "
            "parameters were not"
        )
    return 0


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


def _cfg_path(cfg: str | None, root: Path) -> Path | None:
    if cfg is not None:
        path = Path(cfg)
        if not path.is_file():
            raise CliError(f"strategy.yml does not exist: {path}")
        return path
    candidate = root / "strategy.yml"
    if candidate.is_file():
        return candidate
    return None
