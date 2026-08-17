"""Scaffold a strategy that runs as generated.

The value here is entirely in the ``strategy.yml``. A template with
``td: [<your-account>]`` and a made-up ticker in it is a template that has to
be corrected before it does anything, and correcting it means finding out
which accounts this node has and which instruments its symbol plane knows —
which is what the node is for asking. So this asks, and writes the answers.

``--offline`` skips that and leaves placeholders, for scaffolding without a
node to hand. It says so, because a document full of placeholders that looked
finished would be worse than no document.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from mftik.cli.client import CliError, connected

#: Every venue provides this one. ``ticker`` and ``trade`` are in MD's
#: connector protocol too, but paper — the venue a new node actually has
#: credentials for — serves neither today, and a scaffold that cannot run on
#: the venue you already have is a scaffold that teaches nothing.
FEED_TOPIC = "orderbook"

#: What goes in the document when there is nothing real to put there. Shaped
#: so the refusal names it: an account by this name will not resolve, and the
#: error says which name it could not find.
PLACEHOLDER_ACCOUNT = "<no account on this node — add one first>"
PLACEHOLDER_FEED = "orderbook.Paper_Spot_BTCUSDT"

_SLUG = re.compile(r"[^a-z0-9_]+")

_STRATEGY_PY = '''\
"""{title} — a starting point.

Every hook is optional; this one reads the book and stops after a few
updates so a first run ends by itself. See ``Strategy`` for the full set:
order entry through ``self.oms``, balances through ``self.ledger``, history
through ``self.mds`` and ``self.tape``, instrument filters through
``self.symbols``, and scheduling through ``self.timer``.
"""

from typing import Any

from mftik.strategy import Strategy


class {cls}(Strategy):
    name = "{name}"

    @classmethod
    def on_initialized(cls, params: Any) -> dict[str, Any]:
        """Validate ``sts:`` before the session starts.

        Raising here refuses the deploy, and ``mftik check`` runs it on your
        machine — so a bad number is caught before anything reaches a venue.
        """
        out = super().on_initialized(params)
        stop_after = int(out.get("stop_after", 5))
        if stop_after <= 0:
            raise ValueError(f"stop_after must be positive, got {{stop_after}}")
        out["stop_after"] = stop_after
        return out

    def __init__(self) -> None:
        super().__init__()
        self._seen = 0

    async def on_start(self) -> None:
        await self.log(f"started; will exit after {{self.paras['stop_after']}} books")

    async def on_order_book(self, book) -> None:
        """One full snapshot per update — MD does not forward depth diffs."""
        if not book.bids or not book.asks:
            return
        self._seen += 1
        await self.log(
            f"#{{self._seen}} {{book.symbol}} "
            f"{{book.bids[0].price}} / {{book.asks[0].price}}"
        )
        if self._seen >= self.paras["stop_after"]:
            self.exit("seen_enough")
'''

_STRATEGY_YML = """\
# Deploy document. `mftik run` sends this; `mftik check` validates it.
#
# td:  account names, as this node knows them (mftik whoami's node, /apis)
# md:  feeds, each `topic.UniversalTicker`
# sts: your own parameters — on_initialized above decides what is valid
td: [{td}]
md: [{md}]
restart: never
sts:
  stop_after: 5
"""


def init(args: argparse.Namespace) -> int:
    root = Path(args.path)
    name = _slug(args.name or root.resolve().name)
    cls = _class_name(name)

    strategy_py = root / "strategy.py"
    strategy_yml = root / "strategy.yml"
    existing = [p for p in (strategy_py, strategy_yml) if p.exists()]
    if existing and not args.force:
        listed = ", ".join(str(p) for p in existing)
        raise CliError(f"{listed} already exists — pass --force to overwrite")

    account, feed, note = _from_node(args)

    root.mkdir(parents=True, exist_ok=True)
    strategy_py.write_text(
        _STRATEGY_PY.format(title=cls, cls=cls, name=name), encoding="utf-8"
    )
    strategy_yml.write_text(
        _STRATEGY_YML.format(td=_quoted(account), md=_quoted(feed)),
        encoding="utf-8",
    )

    print(f"created {strategy_py}")
    print(f"created {strategy_yml}")
    if note:
        print(f"\n{note}")
    else:
        print(f"\n  mftik check {root}")
        print(f"  mftik run {root}")
    return 0


def _from_node(args: argparse.Namespace) -> tuple[str, str, str | None]:
    """``(account, feed, warning)`` — real ones, or placeholders and why."""
    if args.offline:
        return (
            PLACEHOLDER_ACCOUNT,
            PLACEHOLDER_FEED,
            "Written offline, so strategy.yml names an account and a feed that\n"
            "may not exist. Fill them in, then: mftik check <dir>",
        )

    _, client = connected(args.profile)
    with client:
        apis = (client.get("/apis") or {}).get("apis") or []
        if not apis:
            return (
                PLACEHOLDER_ACCOUNT,
                PLACEHOLDER_FEED,
                "This node has no trading accounts, so strategy.yml has a\n"
                "placeholder where one should be. Add a venue credential in the\n"
                "UI, then put its name in td:.",
            )
        account = str(apis[0].get("name") or "")
        venue = str(apis[0].get("venue") or "")
        feed = _feed_for(client, venue)

    if feed is None:
        return (
            account,
            PLACEHOLDER_FEED,
            f"The symbol plane knows no instruments for {venue!r} yet, so md:\n"
            f"is a guess. Check it with: mftik check <dir>",
        )
    return account, feed, None


def _feed_for(client, venue: str) -> str | None:  # noqa: ANN001
    """A feed key for an instrument this venue actually lists."""
    if not venue:
        return None
    body = client.get(
        "/sym/symbols", params={"venue": venue, "limit": 200, "slim": True}
    )
    tickers = [
        str(row.get("universal_ticker") or "")
        for row in (body or {}).get("symbols") or []
    ]
    tickers = [t for t in tickers if t]
    if not tickers:
        return None
    # Prefer the pair everybody recognises, so the first run is legible.
    chosen = next(
        (t for t in tickers if t.upper().endswith("_BTCUSDT")), sorted(tickers)[0]
    )
    return f"{FEED_TOPIC}.{chosen}"


def _slug(raw: str) -> str:
    """A directory name as a strategy name the registry will accept."""
    slug = _SLUG.sub("_", raw.strip().lower()).strip("_")
    if slug and slug[0].isdigit():
        slug = f"s_{slug}"
    if not slug:
        raise CliError(
            f"cannot make a strategy name out of {raw!r} — pass --name"
        )
    return slug


def _class_name(name: str) -> str:
    return "".join(part.title() for part in name.split("_") if part) or "Strategy"


def _quoted(value: str) -> str:
    return '"' + value.replace('"', '\\"') + '"'
