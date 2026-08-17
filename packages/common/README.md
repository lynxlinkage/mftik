# mftik

The strategy SDK and command-line client for a [MFTIK](https://github.com/lynxlinkage/mftik)
node — a self-hosted, mid-frequency algorithmic trading platform.

You run the node yourself with Docker Compose. This package is what you install
on the machine you write strategies on.

```bash
pip install mftik
```

## Writing a strategy

A strategy is a subclass of `Strategy` and a directory of `.py` files. It reads
market data through the hooks it overrides and trades through the accessors the
base class binds.

```python
from mftik.strategy import Strategy


class MyStrategy(Strategy):
    name = "my_strategy"

    async def on_best_quote(self, quote) -> None:
        await self.log(f"{quote.universal_ticker} {quote.bid}/{quote.ask}")
```

`self.oms` places and cancels orders, `self.ledger` reads balances, `self.mds`
queries history the feeds do not carry, `self.tape` warms up on prints from
before the session started, `self.symbols` gives you the tick and step sizes to
round to, and `self.timer` schedules work. See `Strategy`'s docstring for the
full set of hooks.

A strategy may import the standard library, `mftik`, and other files in its own
directory — nothing else. The node runs the source you send it rather than an
environment you built, so a third-party import would be a module that is not
there. `mftik check` tells you before you push.

## The client

The `mftik` command talks to a node you have connected to. Profiles live in
`~/.config/mftik/config.toml`, one per node, holding its URL and the API key it
issued.

```bash
mftik profiles          # the nodes this machine knows
mftik disconnect <name> # forget one
```

`connect`, `init`, `check` and `run` are being built — see
[docs/CLI.md](https://github.com/lynxlinkage/mftik/blob/main/docs/CLI.md) for the
shape they are landing in.

## License

MIT
