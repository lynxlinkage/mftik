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

```bash
mftik connect https://node.example.com   # sign in once; stores an API key
mftik whoami                             # who you are to that node
mftik profiles                           # every node this machine knows
mftik disconnect <name>                  # forget one
mftik check ./hello                      # import gate and on_initialized, offline
```

`connect` signs in with your password, mints an API key through that session,
stores the key and drops the session — so the password is never written down.
Profiles live in `~/.config/mftik/config.toml` at mode `0600`. For CI, pass an
existing key with `--token` and no prompt is reached.

`push` and `run` are being built — see
[docs/CLI.md](https://github.com/lynxlinkage/mftik/blob/main/docs/CLI.md) for the
shape they are landing in.

## License

MIT
