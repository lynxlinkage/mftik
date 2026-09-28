# mftik

The strategy SDK and command-line client for a [MFTIK](https://github.com/lynxlinkage/mftik)
node — a self-hosted, mid-frequency algorithmic trading platform.

You run the node yourself with Docker Compose — `mftik node-init` writes the
stack for you. This package is both that and what you install on the machine
you write strategies on.

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

A session that was running when STS went away can come back. Override
`on_rebuild` and set `rebuildable = True`. Facts you `remember` while running
are handed back there, before `on_start`; resting orders, fills and position
come from recon, not from anything you stored. Leave `rebuildable` off if
coming back would mean trading beside orders you no longer know are yours.

A strategy may import the standard library, `mftik`, and other files in its own
directory — nothing else. The node runs the source you send it rather than an
environment you built, so a third-party import would be a module that is not
there. `mftik check` tells you before you push.

## Hand the loop back while you compute

Your hooks are coroutines on the session's own loop, and the heartbeat task
that watches for MD's acknowledgements is another task on the same loop. A
hook that does not await keeps that task from running, and once an
acknowledgement has gone unseen for `LEASE_HEARTBEAT_INTERVAL_S` ×
`LEASE_MISS_LIMIT` the session fails itself with `md feed from {instance}
stopped: session can no longer run` — `{instance}` being the name of the MD
holding the feed, `md` by default. That window is about 3 seconds, measured
from the last acknowledgement rather than from where you stopped awaiting, so
stay well under it.

`self.tape.read` hands back up to 200,000 prints, which is well past that, so
work through them in slices, breathing between records:

```python
from mftik.strategy import Strategy, breathe, slice_deadline


class MyStrategy(Strategy):
    async def on_start(self) -> None:
        tape = await self.tape.read(self.paras["ticker"], topic="aggtrade")
        deadline = slice_deadline()
        for print_ in tape.records:
            deadline = await breathe(deadline)
            self._fold(print_)
```

`slice_deadline()` says when the current slice is spent. `breathe(deadline)`
returns without suspending while that slice still has time and yields only
once it does not, so the cost per record is a clock read rather than a
reschedule. Keep what it returns and pass it back in — that is the next
deadline.

The same pair belongs around any other long stretch of computation in a hook.
A `read(..., on_print=...)` callback is the exception: the read breathes
before every record, and that clock check counts whatever your previous
callback spent — so an ordinary callback, which may be a plain sync function,
needs nothing added. If one call of it is long, make the callback `async` and
breathe inside it.

## The client

```bash
mftik node-init ./mynode                 # a whole node: compose, edge, .env
mftik connect https://node.example.com   # sign in once; stores an API key
mftik whoami                             # who you are to that node
mftik profiles                           # every node this machine knows
mftik disconnect <name>                  # forget one
mftik init ./hello                       # scaffold, filled in from that node
mftik check ./hello                      # import gate and on_initialized, offline
mftik run ./hello                        # push, deploy, tail; ^C stops it
mftik push ./hello                       # copy the tree into the node's private registry
mftik run ./hello                        # push, deploy, tail the session log
mftik ps                                 # what is running
mftik logs -f <session>                  # what it is saying
mftik stop <session>                     # stop it
```

`connect` signs in with your password, mints an API key through that session,
stores the key and drops the session — so the password is never written down.
Profiles live in `~/.config/mftik/config.toml` at mode `0600`. For CI, pass an
existing key with `--token` and no prompt is reached.

`init` is being built — see
[docs/CLI.md](https://github.com/lynxlinkage/mftik/blob/main/docs/CLI.md) for the
shape it is landing in.

## License

MIT
