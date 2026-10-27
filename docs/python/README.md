# Python SDK

`mftik` is the strategy SDK for an mftiko session. A process started with `python -m mftik.run module:Class` is one session worker. These pages are the user guide and the API reference. They are hand-written Markdown. They are not generated from docstrings, and there is no separate documentation site.

| Page | What it covers |
| --- | --- |
| [Install and run](install.md) | The wheel, `python -m mftik.run`, and what the process has to be |
| [Lifecycle](lifecycle.md) | `on_init`, `on_start`, `on_stop`, and the 17 optional hooks, with the Go names |
| [Context](context.md) | What a hook may call: instruments, orders, reads, timers |
| [Errors and exit codes](errors.md) | The exception hierarchy, outcomes, and process status |
| [Threads](threads.md) | The GIL, `Poster`, and how long a `Context` or `BookView` stays valid |
| [Runtime notes](runtime.md) | `multiprocessing`, SIGTERM, and `Limits.memory_mb` |
| [Pack and deploy](pack.md) | `python -m mftik.pack`, `StrategyRef.code`, and running a session with `mftiko` |
| [Session spec](session.md) | Every field of the session file and what it means to a Python strategy |
| [Volume and inputs](volume.md) | Durable files and read-only inputs |
| [Examples](examples.md) | `md_watch`, `maker`, and `stateful`, quoted from `examples/python` |
| [API reference](reference.md) | Every public name |

A hook runs on one thread, one at a time. It must return. The only object that may cross threads is a `Poster`. `Context` and `BookView` work only inside the hook that received them.

A fenced listing is an excerpt of a file under `examples/python`. The info string names that file and the inclusive line numbers.
