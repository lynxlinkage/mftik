# Threads

Hooks run on the thread that called `run`. For `python -m mftik.run` that is the main thread. The SDK does not start a Python thread of its own. Market data, account events, timers, and read replies are queued by Rust threads and delivered on this one thread, one hook at a time. A strategy does not need a lock for fields that only hooks touch.

The GIL is held during a hook and released when the hook returns. Between hooks the runtime does not hold it. `resolve` releases it while the symbol lookup waits. `read` and `write` release it while the filesystem calls run. Those calls still occupy the dispatcher until they return, so the next event waits.

## Do not block a hook

A hook must return. The dispatcher delivers nothing else until it does. A hook that runs longer than the session's `hook_timeout_s` ends the session `HUNG`: exit code 21, reason `hook timeout`, and `on_stop` does not run. A hook longer than 100 milliseconds counts as slow and is logged. That 100 millisecond mark is not the hard stop.

`on_init` and `on_start` may call `resolve`, which waits, and they may read the volume. That wait is the one blocking SDK call, and only those two hooks may make it. After `on_start`, do not wait on the network, a lock, or a sleep inside a hook. Schedule later work with `after` instead of sleeping. The delay is a number of seconds, the same unit as `time.sleep`, but the callback runs on the dispatcher rather than blocking this hook. `md_watch` and `stateful` arm the next period from that callback. See [Examples](examples.md).

The Beat thread does not take the GIL, so a hook that holds the GIL does not by itself stop the heartbeat. The hook budget still applies. A C extension that blocks without releasing the GIL stalls other Python threads for the same reason any such extension does. It does not change the rule: the hook has to return before the budget expires.

Do not run an asyncio loop on this thread and then block a hook waiting for it. The receive path is already on other threads. A hook that stalls also stalls delivery.

## What may leave the hook

`Context` is valid only inside the hook that received it. Using it after the hook returns, or from any other thread, raises `ContextError`. The message is `mftik.Context is only valid inside the hook that received it`.

`book` returns a `BookView` or `None`. The view is valid only inside that same hook. Using it later, or from another thread, raises `ContextError` with the message `mftik.BookView is only valid inside the hook that received it`. The lists from `bids` and `asks` are copies. Those lists, and the copies returned by `order`, `open_orders`, `position`, and `balance`, are ordinary objects and stay usable after the hook returns.

`poster` returns a `Poster`. That object is the one handle that may be used from another Python thread. `Poster.post` takes a callable and queues it on the account buffer. The callable runs later on the dispatcher, as a hook, with a new `Context`. It receives one argument, that context. `Context.post` queues the same kind of callback from inside the hook.

`post` returns without an error when the callback cannot be queued: the dispatcher does not exist yet, or the account buffer is already closed because the session is stopping. The callback is dropped. A callback queued successfully runs once. If it raises, the session ends `CRASHED`, the same as any other hook after start. See [Errors and exit codes](errors.md).

A thread the strategy starts itself must not read or write strategy fields except from a `post` callback, and it must not keep a `Context` or a `BookView`. The callback is serialized with every other hook, so it may use the strategy's attributes when it runs.
