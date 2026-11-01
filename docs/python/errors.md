# Errors and exit codes

The process ends with one of the statuses in PROTOCOL §5.3. `run` does not return. The parent of the process observes the exit code. The session status carries the outcome and the reason.

| Outcome | When | `on_stop` | Exit code |
| --- | --- | --- | --- |
| `DONE` | A stop from the platform, or `exit` | runs | 0 |
| `INIT_FAILED` | `on_init` or `on_start` failed, or a deadline expired | only when start failed and it was not a timeout | 10 |
| `CRASHED` | A hook after start raised, or the hook panicked | runs | 20 |
| `HUNG` | A hook ran past `hook_timeout_s` | does not run | 21 |
| `FAILED` | `fail`, the platform asked the session to fail, or the account queue overflowed | runs | 30 |

Exit code 1 is the worker giving up before it can keep speaking the control protocol, for example when it cannot take the control file descriptor. A process killed by a signal has no outcome from this table. The platform records the signal. `HUNG` from the platform's own heartbeat watchdog is that kind of kill: the process is signaled, and the platform treats the outcome as `HUNG` with reason `no_beat`.

`CRASHED` does not deliver account events that were still queued. `DONE` and `FAILED` do deliver the account events received before the stop, and then run `on_stop`. The event that overflowed the account queue is not one of those events. The reason for that overflow is `td buffer full`.

A deadline on init or start uses reason `deadline`, does not wait for the hook, does not run `on_stop`, and exits 10. A hook that exceeds `hook_timeout_s` uses reason `hook timeout` and exits 21. `on_stop` is not called.

When the strategy ends the session itself, or the control connection closes, `on_stop` is also under `hook_timeout_s`. If that hook exceeds the budget, the process exits 21 and the platform is told that `on_stop` did not finish. The outcome already sent stays what it was. It is not rewritten to `HUNG`.

An exception from `on_stop` does not change the outcome or the exit code either. The traceback is written to stderr.

## Exceptions from hooks

An exception from `on_init` or `on_start` ends the session `INIT_FAILED`. `Done.error` is `{type name}: {message}`. The traceback is only on stderr.

An exception from any later hook, including a read callback, an `after` callback, and a `post` callback, is reported as a hook panic. The outcome is `CRASHED`, the reason is `hook panic: {type name}: {message}`, and `on_stop` still runs.

`SystemExit` and `KeyboardInterrupt` follow those same rules. `sys.exit` does not call `exit`. A `KeyboardInterrupt` is not the platform's stop signal. The platform stops a session with SIGTERM, which the runtime already handles. See [Runtime notes](runtime.md).

The text of an exception is stored in `Done.error` and in the exiting reason. It must not contain a secret. The same rule applies to the reason passed to `fail` and to lines written to stdout or stderr.

A load failure uses the same `INIT_FAILED` ending. Its `Done.error` is `load module:Class: {type name}: {message}`. See [Install and run](install.md).

## `Error` and its subclasses

`Error` is the base class of the SDK's context failures. The subclasses are `NotInInit`, `UnknownUid`, `UnknownAccount`, `AlgoNotDeclared`, `NoFeed`, `Stopping`, `ReadError`, `ContextError`, and `ReplayError`. Catch `Error` to handle any of them. They are not subclasses of each other.

| Exception | Message |
| --- | --- |
| `NotInInit` | `sdk: only OnInit and OnStart may call this` |
| `UnknownUid` | `sdk: the instrument is not known to this session` |
| `UnknownAccount` | `sdk: the account is not bound to this session` |
| `AlgoNotDeclared` | `sdk: the algo_id is not declared for this account` |
| `NoFeed` | `sdk: the session has no such feed` |
| `Stopping` | `sdk: the session is stopping` |

`ContextError` means a `Context` or a `BookView` was used outside the hook that produced it. The messages are `mftik.Context is only valid inside the hook that received it` and `mftik.BookView is only valid inside the hook that received it`. `now` does not raise `ContextError`: it is the start of the hook running on this thread, or the time now when none is. See [Threads](threads.md).

`ReadError` is not raised by `tape`, `book_snapshot`, `fetch_klines`, or `fetch_open_interest`. Those methods hand it to the callback as the third argument. The message is the runtime's text. A timeout looks like `mdpath: md.view: context deadline exceeded`. A missing responder looks like `mdpath: md.fetch: nats: no responders available for request`.

`ReplayError` is raised by `replay`, not by a hook. It means the event log could not be replayed at all, for example a directory with no log of that incarnation. Differences between the replay and the recording are not errors: they are lines in `ReplayReport.mismatches`. The message starts `sdk: replay:`. See [Replay](replay.md).

Other failures use the base `Error` and keep their text. A ticker `resolve` cannot find raises `Error` with a message that starts `symbol: no such instrument:`. `rand` before OnInit raises `Error` with the message `sdk: the session has no seed before OnInit`. `ValueError` and `TypeError` are used for arguments the SDK rejects before a session call, such as a bad `after` delay, a non-`Order` passed to `submit`, or a `str` passed as volume bytes. Those are Python's own exceptions, not subclasses of `Error`.

`submit` raises `ValueError` when `side`, `order_type`, or `tif` is an int the order path cannot store (`unknown side N`, and the same form for `order_type` and `tif`). The object may still hold that int. Proto enums such as `Side` keep an unknown int. `Origin` and `RejectedRequest` do not: constructing either with an unknown int raises `ValueError`.

A bool is not an enum value. Passing one as `side`, `order_type`, or `tif` when constructing an `Order` raises `TypeError`, message `bool is not an enum value`. Constructing `Origin`, `RejectedRequest`, or a proto enum with a bool raises `ValueError` with that same message.
