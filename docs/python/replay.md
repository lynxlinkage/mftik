# Replay

`replay` runs a strategy class again over one incarnation of a session's event log, in the calling process, without NATS and without the platform. It returns a `ReplayReport` that lists every way the run differs from the recording. Use it to check that a changed strategy still makes the same calls the recorded one made.

`replay(strategy_cls, dir, *, incarnation=None, volume_dir=None, input_dir=None)`

## The log

A session writes the event log only when its spec turns it on (`InitPayload.event_log`). The files are under the session volume, in `eventlog`, named `{incarnation}-{n}.mel`. `dir` is that `eventlog` directory. The format is PROTOCOL §12. `mftiko-eventlog dump` prints a file as JSON. The log does not hold the strategy's own log lines.

## Arguments

`strategy_cls` is a subclass of `Strategy`. `replay` constructs it with no arguments, as `python -m mftik.run` does. Anything else raises `TypeError`. An exception from the constructor is raised as it is, not wrapped.

`incarnation` picks which incarnation in `dir` to run. `None` is the newest one there. Otherwise it is an `int` from 1 to 2³²−1; a value outside that range raises `ValueError`, and a `bool` or a non-integer raises `TypeError`.

`volume_dir` and `input_dir` are what `volume_dir` and `input_dir` on the `Context` return during the replay. Unset, both are empty, as in a session that has no volume and no inputs. `replay` does not create, copy, or clean either directory. When `volume_dir` is set, a hook that writes the volume writes there again.

`dir`, `volume_dir`, and `input_dir` are `str` or `os.PathLike[str]`. A `bytes` path raises `TypeError`. The arguments are checked before the class is constructed.

## What runs

Each recorded input whose hook returned in the session runs again, in log order, on the calling thread: `on_init`, `on_start`, feed and book hooks, account events, read callbacks, `after` and `post` callbacks, and `on_stop`. A hook that did not return in the session (the process was killed, or the hook ran past `hook_timeout_s`) is not run, and is counted in `skipped`.

Inputs come from the log. `now` is the recorded start of the hook. `rand` is a new `random.Random` built from the recorded seed, so each replay draws the same numbers the session drew. `resolve` returns the recorded answer, in the recorded order. Read callbacks get the recorded replies. Client order ids continue from the counter the session had at Init. Nothing is sent: `submit`, `cancel`, the reads, and `resolve` are compared, not delivered.

## What is compared

The replay compares the calls the strategy makes and how each hook ends:

- Each order call: the operation, the account, and the client order ids, then the request that would be sent.
- Each `md.view` and `md.fetch` read: the kind and the subject.
- Each `resolve`: the ticker, in order. A recorded call the replay does not make again is a difference too.
- How each hook ended. An exception ends a hook with `{type name}: {message}`, the same text the session recorded and put in `Done.error`. A hook that raised in the session must raise an exception with the same class name and the same message; a hook that returned must return.
- A timer or a `post` that ran in the session and that the replay did not schedule.

Errors that come from the log are raised as the class the session raised. A recorded `resolve` failure is `NotInInit`, `UnknownUid`, or `Stopping` when the session got one of those, and `Error` with the recorded text otherwise. A read callback gets `ReadError` with the recorded text.

Log lines are not compared. The strategy's `log` and `ctx.log` lines, and tracebacks, still go to stderr during the replay.

## ReplayReport

`ReplayReport` is read-only. Its fields are the Rust report's.

| Field | Meaning |
| --- | --- |
| `session_id` | The session that wrote the log |
| `incarnation` | The incarnation that ran |
| `sdk_version` | The `mftik` release that wrote the log |
| `sdk_commit` | That release's commit |
| `inputs` | Recorded inputs whose hooks ran again, `on_init` and `on_start` included |
| `skipped` | Recorded inputs whose hook did not return in the session, not run again |
| `missing` | Record numbers absent from the files: frames the session's writer dropped when its buffer was full |
| `orders` | Recorded order calls that were compared |
| `reads` | Recorded reads that were compared |
| `mismatches` | A `tuple` of `str`, one per difference. Empty means the replay made the same calls and ended each hook the same way |

A difference does not raise. Each line names the record number or call id, what the session recorded, and what the replay did.

## Errors

`ReplayError` is raised when the log cannot be replayed at all: `dir` cannot be read, it has no file of that incarnation, a file is not an event log or has an unknown format version, a frame has a bad length, or the log does not start with the `init` frame. A file that ends inside a frame is what a killed process leaves; the frames before it are replayed. `ReplayError` is a subclass of `Error`. Its message starts `sdk: replay:`.

`replay` called inside a hook raises `ReplayError` with the message `sdk: replay: called inside a hook`. A process that has called `run` hosts a session, and `replay` there raises `ReplayError` with the message `sdk: replay: this process hosts a session`. Replay in a separate process, such as a test.

## Threads

A process runs one replay at a time. The client order id counter and the `rand` state belong to the replay that is running. A call from another thread waits for the running one to return. The GIL is released while it waits and between hooks; each hook takes it back, as in a session.

## Example

`examples/python/sampler/test_e2e.py` records one `sampler` session with the event log on, then replays it in the test process. The recorded strategy replays with no difference:

```python examples/python/sampler/test_e2e.py:127-133
    def test_the_recorded_session_replays_with_no_mismatch(self) -> None:
        self.assertTrue(any(self.log_dir.glob("*.mel")), list(self.log_dir.iterdir()))
        report = mftik.replay(Sampler, self.log_dir)
        self.assertEqual(report.mismatches, ())
        self.assertEqual(report.skipped, 0, report)
        self.assertEqual(report.missing, 0, report)
        self.assertEqual(report.orders, 0, report)
```

A changed strategy whose timer raises does not:

```python examples/python/sampler/test_e2e.py:90-94
class Changed(Sampler):
    """Sampler whose timer raises: a modified strategy (E07-D024)."""

    def _on_timer(self, ctx: mftik.Context) -> None:
        raise ValueError("sampler: changed")
```

Its first timer ends with `ValueError: sampler: changed` where the session returned, and the timers the session ran after it are never scheduled. Both are lines in `mismatches`. The same test replays twice more and checks that every hook's `now` and draw are the recorded ones.
