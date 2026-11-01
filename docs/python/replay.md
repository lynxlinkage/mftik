# Replay

`replay` runs a strategy class over one incarnation of a recorded event log, without NATS or the control connection (PROTOCOL §12.6). It is the Python wrapper around the Rust engine `mftik_rt::replay`. Log lines are not compared.

The session that wrote the log must have had the event log on. Frames are under the volume's `eventlog` directory. That directory is `dir`.

`replay` constructs the class with no arguments, the same way `python -m mftik.run module:Class` starts a worker. Pass the class, not an instance. A bool `incarnation` raises `TypeError`. `dir`, `volume_dir`, and `input_dir` accept a `str` or an `os.PathLike` of a `str`. A bytes path raises `TypeError` (`dir must be a str, not bytes`, and the same form for the other two).

`incarnation`, `volume_dir`, and `input_dir` map to Rust `ReplayOptions`. `None`, or leaving them out, uses the Rust defaults: newest incarnation in the directory, empty volume and input directories. Incarnation `0` is also the newest file. `volume_dir` and `input_dir` are what `Context.volume_dir` and `Context.input_dir` return during the replay.

```python examples/python/sampler/README.md:37-41
import mftik
from sampler import Sampler

report = mftik.replay(Sampler, "eventlog")
assert not report.mismatches
```

`sampler` is the recorded example. Record it, then replay that class over the same `eventlog` directory. The report's `mismatches` is empty. A subclass that changes a hook, for example by raising in `on_start`, produces a mismatch. See [Examples](examples.md).

## Report

The return is a `ReplayReport`. It is read-only. Python code does not construct it. The fields match the Rust report.

| Field | Meaning |
| --- | --- |
| `session_id` | Session id from the log header |
| `incarnation` | Incarnation that was replayed |
| `sdk_version` | Release that wrote the log |
| `sdk_commit` | Commit that wrote the log |
| `inputs` | Recorded inputs whose hooks ran again, `on_init` and `on_start` included |
| `skipped` | Inputs whose hook did not return in the recording; they are not run again |
| `missing` | `rec_seq` numbers that are not in the files |
| `orders` | Recorded order calls that were compared |
| `reads` | Recorded md reads that were compared |
| `mismatches` | Each way the replay differs from the recording, as a tuple of strings. Empty means the outputs match |

A mismatch names the `rec_seq`, the kind, the recorded value, and the replayed value. Differences belong on this field. They do not raise.

`ReplayReport` compares with `__eq__` and prints with `__repr__`.

## Errors

The log could not be replayed at all when `dir` cannot be read, has no file for that incarnation, or the header does not parse. That is `ReplayError`, a subclass of `Error`. The message starts with `sdk: replay:`. Catch `Error` to handle it with the other SDK failures. See [Errors and exit codes](errors.md).

A context failure raised during the replay uses the same `mftik` subclass a live session would raise (`UnknownUid`, `NotInInit`, and the rest). Hook endings are compared as `{type}: {message}`, the same string a live session stores on `hook_end` and in `Done.error`. The type name is the exception class's `__name__` (`RuntimeError`, `UnknownUid`), not a module path.

Each `replay` drops the process-wide `Context.rand` object so the next call seeds a new `random.Random` from that log's `init` frame. See [Context](context.md).
