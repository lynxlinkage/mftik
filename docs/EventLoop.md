# uvloop — why every process here runs it, and what that was worth

All six processes run uvloop. Five of them were changed to; the API had been
doing it by accident since the day it was written, and now says so.

What it bought is roughly a tenth of the CPU each plane burns. It is not worth
much latency, and the rest of this document is mostly the evidence for that
second sentence — because the change is easy to make for the wrong reason, and
the number that would justify the wrong reason is not there.

`just loop-bench` reproduces every measurement below.

## What this changed

| | Before | After |
|---|---|---|
| api | `uvicorn.run(...)`, no `loop` → `auto` → uvloop | `loop="uvloop"`, stated |
| td, md, sts, sym, paper | `asyncio.run(amain())` | `uvloop.run(amain())` |
| declared dependency | none of the six named uvloop | all six do |
| test suite | stdlib loop | uvloop, and `packages` on both |

`uvloop.run` rather than a policy installed at import: it builds the loop for
that one call and leaves `asyncio`'s global policy alone, so each entrypoint
states the loop it runs on and nothing else in the process is reached into.

**The dependency was declared even though the image already had it.** uvloop
arrives as a `uvicorn[standard]` extra (`apps/api/pyproject.toml`), and the
Dockerfile installs the whole workspace at once with `uv sync --all-packages`
(`Dockerfile:32`), so `import uvloop` already worked in every container. But it
worked *by proximity*: `uv tree --package mftik-td` did not mention uvloop, so a
per-app sync — a slim image, a local `uv sync --package mftik-sts` — would have
built a container that started and died on the import. Naming it in the five
apps is what makes the import a fact rather than a coincidence. The API names it
too, now that its code asks for `loop="uvloop"` by name.

It is *not* declared in `packages/common`. That is the distribution a strategy
author installs with `pip install mftik`, uvloop publishes no Windows wheel, and
the SDK's own entrypoint — the `mftik` CLI — is synchronous and needs no loop.

## What made this cheap

Facts checkable in the tree, and the reason this was a five-line change rather
than a project.

**uvloop was already in the lockfile**, pinned at 0.22.1 (`uv.lock`), with
prebuilt manylinux and musllinux wheels for x86-64 and aarch64 on 3.12, 3.13
and 3.14. Nothing compiles, and the image did not grow.

**Each process owned exactly one loop, created in one line.** There is no
`new_event_loop`, no `set_event_loop`, no `get_event_loop`, no
`set_event_loop_policy` and no `run_forever` anywhere in the repository, so
there was nothing to unpick — only `asyncio.run` to replace, in
`apps/td/src/mftik_td/app.py`, `apps/md/src/mftik_md/app.py`,
`apps/sts/src/mftik_sts/app.py`, `apps/sym/src/mftik_sym/app.py` and
`apps/paper/src/mftik_paper/app.py`.

**Sessions are tasks, not processes.** STS, TD and MD all multiplex sessions
onto one loop with `asyncio.create_task`. That is why the CPU saving is the part
worth having: one slow callback is felt by every session in the process, so a
tenth of the CPU back is a tenth more headroom before a plane has to be split.
It is also why there was exactly one loop per container to convert.

**Nothing in the tree uses the parts of asyncio uvloop does not implement.**
No `add_reader` or `add_writer`, no child watchers, no `subprocess_exec` or
`create_subprocess_*`, no Unix-socket servers, no `SelectorEventLoop`
references, and no `EventLoopPolicy` subclass. The four loop APIs that *are*
used — `loop.add_signal_handler` for SIGINT/SIGTERM in all five domains,
`asyncio.to_thread` for eventlog disk writes
(`packages/common/src/mftik/strategy/eventlog.py:293`),
`loop.run_in_executor` against a dedicated pool for alert matching
(`apps/api/src/mftik_api/alert_eval.py:20`), and `loop.time()` — are all
supported. Two of them behave differently, which is the next section.

**The scripts under `scripts/` were left on `asyncio.run`.** They are one-shot
operator tools — seed, fetch, a cursor reset, a backfill probe — that make a
handful of round trips and exit. There is no loop-sensitive behaviour to align
in a process whose whole life is shorter than one plane's heartbeat interval,
and `loop_bench.py` runs both loops on purpose.

## The two behaviours that actually differ

`just loop-bench --probe` runs both loops through the behaviours this tree
depends on. Everything is identical except the `loop.time()` rows and the
sub-millisecond sleep, and neither of those turns out to reach a call site here.

| Probe | asyncio | uvloop |
|---|---|---|
| `loop.time()` steps per 2000 yields | 2000 | 2 |
| `loop.time()` smallest step | 1.4 µs | 1000 µs |
| `await asyncio.sleep(0.0001)` | 1121 µs | 1.7 µs |
| `await asyncio.sleep(0.01)` | 10135 µs | 10127 µs |
| `add_signal_handler` | delivered | delivered |
| `to_thread` / custom executor | ok | ok |
| refused connect raises | `ConnectionRefusedError` | `ConnectionRefusedError` |
| named tasks, cancellation, exception handler | same | same |

**`loop.time()` is a millisecond clock under uvloop.** libuv keeps its own
cached clock and reports whole milliseconds, so a loop that yields two thousand
times sees the clock move twice. Every deadline in this tree built on that clock
is seconds: `LEASE_GRACE_S` is 3.0 in MD and 5.0 in TD,
`PENDING_NEW_TIMEOUT_S` 5.0, `UNKNOWN_FORCE_RECON_S` 10.0,
`UNKNOWN_FORCE_RECON_INTERVAL_S` 60.0, `CANCEL_RETRY_S` 1.0. A millisecond is
0.1% of the tightest of them, so nothing in the tree can tell the difference
today.

It is still worth writing down, because it is a ceiling rather than a bug. The
venue sockets stamp `stats.last_frame_at` off this clock, and any future work
that wants to say how long a print took to reach a strategy cannot get that from
`loop.time()` on uvloop. `time.perf_counter()` is the clock for that, and it is
unaffected: every duration in the probe and the benchmark is measured with it,
and the two loops agree on the sleeps they both honour to within 8 µs.

**A sub-millisecond sleep stops being a sleep.** asyncio floors
`sleep(0.0001)` at about a millisecond; uvloop returns from it in under two
microseconds. A poll loop written as `await asyncio.sleep(0.0001)` goes from a
thousand laps a second to a spin. Nothing did: the shortest sleep anywhere in
the tree, production or test, is `0.01`, which both loops honour to within 8 µs.
It is a rule for whoever writes the next poll loop, not something this change had
to work around.

## The suite says the same thing on either loop

The suite now runs on uvloop by default, because a suite on a different loop
from the node cannot see a loop-specific regression. `conftest.py` implements
pytest-asyncio's `pytest_asyncio_loop_factories` hook and reads
`MFTIK_TEST_LOOP`, so either loop is one environment variable away. The hook
rather than the older `event_loop_policy` fixture: overriding that fixture is
deprecated and warns, and loop policies are leaving asyncio itself.

CI runs the whole suite on uvloop and then `packages` again on the stdlib loop.
That second pass is not symmetry — `packages/common` ships as `mftik`, so
nothing a strategy author imports may require uvloop to work.

All 2,926 tests were run both ways while making this change: every venue adapter
against its stub server, the OMS, the broker, the TD/MD/STS session machinery.
209 seconds either way, the suite being bound by fakeredis and sqlite rather
than by the loop. **No test failed on uvloop that passed on asyncio.**

The handful that did fail deserve the honest version rather than a rounded one:
the same set on both loops except one that passed on uvloop, all of them about
the on-disk strategy registry and an environment overlay rather than about async
anything, and all of them order-dependent — run in isolation the set changes
again. They reproduce on asyncio with none of this applied.

**One thing worth knowing that this change did not cause.** A domain that gets
SIGTERM exits non-zero, because `run_until_stopped` sees the plane's own loops
finish in the same `asyncio.wait` batch as its stopper and reports them as having
"ended before shutdown". Booting Paper for real and signalling it gives that
result six times out of six on *both* loops, so uvloop neither introduced it nor
hides it. It is left alone here rather than folded into a loop swap, but a clean
shutdown that looks like a crash to whatever reads exit codes is worth its own
change.

## What it bought

Measured with `just loop-bench` against a real Redis, driving the actual
`Broker` and the actual pydantic envelopes. Medians of five reps.

| Case | What it is | asyncio | uvloop | uvloop gain |
|---|---|---|---|---|
| `tcp_echo` | bare stream round trip — **the control** | 69,196/s | 104,570/s | **1.51x** |
| `ws_ingest` | a venue read loop: recv + `json.loads` | 89,685/s | 97,274/s | 1.08x |
| `subscribe` | STS feed pump: `get_message` + envelope parse | 9,943/s | 10,103/s | 1.02x |
| `rpc` | `Broker.request` p50 latency (lower is better) | 332 µs | 309 µs | 1.07x |
| `fanout` | `Dispatcher.publish` as it is written | 1,747/s | 1,731/s | 0.99x |

Read the control first. On a raw transport with nothing above it, uvloop is
half again as fast, so the harness can plainly see what uvloop is good at. The
rest of the table is that advantage being diluted, and the order is not a
coincidence — it is the amount of Python between the socket and the payload.
Add `websockets` framing and a `json.loads` and 1.51x becomes 1.08x. Add
redis-py's protocol handling and pydantic validation and it becomes 1.02x. On
MD's fan-out it disappears into the noise, because that path is not spending its
time in the loop at all: it is spending it on `sessions + 1` serialised Redis
round trips per print (`apps/md/src/mftik_md/session/dispatcher.py:103`), and no
loop makes a round trip that still has to happen any cheaper.

**CPU is the exception, and it is consistent.** uvloop costs 10–12% less CPU
for identical work on every Redis case — 63.4 → 56.5 µs per fan-out round trip,
100.4 → 91.8 µs per message received, 0.50 → 0.45 s for 1,500 RPCs. That is the
reason this change was made, and it is a different claim from the one a loop
swap is usually proposed to make.

## The lever that is bigger than the loop

The same harness runs the fan-out a second way, with the N publishes and the
tape append batched into one round trip instead of `N+1` awaited ones:

| Fan-out variant | asyncio | uvloop |
|---|---|---|
| as it is written | 1,747/s | 1,731/s |
| pipelined | 5,663/s | 5,794/s |

**3.24x, on the same loop.** Nothing here batches those round trips yet, and
that is the point of leaving this section in: if MD's throughput is ever the
problem, this change is not the fix for it and switching loops again will not
help either. The two levers do not compete for the same work, but only one of
them is on the critical path of a busy feed.

The same shape shows up elsewhere and is worth naming while it is in view:
`Broker.subscribe` sleeps 10 ms between empty polls and `Broker.request` polls
BLPOP in one-second laps; `envapply` runs a blocking `subprocess.run` on the
loop that serves it. None of those are loop problems, and a faster loop hides
none of them.

## Why one loop rather than the faster one

The CPU is the payoff, but consistency is the reason the change was worth making
rather than deferring. Before it, the API ran uvloop because a transitive extra
happened to be installed and the five domains ran the stdlib loop because nobody
had said otherwise. That is one node running two loops by accident: a
loop-sensitive bug would reproduce differently in the API than in STS, and
nothing in the repository would have explained why. Either loop was defensible;
running both without having chosen was not.

That is also why the API's `loop="uvloop"` is in the diff despite changing no
behaviour. With `auto`, a dependency bump that dropped the `standard` extra would
have moved the API back to the stdlib loop silently, and the only trace would
have been a latency graph.

## What to watch

In this order:

**Per-plane CPU at a fixed message rate.** Where the 10–12% should appear, and
the only place the case for this change rests. If it does not show up in
production, the change bought nothing and should be said so.

**Reconnect behaviour on the venue sockets.** The largest body of OS-error
handling in the tree and the least exercised by tests. The probe confirms a
refused connection still raises `ConnectionRefusedError` and still is an
`OSError`, so every `except OSError` still catches it — but that is a check on
one error, not on nine venues' worth of disconnect paths.

**Eventlog write latency.** `asyncio.to_thread` is the one place the loop hands
work to a thread on a path a strategy can feel.

**Anything that starts measuring durations off `loop.time()`.** It is a
millisecond clock now. Use `time.perf_counter()`.
